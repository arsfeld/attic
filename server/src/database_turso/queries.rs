//! Raw SQL query implementations for Turso database backend.

use std::sync::Arc;

use anyhow::anyhow;
use chrono::Utc;

use crate::error::{ErrorKind, ServerError, ServerResult};
use crate::narinfo::Compression;
use attic::cache::CacheName;
use attic::hash::Hash;
use attic::nix_store::StorePathHash;

use super::connection::TursoConnection;
use super::models::{CacheModel, ChunkModel, ChunkState, NarModel, NarState, ObjectModel};
use super::{ChunkGuard, NarGuard};

/// A simple error type for database operations that implements StdError.
#[derive(Debug)]
struct DbError(String);

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for DbError {}

/// Helper to convert errors to ServerError.
fn db_err<E: std::fmt::Display>(e: E) -> ServerError {
    ServerError::database_error(DbError(e.to_string()))
}

/// Finds a cache by name.
pub async fn find_cache(
    conn: &TursoConnection,
    cache: &CacheName,
) -> ServerResult<CacheModel> {
    let sql = r#"
        SELECT id, name, keypair, is_public, store_dir, priority,
               upstream_cache_key_names, created_at, deleted_at, retention_period
        FROM cache
        WHERE name = ?1 AND deleted_at IS NULL
    "#;

    let mut rows = conn.query(sql, [cache.as_str()])
        .await
        .map_err(db_err)?;

    match rows.next().await.map_err(db_err)? {
        Some(row) => CacheModel::from_row(&row).map_err(db_err),
        None => Err(ErrorKind::NoSuchCache.into()),
    }
}

/// Finds an object and its associated cache, NAR, and optionally chunks.
pub async fn find_object_and_chunks_by_store_path_hash(
    conn: &TursoConnection,
    cache: &CacheName,
    store_path_hash: &StorePathHash,
    include_chunks: bool,
) -> ServerResult<(ObjectModel, CacheModel, NarModel, Vec<Option<ChunkModel>>)> {
    if include_chunks {
        find_object_with_chunks(conn, cache, store_path_hash).await
    } else {
        find_object_without_chunks(conn, cache, store_path_hash).await
    }
}

/// Finds an object without including chunks.
async fn find_object_without_chunks(
    conn: &TursoConnection,
    cache: &CacheName,
    store_path_hash: &StorePathHash,
) -> ServerResult<(ObjectModel, CacheModel, NarModel, Vec<Option<ChunkModel>>)> {
    let sql = r#"
        SELECT
            o.id, o.cache_id, o.nar_id, o.store_path_hash, o.store_path,
            o.references, o.system, o.deriver, o.sigs, o.ca,
            o.created_at, o.last_accessed_at, o.created_by,
            c.id, c.name, c.keypair, c.is_public, c.store_dir, c.priority,
            c.upstream_cache_key_names, c.created_at, c.deleted_at, c.retention_period,
            n.id, n.state, n.nar_hash, n.nar_size, n.compression,
            n.num_chunks, n.completeness_hint, n.holders_count, n.created_at
        FROM object o
        INNER JOIN cache c ON o.cache_id = c.id
        INNER JOIN nar n ON o.nar_id = n.id
        WHERE c.name = ?1
          AND c.deleted_at IS NULL
          AND o.store_path_hash = ?2
          AND n.state = 'V'
    "#;

    let mut rows = conn.query(sql, (cache.as_str(), store_path_hash.as_str()))
        .await
        .map_err(db_err)?;

    match rows.next().await.map_err(db_err)? {
        Some(row) => {
            let object = ObjectModel::from_row_prefixed(&row, "o_", 0)
                .map_err(db_err)?;
            let cache_model = CacheModel::from_row_prefixed(&row, "c_", ObjectModel::column_count())
                .map_err(db_err)?;
            let nar = NarModel::from_row_prefixed(
                &row,
                "n_",
                ObjectModel::column_count() + CacheModel::column_count(),
            )
            .map_err(db_err)?;

            Ok((object, cache_model, nar, Vec::new()))
        }
        None => Err(ErrorKind::NoSuchObject.into()),
    }
}

/// Finds an object including all its chunks.
async fn find_object_with_chunks(
    conn: &TursoConnection,
    cache: &CacheName,
    store_path_hash: &StorePathHash,
) -> ServerResult<(ObjectModel, CacheModel, NarModel, Vec<Option<ChunkModel>>)> {
    let sql = r#"
        SELECT
            o.id, o.cache_id, o.nar_id, o.store_path_hash, o.store_path,
            o.references, o.system, o.deriver, o.sigs, o.ca,
            o.created_at, o.last_accessed_at, o.created_by,
            c.id, c.name, c.keypair, c.is_public, c.store_dir, c.priority,
            c.upstream_cache_key_names, c.created_at, c.deleted_at, c.retention_period,
            n.id, n.state, n.nar_hash, n.nar_size, n.compression,
            n.num_chunks, n.completeness_hint, n.holders_count, n.created_at,
            ch.id, ch.state, ch.chunk_hash, ch.chunk_size, ch.file_hash,
            ch.file_size, ch.compression, ch.remote_file, ch.remote_file_id,
            ch.holders_count, ch.created_at,
            cr.id, cr.nar_id, cr.seq, cr.chunk_id, cr.chunk_hash, cr.compression
        FROM object o
        INNER JOIN cache c ON o.cache_id = c.id
        INNER JOIN nar n ON o.nar_id = n.id
        INNER JOIN chunkref cr ON cr.nar_id = n.id
        LEFT JOIN chunk ch ON cr.chunk_id = ch.id AND (ch.state = 'V' OR ch.state IS NULL)
        WHERE c.name = ?1
          AND c.deleted_at IS NULL
          AND o.store_path_hash = ?2
          AND n.state = 'V'
        ORDER BY cr.seq ASC
    "#;

    let mut rows = conn.query(sql, (cache.as_str(), store_path_hash.as_str()))
        .await
        .map_err(db_err)?;

    let mut first_row: Option<(ObjectModel, CacheModel, NarModel)> = None;
    let mut chunks = Vec::new();

    while let Some(row) = rows.next().await.map_err(db_err)? {
        if first_row.is_none() {
            let object = ObjectModel::from_row_prefixed(&row, "o_", 0)
                .map_err(db_err)?;
            let cache_model = CacheModel::from_row_prefixed(&row, "c_", ObjectModel::column_count())
                .map_err(db_err)?;
            let nar = NarModel::from_row_prefixed(
                &row,
                "n_",
                ObjectModel::column_count() + CacheModel::column_count(),
            )
            .map_err(db_err)?;
            first_row = Some((object, cache_model, nar));
        }

        let chunk_start_idx = ObjectModel::column_count() + CacheModel::column_count() + NarModel::column_count();
        let chunk = ChunkModel::try_from_row_prefixed(&row, "ch_", chunk_start_idx)
            .map_err(db_err)?;
        chunks.push(chunk);
    }

    match first_row {
        Some((object, cache_model, nar)) => {
            if chunks.len() != nar.num_chunks as usize {
                return Err(ErrorKind::DatabaseError(anyhow!(
                    "Database returned the wrong number of chunks: Expected {}, got {}",
                    nar.num_chunks,
                    chunks.len()
                ))
                .into());
            }

            Ok((object, cache_model, nar, chunks))
        }
        None => Err(ErrorKind::NoSuchObject.into()),
    }
}

/// Finds and locks a NAR by its hash.
pub async fn find_and_lock_nar(
    conn: Arc<TursoConnection>,
    nar_hash: &Hash,
) -> ServerResult<Option<NarGuard>> {
    let sql = r#"
        UPDATE nar
        SET holders_count = holders_count + 1
        WHERE id = (
            SELECT id FROM nar
            WHERE nar_hash = ?1 AND state = 'V'
            LIMIT 1
        )
        RETURNING id, state, nar_hash, nar_size, compression,
                  num_chunks, completeness_hint, holders_count, created_at
    "#;

    let mut rows = conn.query(sql, [nar_hash.to_typed_base16()])
        .await
        .map_err(db_err)?;

    match rows.next().await.map_err(db_err)? {
        Some(row) => {
            let nar = NarModel::from_row(&row).map_err(db_err)?;
            Ok(Some(NarGuard::new(conn, nar)))
        }
        None => Ok(None),
    }
}

/// Finds and locks a chunk by its hash and compression type.
pub async fn find_and_lock_chunk(
    conn: Arc<TursoConnection>,
    chunk_hash: &Hash,
    compression: Compression,
) -> ServerResult<Option<ChunkGuard>> {
    let sql = r#"
        UPDATE chunk
        SET holders_count = holders_count + 1
        WHERE id = (
            SELECT id FROM chunk
            WHERE chunk_hash = ?1 AND state = 'V' AND compression = ?2
            LIMIT 1
        )
        RETURNING id, state, chunk_hash, chunk_size, file_hash, file_size,
                  compression, remote_file, remote_file_id, holders_count, created_at
    "#;

    let mut rows = conn.query(sql, (chunk_hash.to_typed_base16(), compression.as_str().to_string()))
        .await
        .map_err(db_err)?;

    match rows.next().await.map_err(db_err)? {
        Some(row) => {
            let chunk = ChunkModel::from_row(&row).map_err(db_err)?;
            Ok(Some(ChunkGuard::new(conn, chunk)))
        }
        None => Ok(None),
    }
}

/// Decrements the holders_count of a NAR.
pub async fn decrement_nar_holders(conn: &TursoConnection, nar_id: i64) -> anyhow::Result<()> {
    let sql = "UPDATE nar SET holders_count = holders_count - 1 WHERE id = ?1";
    conn.execute(sql, [nar_id]).await?;
    Ok(())
}

/// Decrements the holders_count of a chunk.
pub async fn decrement_chunk_holders(conn: &TursoConnection, chunk_id: i64) -> anyhow::Result<()> {
    let sql = "UPDATE chunk SET holders_count = holders_count - 1 WHERE id = ?1";
    conn.execute(sql, [chunk_id]).await?;
    Ok(())
}

/// Bumps the last_accessed_at timestamp of an object.
pub async fn bump_object_last_accessed(
    conn: &TursoConnection,
    object_id: i64,
) -> ServerResult<()> {
    let now = Utc::now().to_rfc3339();
    let sql = "UPDATE object SET last_accessed_at = ?1 WHERE id = ?2";

    conn.execute(sql, (now, object_id))
        .await
        .map_err(db_err)?;

    Ok(())
}

// ============================================================================
// Additional query functions for other parts of the codebase
// ============================================================================

/// Creates a new cache.
pub async fn create_cache(
    conn: &TursoConnection,
    name: &str,
    keypair: &str,
    is_public: bool,
    store_dir: &str,
    priority: i32,
    upstream_cache_key_names: &[String],
) -> ServerResult<CacheModel> {
    let now = Utc::now().to_rfc3339();
    let upstream_json = serde_json::to_string(upstream_cache_key_names)
        .map_err(db_err)?;
    let is_public_i64 = if is_public { 1i64 } else { 0i64 };

    let sql = r#"
        INSERT INTO cache (name, keypair, is_public, store_dir, priority,
                          upstream_cache_key_names, created_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        RETURNING id, name, keypair, is_public, store_dir, priority,
                  upstream_cache_key_names, created_at, deleted_at, retention_period
    "#;

    let mut rows = conn.query(sql, (
        name,
        keypair,
        is_public_i64,
        store_dir,
        priority as i64,
        upstream_json.as_str(),
        now.as_str(),
    ))
        .await
        .map_err(db_err)?;

    match rows.next().await.map_err(db_err)? {
        Some(row) => CacheModel::from_row(&row).map_err(db_err),
        None => Err(ErrorKind::DatabaseError(anyhow!("Failed to create cache")).into()),
    }
}

/// Creates a new NAR.
pub async fn create_nar(
    conn: &TursoConnection,
    nar_hash: &str,
    nar_size: i64,
    compression: &str,
    num_chunks: i32,
    state: NarState,
) -> ServerResult<NarModel> {
    let now = Utc::now().to_rfc3339();

    let sql = r#"
        INSERT INTO nar (state, nar_hash, nar_size, compression, num_chunks,
                        completeness_hint, holders_count, created_at)
        VALUES (?1, ?2, ?3, ?4, ?5, 0, 0, ?6)
        RETURNING id, state, nar_hash, nar_size, compression,
                  num_chunks, completeness_hint, holders_count, created_at
    "#;

    let mut rows = conn.query(sql, (
        state.to_db_value(),
        nar_hash,
        nar_size,
        compression,
        num_chunks as i64,
        now.as_str(),
    ))
        .await
        .map_err(db_err)?;

    match rows.next().await.map_err(db_err)? {
        Some(row) => NarModel::from_row(&row).map_err(db_err),
        None => Err(ErrorKind::DatabaseError(anyhow!("Failed to create NAR")).into()),
    }
}

/// Updates the state of a NAR.
pub async fn update_nar_state(
    conn: &TursoConnection,
    nar_id: i64,
    state: NarState,
) -> ServerResult<()> {
    let sql = "UPDATE nar SET state = ?1 WHERE id = ?2";
    conn.execute(sql, (state.to_db_value(), nar_id))
        .await
        .map_err(db_err)?;
    Ok(())
}

/// Updates the state of a chunk.
pub async fn update_chunk_state(
    conn: &TursoConnection,
    chunk_id: i64,
    state: ChunkState,
) -> ServerResult<()> {
    let sql = "UPDATE chunk SET state = ?1 WHERE id = ?2";
    conn.execute(sql, (state.to_db_value(), chunk_id))
        .await
        .map_err(db_err)?;
    Ok(())
}

/// Updates NAR completeness hint.
pub async fn update_nar_completeness_hint(
    conn: &TursoConnection,
    nar_id: i64,
    completeness_hint: bool,
) -> ServerResult<()> {
    let sql = "UPDATE nar SET completeness_hint = ?1 WHERE id = ?2";
    let hint = if completeness_hint { 1i64 } else { 0i64 };
    conn.execute(sql, (hint, nar_id))
        .await
        .map_err(db_err)?;
    Ok(())
}

/// Deletes a cache (soft delete).
pub async fn soft_delete_cache(conn: &TursoConnection, cache_id: i64) -> ServerResult<()> {
    let now = Utc::now().to_rfc3339();
    let sql = "UPDATE cache SET deleted_at = ?1 WHERE id = ?2";
    conn.execute(sql, (now, cache_id))
        .await
        .map_err(db_err)?;
    Ok(())
}

/// Deletes a cache (hard delete).
pub async fn hard_delete_cache(conn: &TursoConnection, cache_id: i64) -> ServerResult<()> {
    let sql = "DELETE FROM cache WHERE id = ?1";
    conn.execute(sql, [cache_id]).await.map_err(db_err)?;
    Ok(())
}
