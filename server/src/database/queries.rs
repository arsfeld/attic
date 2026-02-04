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
pub async fn find_cache(conn: &TursoConnection, cache: &CacheName) -> ServerResult<CacheModel> {
    let sql = r#"
        SELECT id, name, keypair, is_public, store_dir, priority,
               upstream_cache_key_names, created_at, deleted_at, retention_period
        FROM cache
        WHERE name = ?1 AND deleted_at IS NULL
    "#;

    let mut rows = conn.query(sql, [cache.as_str()]).await.map_err(db_err)?;

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

    let mut rows = conn
        .query(sql, (cache.as_str(), store_path_hash.as_str()))
        .await
        .map_err(db_err)?;

    match rows.next().await.map_err(db_err)? {
        Some(row) => {
            let object = ObjectModel::from_row_prefixed(&row, "o_", 0).map_err(db_err)?;
            let cache_model =
                CacheModel::from_row_prefixed(&row, "c_", ObjectModel::column_count())
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

    let mut rows = conn
        .query(sql, (cache.as_str(), store_path_hash.as_str()))
        .await
        .map_err(db_err)?;

    let mut first_row: Option<(ObjectModel, CacheModel, NarModel)> = None;
    let mut chunks = Vec::new();

    while let Some(row) = rows.next().await.map_err(db_err)? {
        if first_row.is_none() {
            let object = ObjectModel::from_row_prefixed(&row, "o_", 0).map_err(db_err)?;
            let cache_model =
                CacheModel::from_row_prefixed(&row, "c_", ObjectModel::column_count())
                    .map_err(db_err)?;
            let nar = NarModel::from_row_prefixed(
                &row,
                "n_",
                ObjectModel::column_count() + CacheModel::column_count(),
            )
            .map_err(db_err)?;
            first_row = Some((object, cache_model, nar));
        }

        let chunk_start_idx =
            ObjectModel::column_count() + CacheModel::column_count() + NarModel::column_count();
        let chunk =
            ChunkModel::try_from_row_prefixed(&row, "ch_", chunk_start_idx).map_err(db_err)?;
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

    let mut rows = conn
        .query(sql, [nar_hash.to_typed_base16()])
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

    let mut rows = conn
        .query(
            sql,
            (
                chunk_hash.to_typed_base16(),
                compression.as_str().to_string(),
            ),
        )
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
pub async fn bump_object_last_accessed(conn: &TursoConnection, object_id: i64) -> ServerResult<()> {
    let now = Utc::now().to_rfc3339();
    let sql = "UPDATE object SET last_accessed_at = ?1 WHERE id = ?2";

    conn.execute(sql, (now, object_id)).await.map_err(db_err)?;

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
    let upstream_json = serde_json::to_string(upstream_cache_key_names).map_err(db_err)?;
    let is_public_i64 = if is_public { 1i64 } else { 0i64 };

    let sql = r#"
        INSERT INTO cache (name, keypair, is_public, store_dir, priority,
                          upstream_cache_key_names, created_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        RETURNING id, name, keypair, is_public, store_dir, priority,
                  upstream_cache_key_names, created_at, deleted_at, retention_period
    "#;

    let mut rows = conn
        .query(
            sql,
            (
                name,
                keypair,
                is_public_i64,
                store_dir,
                priority as i64,
                upstream_json.as_str(),
                now.as_str(),
            ),
        )
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

    let mut rows = conn
        .query(
            sql,
            (
                state.to_db_value(),
                nar_hash,
                nar_size,
                compression,
                num_chunks as i64,
                now.as_str(),
            ),
        )
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
    conn.execute(sql, (hint, nar_id)).await.map_err(db_err)?;
    Ok(())
}

/// Deletes a cache (soft delete).
pub async fn soft_delete_cache(conn: &TursoConnection, cache_id: i64) -> ServerResult<()> {
    let now = Utc::now().to_rfc3339();
    let sql = "UPDATE cache SET deleted_at = ?1 WHERE id = ?2";
    conn.execute(sql, (now, cache_id)).await.map_err(db_err)?;
    Ok(())
}

/// Deletes a cache (hard delete).
pub async fn hard_delete_cache(conn: &TursoConnection, cache_id: i64) -> ServerResult<()> {
    let sql = "DELETE FROM cache WHERE id = ?1";
    conn.execute(sql, [cache_id]).await.map_err(db_err)?;
    Ok(())
}

// ============================================================================
// NAR operations for upload_path.rs
// ============================================================================

/// Inserts a new NAR and returns the inserted model.
pub async fn insert_nar(
    conn: &TursoConnection,
    state: NarState,
    nar_hash: &str,
    nar_size: i64,
    compression: &str,
    num_chunks: i32,
) -> ServerResult<NarModel> {
    let now = Utc::now().to_rfc3339();

    let sql = r#"
        INSERT INTO nar (state, nar_hash, nar_size, compression, num_chunks,
                        completeness_hint, holders_count, created_at)
        VALUES (?1, ?2, ?3, ?4, ?5, 0, 0, ?6)
        RETURNING id, state, nar_hash, nar_size, compression,
                  num_chunks, completeness_hint, holders_count, created_at
    "#;

    let mut rows = conn
        .query(
            sql,
            (
                state.to_db_value(),
                nar_hash,
                nar_size,
                compression,
                num_chunks as i64,
                now.as_str(),
            ),
        )
        .await
        .map_err(db_err)?;

    match rows.next().await.map_err(db_err)? {
        Some(row) => NarModel::from_row(&row).map_err(db_err),
        None => Err(ErrorKind::DatabaseError(anyhow!("Failed to insert NAR")).into()),
    }
}

/// Updates a NAR's state, num_chunks, and optionally completeness_hint.
pub async fn update_nar(
    conn: &TursoConnection,
    nar_id: i64,
    state: Option<NarState>,
    num_chunks: Option<i32>,
    completeness_hint: Option<bool>,
) -> ServerResult<()> {
    let mut updates = Vec::new();
    let mut params: Vec<String> = Vec::new();

    if let Some(s) = state {
        updates.push(format!("state = ?{}", params.len() + 1));
        params.push(s.to_db_value().to_string());
    }
    if let Some(n) = num_chunks {
        updates.push(format!("num_chunks = ?{}", params.len() + 1));
        params.push(n.to_string());
    }
    if let Some(h) = completeness_hint {
        updates.push(format!("completeness_hint = ?{}", params.len() + 1));
        params.push(if h { "1".to_string() } else { "0".to_string() });
    }

    if updates.is_empty() {
        return Ok(());
    }

    let sql = format!(
        "UPDATE nar SET {} WHERE id = ?{}",
        updates.join(", "),
        params.len() + 1
    );
    params.push(nar_id.to_string());

    // Execute with dynamic params - we need to build a tuple of the right size
    // For simplicity, we'll use execute_batch approach
    let full_sql = substitute_params(&sql, &params);
    conn.execute(&full_sql, ()).await.map_err(db_err)?;

    Ok(())
}

/// Helper to substitute params into SQL for dynamic queries.
fn substitute_params(sql: &str, params: &[String]) -> String {
    let mut result = sql.to_string();
    for (i, param) in params.iter().enumerate() {
        let placeholder = format!("?{}", i + 1);
        // Properly quote string values
        let quoted = if param.parse::<i64>().is_ok() {
            param.clone()
        } else {
            format!("'{}'", param.replace('\'', "''"))
        };
        result = result.replace(&placeholder, &quoted);
    }
    result
}

/// Deletes a NAR by ID.
pub async fn delete_nar(conn: &TursoConnection, nar_id: i64) -> ServerResult<()> {
    let sql = "DELETE FROM nar WHERE id = ?1";
    conn.execute(sql, [nar_id]).await.map_err(db_err)?;
    Ok(())
}

// ============================================================================
// Chunk operations for upload_path.rs
// ============================================================================

/// Inserts a new chunk and returns the inserted model.
pub async fn insert_chunk(
    conn: &TursoConnection,
    state: ChunkState,
    chunk_hash: &str,
    chunk_size: i64,
    compression: &str,
    remote_file_json: &str,
    remote_file_id: &str,
) -> ServerResult<ChunkModel> {
    let now = Utc::now().to_rfc3339();

    let sql = r#"
        INSERT INTO chunk (state, chunk_hash, chunk_size, file_hash, file_size,
                          compression, remote_file, remote_file_id, holders_count, created_at)
        VALUES (?1, ?2, ?3, NULL, NULL, ?4, ?5, ?6, 0, ?7)
        RETURNING id, state, chunk_hash, chunk_size, file_hash, file_size,
                  compression, remote_file, remote_file_id, holders_count, created_at
    "#;

    let mut rows = conn
        .query(
            sql,
            (
                state.to_db_value(),
                chunk_hash,
                chunk_size,
                compression,
                remote_file_json,
                remote_file_id,
                now.as_str(),
            ),
        )
        .await
        .map_err(db_err)?;

    match rows.next().await.map_err(db_err)? {
        Some(row) => ChunkModel::from_row(&row).map_err(db_err),
        None => Err(ErrorKind::DatabaseError(anyhow!("Failed to insert chunk")).into()),
    }
}

/// Updates a chunk's state, file_hash, file_size, and holders_count.
pub async fn update_chunk(
    conn: &TursoConnection,
    chunk_id: i64,
    state: Option<ChunkState>,
    file_hash: Option<&str>,
    file_size: Option<i64>,
    holders_count: Option<i32>,
) -> ServerResult<ChunkModel> {
    let mut updates = Vec::new();

    if let Some(s) = state {
        updates.push(format!("state = '{}'", s.to_db_value()));
    }
    if let Some(h) = file_hash {
        updates.push(format!("file_hash = '{}'", h.replace('\'', "''")));
    }
    if let Some(s) = file_size {
        updates.push(format!("file_size = {}", s));
    }
    if let Some(c) = holders_count {
        updates.push(format!("holders_count = {}", c));
    }

    if updates.is_empty() {
        // Just fetch the current chunk
        let sql = r#"
            SELECT id, state, chunk_hash, chunk_size, file_hash, file_size,
                   compression, remote_file, remote_file_id, holders_count, created_at
            FROM chunk WHERE id = ?1
        "#;
        let mut rows = conn.query(sql, [chunk_id]).await.map_err(db_err)?;
        match rows.next().await.map_err(db_err)? {
            Some(row) => return ChunkModel::from_row(&row).map_err(db_err),
            None => return Err(ErrorKind::DatabaseError(anyhow!("Chunk not found")).into()),
        }
    }

    let sql = format!(
        r#"UPDATE chunk SET {}
           WHERE id = {}
           RETURNING id, state, chunk_hash, chunk_size, file_hash, file_size,
                     compression, remote_file, remote_file_id, holders_count, created_at"#,
        updates.join(", "),
        chunk_id
    );

    let mut rows = conn.query(&sql, ()).await.map_err(db_err)?;

    match rows.next().await.map_err(db_err)? {
        Some(row) => ChunkModel::from_row(&row).map_err(db_err),
        None => Err(ErrorKind::DatabaseError(anyhow!("Failed to update chunk")).into()),
    }
}

/// Deletes a chunk by ID.
pub async fn delete_chunk(conn: &TursoConnection, chunk_id: i64) -> ServerResult<()> {
    let sql = "DELETE FROM chunk WHERE id = ?1";
    conn.execute(sql, [chunk_id]).await.map_err(db_err)?;
    Ok(())
}

// ============================================================================
// ChunkRef operations for upload_path.rs
// ============================================================================

/// Inserts a new chunkref.
pub async fn insert_chunkref(
    conn: &TursoConnection,
    nar_id: i64,
    seq: i32,
    chunk_id: Option<i64>,
    chunk_hash: &str,
    compression: &str,
) -> ServerResult<i64> {
    let sql = r#"
        INSERT INTO chunkref (nar_id, seq, chunk_id, chunk_hash, compression)
        VALUES (?1, ?2, ?3, ?4, ?5)
        RETURNING id
    "#;

    let chunk_id_param = chunk_id.map(|id| id.to_string());
    let chunk_id_str = chunk_id_param.as_deref();

    let mut rows = conn
        .query(
            sql,
            (
                nar_id,
                seq as i64,
                chunk_id_str,
                chunk_hash,
                compression,
            ),
        )
        .await
        .map_err(db_err)?;

    match rows.next().await.map_err(db_err)? {
        Some(row) => row.get::<i64>(0).map_err(db_err),
        None => Err(ErrorKind::DatabaseError(anyhow!("Failed to insert chunkref")).into()),
    }
}

/// Finds chunkrefs with null chunk_id for a NAR (missing chunks).
pub async fn find_chunkref_missing_chunk(
    conn: &TursoConnection,
    nar_id: i64,
) -> ServerResult<Option<super::models::ChunkRefModel>> {
    let sql = r#"
        SELECT id, nar_id, seq, chunk_id, chunk_hash, compression
        FROM chunkref
        WHERE nar_id = ?1 AND chunk_id IS NULL
        LIMIT 1
    "#;

    let mut rows = conn.query(sql, [nar_id]).await.map_err(db_err)?;

    match rows.next().await.map_err(db_err)? {
        Some(row) => Ok(Some(super::models::ChunkRefModel::from_row(&row).map_err(db_err)?)),
        None => Ok(None),
    }
}

/// Updates chunkref to point to a specific chunk.
pub async fn update_chunkref_chunk_id(
    conn: &TursoConnection,
    chunkref_id: i64,
    chunk_id: i64,
) -> ServerResult<()> {
    let sql = "UPDATE chunkref SET chunk_id = ?1 WHERE id = ?2";
    conn.execute(sql, (chunk_id, chunkref_id))
        .await
        .map_err(db_err)?;
    Ok(())
}

/// Repairs broken chunkrefs by updating those with null chunk_id that match the given hash and compression.
/// Returns the number of affected rows.
pub async fn update_many_chunkrefs_by_hash(
    conn: &TursoConnection,
    chunk_id: i64,
    chunk_hash: &str,
    compression: &str,
) -> ServerResult<u64> {
    let sql = r#"
        UPDATE chunkref
        SET chunk_id = ?1
        WHERE chunk_id IS NULL
          AND chunk_hash = ?2
          AND compression = ?3
    "#;

    let affected = conn
        .execute(sql, (chunk_id, chunk_hash, compression))
        .await
        .map_err(db_err)?;

    Ok(affected)
}

// ============================================================================
// Object operations for upload_path.rs
// ============================================================================

/// Inserts or updates an object (upsert on store_path_hash + cache_id conflict).
pub async fn insert_object_upsert(
    conn: &TursoConnection,
    cache_id: i64,
    nar_id: i64,
    store_path_hash: &str,
    store_path: &str,
    references_json: &str,
    system: Option<&str>,
    deriver: Option<&str>,
    sigs_json: &str,
    ca: Option<&str>,
    created_by: Option<&str>,
) -> ServerResult<i64> {
    let now = Utc::now().to_rfc3339();

    // SQLite uses INSERT OR REPLACE or ON CONFLICT syntax
    let sql = r#"
        INSERT INTO object (cache_id, nar_id, store_path_hash, store_path,
                           references, system, deriver, sigs, ca, created_at, created_by)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
        ON CONFLICT(cache_id, store_path_hash) DO UPDATE SET
            nar_id = excluded.nar_id,
            store_path = excluded.store_path,
            references = excluded.references,
            system = excluded.system,
            deriver = excluded.deriver,
            sigs = excluded.sigs,
            ca = excluded.ca
        RETURNING id
    "#;

    let mut rows = conn
        .query(
            sql,
            (
                cache_id,
                nar_id,
                store_path_hash,
                store_path,
                references_json,
                system,
                deriver,
                sigs_json,
                ca,
                now.as_str(),
                created_by,
            ),
        )
        .await
        .map_err(db_err)?;

    match rows.next().await.map_err(db_err)? {
        Some(row) => row.get::<i64>(0).map_err(db_err),
        None => Err(ErrorKind::DatabaseError(anyhow!("Failed to insert object")).into()),
    }
}

// ============================================================================
// Garbage collection queries
// ============================================================================

/// Cache info for GC purposes.
pub struct CacheWithRetention {
    pub id: i64,
    pub name: String,
    pub retention_period: i32,
}

/// Finds caches with non-zero retention periods.
pub async fn find_caches_with_retention(
    conn: &TursoConnection,
    default_retention_seconds: i64,
) -> ServerResult<Vec<CacheWithRetention>> {
    let sql = r#"
        SELECT id, name, COALESCE(retention_period, ?1) as retention_period
        FROM cache
        WHERE deleted_at IS NULL
          AND COALESCE(retention_period, ?1) != 0
    "#;

    let mut rows = conn
        .query(sql, [default_retention_seconds])
        .await
        .map_err(db_err)?;

    let mut caches = Vec::new();
    while let Some(row) = rows.next().await.map_err(db_err)? {
        caches.push(CacheWithRetention {
            id: row.get::<i64>(0).map_err(db_err)?,
            name: row.get::<String>(1).map_err(db_err)?,
            retention_period: row.get::<i64>(2).map_err(db_err)? as i32,
        });
    }

    Ok(caches)
}

/// Deletes objects from a cache that are older than the cutoff time.
/// Returns the number of deleted rows.
pub async fn delete_objects_by_cache_and_cutoff(
    conn: &TursoConnection,
    cache_id: i64,
    cutoff: &str,
) -> ServerResult<u64> {
    let sql = r#"
        DELETE FROM object
        WHERE cache_id = ?1
          AND created_at < ?2
          AND (last_accessed_at IS NULL OR last_accessed_at < ?2)
    "#;

    let affected = conn
        .execute(sql, (cache_id, cutoff))
        .await
        .map_err(db_err)?;

    Ok(affected)
}

/// Finds orphan NAR IDs (NARs with no objects referencing them).
pub async fn find_orphan_nar_ids(conn: &TursoConnection) -> ServerResult<Vec<i64>> {
    let sql = r#"
        SELECT n.id
        FROM nar n
        LEFT JOIN object o ON o.nar_id = n.id
        WHERE o.id IS NULL
          AND n.state = 'V'
          AND n.holders_count = 0
    "#;

    let mut rows = conn.query(sql, ()).await.map_err(db_err)?;

    let mut ids = Vec::new();
    while let Some(row) = rows.next().await.map_err(db_err)? {
        ids.push(row.get::<i64>(0).map_err(db_err)?);
    }

    Ok(ids)
}

/// Deletes NARs by their IDs.
/// Returns the number of deleted rows.
pub async fn delete_nars_by_ids(conn: &TursoConnection, nar_ids: &[i64]) -> ServerResult<u64> {
    if nar_ids.is_empty() {
        return Ok(0);
    }

    let placeholders: Vec<String> = nar_ids.iter().map(|id| id.to_string()).collect();
    let sql = format!("DELETE FROM nar WHERE id IN ({})", placeholders.join(", "));

    let affected = conn.execute(&sql, ()).await.map_err(db_err)?;
    Ok(affected)
}

/// Finds orphan chunks (chunks with no chunkrefs referencing them).
pub async fn find_orphan_chunk_ids(conn: &TursoConnection) -> ServerResult<Vec<i64>> {
    let sql = r#"
        SELECT c.id
        FROM chunk c
        LEFT JOIN chunkref cr ON cr.chunk_id = c.id
        WHERE cr.id IS NULL
          AND c.state = 'V'
          AND c.holders_count = 0
    "#;

    let mut rows = conn.query(sql, ()).await.map_err(db_err)?;

    let mut ids = Vec::new();
    while let Some(row) = rows.next().await.map_err(db_err)? {
        ids.push(row.get::<i64>(0).map_err(db_err)?);
    }

    Ok(ids)
}

/// Transitions chunks to Deleted state.
/// Returns the number of affected rows.
pub async fn transition_chunks_to_deleted(
    conn: &TursoConnection,
    chunk_ids: &[i64],
) -> ServerResult<u64> {
    if chunk_ids.is_empty() {
        return Ok(0);
    }

    let placeholders: Vec<String> = chunk_ids.iter().map(|id| id.to_string()).collect();
    let sql = format!(
        "UPDATE chunk SET state = 'D' WHERE id IN ({})",
        placeholders.join(", ")
    );

    let affected = conn.execute(&sql, ()).await.map_err(db_err)?;
    Ok(affected)
}

/// Finds chunks in Deleted state (limited).
pub async fn find_deleted_chunks(
    conn: &TursoConnection,
    limit: u64,
) -> ServerResult<Vec<ChunkModel>> {
    let sql = format!(
        r#"
        SELECT id, state, chunk_hash, chunk_size, file_hash, file_size,
               compression, remote_file, remote_file_id, holders_count, created_at
        FROM chunk
        WHERE state = 'D'
        LIMIT {}
    "#,
        limit
    );

    let mut rows = conn.query(&sql, ()).await.map_err(db_err)?;

    let mut chunks = Vec::new();
    while let Some(row) = rows.next().await.map_err(db_err)? {
        chunks.push(ChunkModel::from_row(&row).map_err(db_err)?);
    }

    Ok(chunks)
}

/// Deletes chunks by their IDs.
/// Returns the number of deleted rows.
pub async fn delete_chunks_by_ids(conn: &TursoConnection, chunk_ids: &[i64]) -> ServerResult<u64> {
    if chunk_ids.is_empty() {
        return Ok(0);
    }

    let placeholders: Vec<String> = chunk_ids.iter().map(|id| id.to_string()).collect();
    let sql = format!(
        "DELETE FROM chunk WHERE id IN ({})",
        placeholders.join(", ")
    );

    let affected = conn.execute(&sql, ()).await.map_err(db_err)?;
    Ok(affected)
}

// ============================================================================
// Queries for get_missing_paths.rs
// ============================================================================

/// Finds objects by store path hashes that have complete NARs.
/// Returns the store_path_hash values that exist in the cache.
pub async fn find_objects_by_store_path_hashes(
    conn: &TursoConnection,
    cache_name: &str,
    store_path_hashes: &[String],
) -> ServerResult<Vec<String>> {
    if store_path_hashes.is_empty() {
        return Ok(Vec::new());
    }

    // Build the IN clause with quoted strings
    let quoted: Vec<String> = store_path_hashes
        .iter()
        .map(|h| format!("'{}'", h.replace('\'', "''")))
        .collect();

    let sql = format!(
        r#"
        SELECT o.store_path_hash
        FROM object o
        INNER JOIN cache c ON o.cache_id = c.id
        INNER JOIN nar n ON o.nar_id = n.id
        WHERE c.name = ?1
          AND c.deleted_at IS NULL
          AND o.store_path_hash IN ({})
          AND n.completeness_hint = 1
    "#,
        quoted.join(", ")
    );

    let mut rows = conn.query(&sql, [cache_name]).await.map_err(db_err)?;

    let mut found = Vec::new();
    while let Some(row) = rows.next().await.map_err(db_err)? {
        found.push(row.get::<String>(0).map_err(db_err)?);
    }

    Ok(found)
}

// ============================================================================
// Cache configuration queries (for cache_config.rs)
// ============================================================================

/// Updates a cache's configuration fields.
pub async fn update_cache(
    conn: &TursoConnection,
    cache_id: i64,
    keypair: Option<&str>,
    is_public: Option<bool>,
    store_dir: Option<&str>,
    priority: Option<i32>,
    upstream_cache_key_names: Option<&str>,
    retention_period: Option<Option<i32>>,
) -> ServerResult<u64> {
    let mut updates = Vec::new();

    if let Some(k) = keypair {
        updates.push(format!("keypair = '{}'", k.replace('\'', "''")));
    }
    if let Some(p) = is_public {
        updates.push(format!("is_public = {}", if p { 1 } else { 0 }));
    }
    if let Some(s) = store_dir {
        updates.push(format!("store_dir = '{}'", s.replace('\'', "''")));
    }
    if let Some(p) = priority {
        updates.push(format!("priority = {}", p));
    }
    if let Some(u) = upstream_cache_key_names {
        updates.push(format!(
            "upstream_cache_key_names = '{}'",
            u.replace('\'', "''")
        ));
    }
    if let Some(rp) = retention_period {
        match rp {
            Some(period) => updates.push(format!("retention_period = {}", period)),
            None => updates.push("retention_period = NULL".to_string()),
        }
    }

    if updates.is_empty() {
        return Ok(0);
    }

    let sql = format!(
        "UPDATE cache SET {} WHERE id = {}",
        updates.join(", "),
        cache_id
    );

    let affected = conn.execute(&sql, ()).await.map_err(db_err)?;
    Ok(affected)
}

/// Inserts a new cache with ON CONFLICT DO NOTHING.
/// Returns the number of rows inserted (0 if cache already exists).
pub async fn insert_cache(
    conn: &TursoConnection,
    name: &str,
    keypair: &str,
    is_public: bool,
    store_dir: &str,
    priority: i32,
    upstream_cache_key_names: &str,
) -> ServerResult<u64> {
    let now = Utc::now().to_rfc3339();
    let is_public_i64 = if is_public { 1i64 } else { 0i64 };

    let sql = r#"
        INSERT INTO cache (name, keypair, is_public, store_dir, priority,
                          upstream_cache_key_names, created_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT(name) DO NOTHING
    "#;

    let affected = conn
        .execute(
            sql,
            (
                name,
                keypair,
                is_public_i64,
                store_dir,
                priority as i64,
                upstream_cache_key_names,
                now.as_str(),
            ),
        )
        .await
        .map_err(db_err)?;

    Ok(affected)
}
