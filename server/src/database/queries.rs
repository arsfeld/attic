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
            o."references", o.system, o.deriver, o.sigs, o.ca,
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
            o."references", o.system, o.deriver, o.sigs, o.ca,
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
            (nar_id, seq as i64, chunk_id_str, chunk_hash, compression),
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
        Some(row) => Ok(Some(
            super::models::ChunkRefModel::from_row(&row).map_err(db_err)?,
        )),
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
    // Note: "references" is a SQL reserved keyword and must be quoted
    let sql = r#"
        INSERT INTO object (cache_id, nar_id, store_path_hash, store_path,
                           "references", system, deriver, sigs, ca, created_at, created_by)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
        ON CONFLICT(cache_id, store_path_hash) DO UPDATE SET
            nar_id = excluded.nar_id,
            store_path = excluded.store_path,
            "references" = excluded."references",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::connection::TursoConfig;
    use crate::database::migrations::run_migrations;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Helper to create a test database with migrations applied.
    async fn create_test_db() -> (std::sync::Arc<TursoConnection>, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test.db");

        let config = TursoConfig {
            url: format!("sqlite://{}", db_path.display()),
            auth_token: None,
            local_replica_path: None,
            sync_interval: Duration::from_secs(60),
        };

        let conn = TursoConnection::connect(config)
            .await
            .expect("Failed to connect");

        run_migrations(&conn).await.expect("Migrations failed");

        (conn, temp_dir)
    }

    // ==================== Cache Query Tests ====================

    #[tokio::test]
    async fn test_create_cache() {
        let (conn, _temp_dir) = create_test_db().await;

        let cache = create_cache(
            &conn,
            "test-cache",
            "test-keypair",
            true,
            "/nix/store",
            40,
            &[],
        )
        .await
        .expect("Create cache failed");

        assert_eq!(cache.name, "test-cache");
        assert_eq!(cache.keypair, "test-keypair");
        assert!(cache.is_public);
        assert_eq!(cache.store_dir, "/nix/store");
        assert_eq!(cache.priority, 40);
        assert!(cache.deleted_at.is_none());
    }

    #[tokio::test]
    async fn test_create_cache_with_upstream() {
        let (conn, _temp_dir) = create_test_db().await;

        let upstream = vec!["cache.nixos.org-1".to_string()];
        let cache = create_cache(
            &conn,
            "test-cache",
            "test-keypair",
            false,
            "/nix/store",
            30,
            &upstream,
        )
        .await
        .expect("Create cache failed");

        assert!(!cache.is_public);
        // upstream_cache_key_names is stored as Json<Vec<String>>
        assert_eq!(cache.upstream_cache_key_names.0, upstream);
    }

    #[tokio::test]
    async fn test_find_cache() {
        let (conn, _temp_dir) = create_test_db().await;

        // Create a cache first
        create_cache(&conn, "my-cache", "keypair", true, "/nix/store", 40, &[])
            .await
            .expect("Create failed");

        // Find it
        let cache_name = "my-cache".parse().expect("Invalid cache name");
        let found = find_cache(&conn, &cache_name).await.expect("Find failed");

        assert_eq!(found.name, "my-cache");
    }

    #[tokio::test]
    async fn test_find_cache_not_found() {
        let (conn, _temp_dir) = create_test_db().await;

        let cache_name = "nonexistent".parse().expect("Invalid cache name");
        let result = find_cache(&conn, &cache_name).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_soft_delete_cache() {
        let (conn, _temp_dir) = create_test_db().await;

        let cache = create_cache(&conn, "to-delete", "keypair", true, "/nix/store", 40, &[])
            .await
            .expect("Create failed");

        // Soft delete
        soft_delete_cache(&conn, cache.id)
            .await
            .expect("Delete failed");

        // Should not be findable now
        let cache_name = "to-delete".parse().expect("Invalid cache name");
        let result = find_cache(&conn, &cache_name).await;
        assert!(result.is_err());

        // But should still exist in database
        let mut rows = conn
            .query("SELECT deleted_at FROM cache WHERE id = ?1", [cache.id])
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let deleted_at: Option<String> = row.get(0).ok();
        assert!(deleted_at.is_some());
    }

    #[tokio::test]
    async fn test_hard_delete_cache() {
        let (conn, _temp_dir) = create_test_db().await;

        let cache = create_cache(&conn, "to-delete", "keypair", true, "/nix/store", 40, &[])
            .await
            .expect("Create failed");

        hard_delete_cache(&conn, cache.id)
            .await
            .expect("Delete failed");

        // Should be completely gone
        let mut rows = conn
            .query("SELECT COUNT(*) FROM cache WHERE id = ?1", [cache.id])
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let count: i64 = row.get(0).expect("Get failed");
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_insert_cache_conflict() {
        let (conn, _temp_dir) = create_test_db().await;

        // Insert first time
        let affected1 = insert_cache(&conn, "dup-cache", "kp1", true, "/nix/store", 40, "[]")
            .await
            .expect("First insert failed");
        assert_eq!(affected1, 1);

        // Insert again with same name - should be ignored
        let affected2 = insert_cache(&conn, "dup-cache", "kp2", false, "/other/store", 50, "[]")
            .await
            .expect("Second insert failed");
        assert_eq!(affected2, 0);

        // Original should be unchanged
        let cache_name = "dup-cache".parse().expect("Invalid cache name");
        let cache = find_cache(&conn, &cache_name).await.expect("Find failed");
        assert_eq!(cache.keypair, "kp1");
        assert!(cache.is_public);
    }

    // ==================== NAR Query Tests ====================

    #[tokio::test]
    async fn test_create_nar() {
        let (conn, _temp_dir) = create_test_db().await;

        let nar = create_nar(
            &conn,
            "sha256:abc123",
            1024,
            "zstd",
            1,
            NarState::PendingUpload,
        )
        .await
        .expect("Create NAR failed");

        assert_eq!(nar.nar_hash, "sha256:abc123");
        assert_eq!(nar.nar_size, 1024);
        assert_eq!(nar.compression, "zstd");
        assert_eq!(nar.num_chunks, 1);
        assert_eq!(nar.state, NarState::PendingUpload);
        assert_eq!(nar.holders_count, 0);
    }

    #[tokio::test]
    async fn test_update_nar_state() {
        let (conn, _temp_dir) = create_test_db().await;

        let nar = create_nar(
            &conn,
            "sha256:def456",
            2048,
            "none",
            1,
            NarState::PendingUpload,
        )
        .await
        .expect("Create NAR failed");

        // Update to Valid
        update_nar_state(&conn, nar.id, NarState::Valid)
            .await
            .expect("Update failed");

        // Verify
        let mut rows = conn
            .query("SELECT state FROM nar WHERE id = ?1", [nar.id])
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let state: String = row.get(0).expect("Get failed");
        assert_eq!(state, "V");
    }

    #[tokio::test]
    async fn test_update_nar_completeness_hint() {
        let (conn, _temp_dir) = create_test_db().await;

        let nar = create_nar(&conn, "sha256:ghi789", 512, "brotli", 2, NarState::Valid)
            .await
            .expect("Create NAR failed");

        // Default is 0 (from migration)
        update_nar_completeness_hint(&conn, nar.id, true)
            .await
            .expect("Update failed");

        let mut rows = conn
            .query("SELECT completeness_hint FROM nar WHERE id = ?1", [nar.id])
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let hint: i64 = row.get(0).expect("Get failed");
        assert_eq!(hint, 1);
    }

    #[tokio::test]
    async fn test_decrement_nar_holders() {
        let (conn, _temp_dir) = create_test_db().await;

        // Create NAR and manually set holders_count
        let nar = create_nar(&conn, "sha256:jkl012", 100, "none", 1, NarState::Valid)
            .await
            .expect("Create NAR failed");

        conn.execute("UPDATE nar SET holders_count = 5 WHERE id = ?1", [nar.id])
            .await
            .expect("Update failed");

        // Decrement
        decrement_nar_holders(&conn, nar.id)
            .await
            .expect("Decrement failed");

        let mut rows = conn
            .query("SELECT holders_count FROM nar WHERE id = ?1", [nar.id])
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let count: i64 = row.get(0).expect("Get failed");
        assert_eq!(count, 4);
    }

    // ==================== Object Query Tests ====================

    #[tokio::test]
    async fn test_bump_object_last_accessed() {
        let (conn, _temp_dir) = create_test_db().await;

        // Create cache and NAR first
        let cache = create_cache(&conn, "test-cache", "keypair", true, "/nix/store", 40, &[])
            .await
            .expect("Create cache failed");
        let nar = create_nar(&conn, "sha256:obj123", 500, "none", 1, NarState::Valid)
            .await
            .expect("Create NAR failed");

        // Insert an object
        conn.execute(
            r#"INSERT INTO object (cache_id, nar_id, store_path_hash, store_path, "references", sigs, created_at)
               VALUES (?1, ?2, 'abc123', '/nix/store/abc123-pkg', '[]', '[]', datetime('now'))"#,
            (cache.id, nar.id),
        )
        .await
        .expect("Insert object failed");

        // Get object id
        let mut rows = conn
            .query("SELECT id FROM object WHERE store_path_hash = 'abc123'", ())
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let object_id: i64 = row.get(0).expect("Get failed");

        // Initially last_accessed_at should be NULL
        let mut rows = conn
            .query(
                "SELECT last_accessed_at FROM object WHERE id = ?1",
                [object_id],
            )
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let initial: Option<String> = row.get(0).ok();
        assert!(initial.is_none());

        // Bump it
        bump_object_last_accessed(&conn, object_id)
            .await
            .expect("Bump failed");

        // Now should be set
        let mut rows = conn
            .query(
                "SELECT last_accessed_at FROM object WHERE id = ?1",
                [object_id],
            )
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let updated: String = row.get(0).expect("Get failed");
        assert!(!updated.is_empty());
    }

    // ==================== Chunk Query Tests ====================

    #[tokio::test]
    async fn test_decrement_chunk_holders() {
        let (conn, _temp_dir) = create_test_db().await;

        // Insert a chunk directly
        conn.execute(
            r#"INSERT INTO chunk (state, chunk_hash, chunk_size, compression, remote_file, remote_file_id, holders_count, created_at)
               VALUES ('V', 'sha256:chunk1', 256, 'zstd', 'chunks/c1.zst', 'uuid-c1', 3, datetime('now'))"#,
            (),
        )
        .await
        .expect("Insert chunk failed");

        let mut rows = conn
            .query(
                "SELECT id FROM chunk WHERE chunk_hash = 'sha256:chunk1'",
                (),
            )
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let chunk_id: i64 = row.get(0).expect("Get failed");

        // Decrement
        decrement_chunk_holders(&conn, chunk_id)
            .await
            .expect("Decrement failed");

        let mut rows = conn
            .query("SELECT holders_count FROM chunk WHERE id = ?1", [chunk_id])
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let count: i64 = row.get(0).expect("Get failed");
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_update_chunk_state() {
        let (conn, _temp_dir) = create_test_db().await;

        conn.execute(
            r#"INSERT INTO chunk (state, chunk_hash, chunk_size, compression, remote_file, remote_file_id, holders_count, created_at)
               VALUES ('P', 'sha256:chunk2', 128, 'none', 'chunks/c2', 'uuid-c2', 0, datetime('now'))"#,
            (),
        )
        .await
        .expect("Insert chunk failed");

        let mut rows = conn
            .query(
                "SELECT id FROM chunk WHERE chunk_hash = 'sha256:chunk2'",
                (),
            )
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let chunk_id: i64 = row.get(0).expect("Get failed");

        // Update state
        update_chunk_state(&conn, chunk_id, ChunkState::Valid)
            .await
            .expect("Update failed");

        let mut rows = conn
            .query("SELECT state FROM chunk WHERE id = ?1", [chunk_id])
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let state: String = row.get(0).expect("Get failed");
        assert_eq!(state, "V");
    }

    // ==================== Update Cache Tests ====================

    #[tokio::test]
    async fn test_update_cache() {
        let (conn, _temp_dir) = create_test_db().await;

        let cache = create_cache(&conn, "update-test", "keypair", true, "/nix/store", 40, &[])
            .await
            .expect("Create cache failed");

        // Update multiple fields (keypair, is_public, store_dir, priority, upstream, retention)
        let affected = update_cache(
            &conn,
            cache.id,
            None,                 // keypair
            Some(false),          // is_public
            Some("/other/store"), // store_dir
            Some(50),             // priority
            None,                 // upstream_cache_key_names
            Some(Some(86400)),    // retention_period
        )
        .await
        .expect("Update failed");

        assert_eq!(affected, 1);

        // Verify changes
        let cache_name = "update-test".parse().expect("Invalid cache name");
        let updated = find_cache(&conn, &cache_name).await.expect("Find failed");

        assert!(!updated.is_public);
        assert_eq!(updated.store_dir, "/other/store");
        assert_eq!(updated.priority, 50);
        assert_eq!(updated.retention_period, Some(86400));
    }

    #[tokio::test]
    async fn test_update_cache_no_changes() {
        let (conn, _temp_dir) = create_test_db().await;

        let cache = create_cache(&conn, "no-change", "keypair", true, "/nix/store", 40, &[])
            .await
            .expect("Create cache failed");

        // Update with no fields
        let affected = update_cache(&conn, cache.id, None, None, None, None, None, None)
            .await
            .expect("Update failed");

        assert_eq!(affected, 0);
    }

    #[tokio::test]
    async fn test_update_cache_clear_retention() {
        let (conn, _temp_dir) = create_test_db().await;

        let cache = create_cache(
            &conn,
            "retention-test",
            "keypair",
            true,
            "/nix/store",
            40,
            &[],
        )
        .await
        .expect("Create cache failed");

        // Set retention period
        update_cache(
            &conn,
            cache.id,
            None,
            None,
            None,
            None,
            None,
            Some(Some(3600)),
        )
        .await
        .expect("Set retention failed");

        // Clear it
        update_cache(&conn, cache.id, None, None, None, None, None, Some(None))
            .await
            .expect("Clear retention failed");

        let cache_name = "retention-test".parse().expect("Invalid cache name");
        let updated = find_cache(&conn, &cache_name).await.expect("Find failed");
        assert!(updated.retention_period.is_none());
    }

    // ==================== Integration Tests for Bug Replication ====================

    /// Test that insert_object_upsert works correctly.
    /// This tests the SQL syntax with the `references` column (reserved keyword).
    /// BUG: `references` is a SQL reserved keyword and must be quoted as `"references"`.
    #[tokio::test]
    async fn test_insert_object_upsert_references_keyword() {
        let (conn, _temp_dir) = create_test_db().await;

        // Create cache and NAR
        let cache = create_cache(&conn, "test-cache", "keypair", true, "/nix/store", 40, &[])
            .await
            .expect("Create cache failed");

        let nar = create_nar(&conn, "sha256:testnar123", 1024, "none", 1, NarState::Valid)
            .await
            .expect("Create NAR failed");

        // This should work but will fail with:
        // "sqlite3 parser error: near REFERENCES, "None": syntax error"
        // because `references` is a SQL reserved keyword
        let result = insert_object_upsert(
            &conn,
            cache.id,
            nar.id,
            "abc123hash",
            "/nix/store/abc123hash-test-pkg",
            r#"["/nix/store/dep1", "/nix/store/dep2"]"#,
            None,
            Some("/nix/store/builder.drv"),
            r#"["sig1", "sig2"]"#,
            None,
            Some("test-user"),
        )
        .await;

        assert!(
            result.is_ok(),
            "insert_object_upsert failed: {:?}",
            result.err()
        );
    }

    /// Test find_object_without_chunks with the `references` column.
    /// BUG: The query uses `o.references` which should be `o."references"`.
    #[tokio::test]
    async fn test_find_object_references_keyword() {
        let (conn, _temp_dir) = create_test_db().await;

        // Create cache, NAR, and object
        let cache = create_cache(&conn, "find-test", "keypair", true, "/nix/store", 40, &[])
            .await
            .expect("Create cache failed");

        let nar = create_nar(&conn, "sha256:findnar456", 2048, "zstd", 1, NarState::Valid)
            .await
            .expect("Create NAR failed");

        // Insert object directly using raw SQL (this works because we quote references)
        // Store path hash must be exactly 32 chars of nix base32: [0123456789abcdfghijklmnpqrsvwxyz]
        let hash_32 = "0123456789abcdfghijklmnpqrsvwxyz"; // exactly 32 valid chars
        conn.execute(
            r#"INSERT INTO object (cache_id, nar_id, store_path_hash, store_path, "references", sigs, created_at)
               VALUES (?1, ?2, ?3, '/nix/store/0123456789abcdfghijklmnpqrsvwxyz-pkg', '[]', '[]', datetime('now'))"#,
            (cache.id, nar.id, hash_32),
        )
        .await
        .expect("Insert object failed");

        // Now try to find it using find_object_and_chunks_by_store_path_hash
        // This will fail because the query uses unquoted `references`
        let cache_name: attic::cache::CacheName = "find-test".parse().expect("Invalid cache name");
        // Store path hash must be exactly 32 characters
        let store_path_hash = attic::nix_store::StorePathHash::new(hash_32.to_string()).unwrap();

        let result =
            find_object_and_chunks_by_store_path_hash(&conn, &cache_name, &store_path_hash, false)
                .await;

        assert!(
            result.is_ok(),
            "find_object_and_chunks_by_store_path_hash failed: {:?}",
            result.err()
        );
    }

    /// Test find_object_with_chunks (include_chunks=true).
    /// BUG: The query uses `o.references` which should be `o."references"`.
    #[tokio::test]
    async fn test_find_object_with_chunks_references_keyword() {
        let (conn, _temp_dir) = create_test_db().await;

        // Create cache, NAR, chunk, chunkref, and object
        let cache = create_cache(&conn, "chunks-test", "keypair", true, "/nix/store", 40, &[])
            .await
            .expect("Create cache failed");

        let nar = create_nar(
            &conn,
            "sha256:chunkednar789",
            4096,
            "zstd",
            1,
            NarState::Valid,
        )
        .await
        .expect("Create NAR failed");

        // Insert chunk (remote_file is a tagged enum: {"Local": {"name": "..."}} or {"S3": {...}})
        let chunk = insert_chunk(
            &conn,
            ChunkState::Valid,
            "sha256:chunk001",
            4096,
            "zstd",
            r#"{"Local":{"name":"chunks/test.zst"}}"#,
            "chunk-uuid-001",
        )
        .await
        .expect("Insert chunk failed");

        // Insert chunkref
        insert_chunkref(&conn, nar.id, 0, Some(chunk.id), "sha256:chunk001", "zstd")
            .await
            .expect("Insert chunkref failed");

        // Insert object directly (store path hash must be exactly 32 chars of nix base32)
        let hash_32 = "zyxwvsrqpnmlkjihgfdcba9876543210"; // exactly 32 valid chars
        conn.execute(
            r#"INSERT INTO object (cache_id, nar_id, store_path_hash, store_path, "references", sigs, created_at)
               VALUES (?1, ?2, ?3, '/nix/store/zyxwvsrqpnmlkjihgfdcba9876543210-pkg', '[]', '[]', datetime('now'))"#,
            (cache.id, nar.id, hash_32),
        )
        .await
        .expect("Insert object failed");

        // Now try to find it with chunks
        let cache_name: attic::cache::CacheName =
            "chunks-test".parse().expect("Invalid cache name");
        let store_path_hash = attic::nix_store::StorePathHash::new(hash_32.to_string()).unwrap();

        let result =
            find_object_and_chunks_by_store_path_hash(&conn, &cache_name, &store_path_hash, true)
                .await;

        assert!(
            result.is_ok(),
            "find_object_with_chunks failed: {:?}",
            result.err()
        );

        let (object, _cache, _nar, chunks) = result.unwrap();
        assert_eq!(object.store_path_hash, hash_32);
        assert_eq!(chunks.len(), 1);
    }

    /// Test transaction commit without active transaction.
    /// BUG: Calling commit() when no transaction is active causes
    /// "SQLite error: cannot commit - no transaction is active"
    #[tokio::test]
    async fn test_commit_without_transaction() {
        let (conn, _temp_dir) = create_test_db().await;

        // Try to commit without starting a transaction
        let result = conn.commit().await;

        // This should fail with "cannot commit - no transaction is active"
        // If it succeeds, that's also acceptable (libSQL might handle this gracefully)
        if let Err(e) = &result {
            let err_str = e.to_string().to_lowercase();
            assert!(
                err_str.contains("no transaction") || err_str.contains("not in a transaction"),
                "Expected 'no transaction' error, got: {}",
                e
            );
        }
    }

    /// Test nested transactions.
    /// BUG: Starting a transaction when one is already active causes
    /// "SQLite error: cannot start a transaction within a transaction"
    #[tokio::test]
    async fn test_nested_transaction_error() {
        let (conn, _temp_dir) = create_test_db().await;

        // Start first transaction
        conn.begin_transaction()
            .await
            .expect("First transaction should start");

        // Try to start another transaction - this should fail
        let result = conn.begin_transaction().await;

        // Clean up - rollback the first transaction
        let _ = conn.rollback().await;

        // The second begin_transaction should have failed
        assert!(
            result.is_err(),
            "Nested transaction should fail, but it succeeded"
        );

        if let Err(e) = result {
            let err_str = e.to_string().to_lowercase();
            assert!(
                err_str.contains("within a transaction")
                    || err_str.contains("nested")
                    || err_str.contains("already"),
                "Expected nested transaction error, got: {}",
                e
            );
        }
    }

    /// Test the full upload flow simulation.
    /// This replicates the upload_path_new_unchunked workflow.
    #[tokio::test]
    async fn test_upload_flow_transaction_handling() {
        let (conn, _temp_dir) = create_test_db().await;

        // Create cache
        let cache = create_cache(&conn, "upload-test", "keypair", true, "/nix/store", 40, &[])
            .await
            .expect("Create cache failed");

        // Simulate chunk upload (outside transaction)
        let chunk = insert_chunk(
            &conn,
            ChunkState::PendingUpload,
            "sha256:uploadchunk",
            1024,
            "zstd",
            r#"{"Local":{"name":"chunks/upload.zst"}}"#,
            "upload-chunk-uuid",
        )
        .await
        .expect("Insert chunk failed");

        // Begin transaction for final updates
        conn.begin_transaction()
            .await
            .expect("Begin transaction failed");

        // Update chunk to valid
        let updated_chunk = update_chunk(
            &conn,
            chunk.id,
            Some(ChunkState::Valid),
            Some("sha256:filehash"),
            Some(512),
            Some(1),
        )
        .await
        .expect("Update chunk failed");

        assert_eq!(updated_chunk.state, ChunkState::Valid);

        // Create NAR
        let nar = insert_nar(&conn, NarState::Valid, "sha256:uploadnar", 1024, "zstd", 1)
            .await
            .expect("Insert NAR failed");

        // Create chunkref
        insert_chunkref(
            &conn,
            nar.id,
            0,
            Some(chunk.id),
            "sha256:uploadchunk",
            "zstd",
        )
        .await
        .expect("Insert chunkref failed");

        // Create object - this is where the references bug would manifest
        let object_result = insert_object_upsert(
            &conn,
            cache.id,
            nar.id,
            "uploadhash789",
            "/nix/store/uploadhash789-test",
            r#"[]"#,
            None,
            None,
            r#"[]"#,
            None,
            Some("uploader"),
        )
        .await;

        match object_result {
            Ok(_) => {
                // Commit should succeed
                conn.commit().await.expect("Commit failed");
            }
            Err(e) => {
                // Rollback and fail the test
                let _ = conn.rollback().await;
                panic!("insert_object_upsert failed: {:?}", e);
            }
        }
    }
}
