use std::io;

use std::io::Cursor;
use std::marker::Unpin;
use std::sync::Arc;

use anyhow::anyhow;
use async_compression::tokio::bufread::{BrotliEncoder, XzEncoder, ZstdEncoder};
use async_compression::Level as CompressionLevel;
use axum::{
    body::Body,
    extract::{Extension, Json},
    http::HeaderMap,
};
use bytes::{Bytes, BytesMut};
use futures::future::join_all;
use futures::StreamExt;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufRead, AsyncReadExt};
use tokio::sync::Semaphore;
use tokio::task::spawn;
use tokio_util::io::StreamReader;
use tracing::instrument;
use uuid::Uuid;

use crate::compression::{CompressionStream, CompressorFn};
use crate::config::CompressionType;
use crate::error::{ErrorKind, ServerError, ServerResult};
use crate::narinfo::Compression;
use crate::{RequestState, State};
use attic::api::v1::upload_path::{
    UploadPathNarInfo, UploadPathResult, UploadPathResultKind, ATTIC_NAR_INFO,
    ATTIC_NAR_INFO_PREAMBLE_SIZE,
};
use attic::chunking::chunk_stream;
use attic::hash::Hash;
use attic::io::{read_chunk_async, HashReader};
use attic::util::Finally;

use crate::database::connection::TursoConnection;
use crate::database::models::{CacheModel, ChunkModel, ChunkState, NarState};
use crate::database::{queries, AtticDatabase, ChunkGuard, TursoDbError};

/// Number of chunks to upload to the storage backend at once.
///
/// TODO: Make this configurable
const CONCURRENT_CHUNK_UPLOADS: usize = 10;

/// Data of a chunk.
enum ChunkData {
    /// Some bytes in memory.
    Bytes(Bytes),

    /// A stream with a user-claimed hash and size that are potentially incorrect.
    Stream(Box<dyn AsyncBufRead + Send + Unpin + 'static>, Hash, usize),
}

/// Result of a chunk upload.
struct UploadChunkResult {
    guard: ChunkGuard,
    deduplicated: bool,
}

/// Uploads a new object to the cache.
///
/// When clients request to upload an object, we first try to increment
/// the `holders_count` of one `nar` row with same NAR hash. If rows were
/// updated, it means the NAR exists in the global cache and we can deduplicate
/// after confirming the NAR hash ("Deduplicate" case). Otherwise, we perform
/// a new upload to the storage backend ("New NAR" case).
#[instrument(skip_all)]
#[axum_macros::debug_handler]
pub(crate) async fn upload_path(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    headers: HeaderMap,
    body: Body,
) -> ServerResult<Json<UploadPathResult>> {
    let stream = body.into_data_stream();
    let mut stream = StreamReader::new(
        stream.map(|r| r.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))),
    );

    let upload_info: UploadPathNarInfo = {
        if let Some(preamble_size_bytes) = headers.get(ATTIC_NAR_INFO_PREAMBLE_SIZE) {
            // Read from the beginning of the PUT body
            let preamble_size: usize = preamble_size_bytes
                .to_str()
                .map_err(|_| {
                    ErrorKind::RequestError(anyhow!(
                        "{} has invalid encoding",
                        ATTIC_NAR_INFO_PREAMBLE_SIZE
                    ))
                })?
                .parse()
                .map_err(|_| {
                    ErrorKind::RequestError(anyhow!(
                        "{} must be a valid unsigned integer",
                        ATTIC_NAR_INFO_PREAMBLE_SIZE
                    ))
                })?;

            if preamble_size > state.config.max_nar_info_size {
                return Err(ErrorKind::RequestError(anyhow!("Upload info is too large")).into());
            }

            let buf = BytesMut::with_capacity(preamble_size);
            let preamble = read_chunk_async(&mut stream, buf)
                .await
                .map_err(|e| ErrorKind::RequestError(e.into()))?;

            if preamble.len() != preamble_size {
                return Err(ErrorKind::RequestError(anyhow!(
                    "Upload info doesn't match specified size"
                ))
                .into());
            }

            serde_json::from_slice(&preamble).map_err(ServerError::request_error)?
        } else if let Some(nar_info_bytes) = headers.get(ATTIC_NAR_INFO) {
            // Read from X-Attic-Nar-Info header
            serde_json::from_slice(nar_info_bytes.as_bytes()).map_err(ServerError::request_error)?
        } else {
            return Err(ErrorKind::RequestError(anyhow!("{} must be set", ATTIC_NAR_INFO)).into());
        }
    };
    let cache_name = &upload_info.cache;

    let database = state.database().await?;
    let cache = req_state
        .auth
        .auth_cache(database, cache_name, |cache, permission| {
            permission.require_push()?;
            Ok(cache)
        })
        .await?;

    let username = req_state.auth.username().map(str::to_string);

    // Try to acquire a lock on an existing NAR
    if let Some(existing_nar) = database.find_and_lock_nar(&upload_info.nar_hash).await? {
        // Deduplicate?
        let missing_chunk = queries::find_chunkref_missing_chunk(database, existing_nar.id).await?;

        if missing_chunk.is_none() {
            // Can actually be deduplicated
            return upload_path_dedup(
                username,
                cache,
                upload_info,
                stream,
                database,
                &state,
                existing_nar,
            )
            .await;
        }
    }

    // New NAR or need to repair
    upload_path_new(username, cache, upload_info, stream, database, &state).await
}

/// Uploads a path when there is already a matching NAR in the global cache.
async fn upload_path_dedup(
    username: Option<String>,
    cache: CacheModel,
    upload_info: UploadPathNarInfo,
    stream: impl AsyncBufRead + Unpin,
    database: &Arc<TursoConnection>,
    state: &State,
    existing_nar: crate::database::NarGuard,
) -> ServerResult<Json<UploadPathResult>> {
    if state.config.require_proof_of_possession {
        let (mut stream, nar_compute) = HashReader::new(stream, Sha256::new());
        tokio::io::copy(&mut stream, &mut tokio::io::sink())
            .await
            .map_err(ServerError::request_error)?;

        // FIXME: errors
        let (nar_hash, nar_size) = nar_compute.get().unwrap();
        let nar_hash = Hash::Sha256(nar_hash.as_slice().try_into().unwrap());

        // Confirm that the NAR Hash and Size are correct
        if nar_hash.to_typed_base16() != existing_nar.nar_hash
            || *nar_size != upload_info.nar_size
            || *nar_size != existing_nar.nar_size as usize
        {
            return Err(ErrorKind::RequestError(anyhow!("Bad NAR Hash or Size")).into());
        }
    }

    // Begin transaction
    let txn = database
        .begin_transaction()
        .await
        .map_err(|e| ServerError::database_error(TursoDbError(e.to_string())))?;

    let result = async {
        // Create a mapping granting the local cache access to the NAR
        let references_json =
            serde_json::to_string(&upload_info.references).map_err(ServerError::request_error)?;
        let sigs_json =
            serde_json::to_string(&upload_info.sigs).map_err(ServerError::request_error)?;

        queries::insert_object_upsert(
            database,
            cache.id,
            existing_nar.id,
            &upload_info.store_path_hash.to_string(),
            &upload_info.store_path,
            &references_json,
            None, // system
            upload_info.deriver.as_deref(),
            &sigs_json,
            upload_info.ca.as_deref(),
            username.as_deref(),
        )
        .await?;

        // Also mark the NAR as complete again
        queries::update_nar_completeness_hint(database, existing_nar.id, true).await?;

        Ok::<(), ServerError>(())
    }
    .await;

    match result {
        Ok(()) => {
            txn.commit()
                .await
                .map_err(|e| ServerError::database_error(TursoDbError(e.to_string())))?;
        }
        Err(e) => {
            let _ = txn.rollback().await;
            return Err(e);
        }
    }

    // Ensure it's not unlocked earlier
    drop(existing_nar);

    Ok(Json(UploadPathResult {
        kind: UploadPathResultKind::Deduplicated,
        file_size: None, // TODO: Sum the chunks
        frac_deduplicated: None,
    }))
}

/// Uploads a path when there is no matching NAR in the global cache.
///
/// It's okay if some other client races to upload the same NAR before
/// us. The `nar` table can hold duplicate NARs which can be deduplicated
/// in a background process.
async fn upload_path_new(
    username: Option<String>,
    cache: CacheModel,
    upload_info: UploadPathNarInfo,
    stream: impl AsyncBufRead + Send + Unpin + 'static,
    database: &Arc<TursoConnection>,
    state: &State,
) -> ServerResult<Json<UploadPathResult>> {
    let nar_size_threshold = state.config.chunking.nar_size_threshold;

    if nar_size_threshold == 0 || upload_info.nar_size < nar_size_threshold {
        upload_path_new_unchunked(username, cache, upload_info, stream, database, state).await
    } else {
        upload_path_new_chunked(username, cache, upload_info, stream, database, state).await
    }
}

/// Uploads a path when there is no matching NAR in the global cache (chunked).
async fn upload_path_new_chunked(
    username: Option<String>,
    cache: CacheModel,
    upload_info: UploadPathNarInfo,
    stream: impl AsyncBufRead + Send + Unpin + 'static,
    database: &Arc<TursoConnection>,
    state: &State,
) -> ServerResult<Json<UploadPathResult>> {
    let chunking_config = &state.config.chunking;
    let compression_config = &state.config.compression;
    let compression_type = compression_config.r#type;
    let compression_level = compression_config.level();
    let compression: Compression = compression_type.into();

    let nar_size_db = i64::try_from(upload_info.nar_size).map_err(ServerError::request_error)?;

    // Create a pending NAR entry
    let nar = queries::insert_nar(
        database,
        NarState::PendingUpload,
        &upload_info.nar_hash.to_typed_base16(),
        nar_size_db,
        compression.as_str(),
        0,
    )
    .await?;
    let nar_id = nar.id;

    let cleanup = Finally::new({
        let database = database.clone();

        async move {
            tracing::warn!("Error occurred - Cleaning up NAR entry");

            if let Err(e) = queries::delete_nar(&database, nar_id).await {
                tracing::warn!("Failed to unregister failed NAR: {}", e);
            }
        }
    });

    let stream = stream.take(upload_info.nar_size as u64);
    let (stream, nar_compute) = HashReader::new(stream, Sha256::new());
    let mut chunks = chunk_stream(
        stream,
        chunking_config.min_size,
        chunking_config.avg_size,
        chunking_config.max_size,
    );

    let upload_chunk_limit = Arc::new(Semaphore::new(CONCURRENT_CHUNK_UPLOADS));
    let mut futures = Vec::new();

    let mut chunk_idx = 0;
    while let Some(bytes) = chunks.next().await {
        let bytes = bytes.map_err(ServerError::request_error)?;
        let data = ChunkData::Bytes(bytes);

        // Wait for a permit before spawning
        //
        // We want to block the receive process as well, otherwise it stays ahead and
        // consumes too much memory
        let permit = upload_chunk_limit.clone().acquire_owned().await.unwrap();
        futures.push({
            let database = database.clone();
            let state = state.clone();
            let require_proof_of_possession = state.config.require_proof_of_possession;

            spawn(async move {
                let chunk = upload_chunk(
                    data,
                    compression_type,
                    compression_level,
                    database.clone(),
                    state,
                    require_proof_of_possession,
                )
                .await?;

                // Create mapping from the NAR to the chunk
                queries::insert_chunkref(
                    &database,
                    nar_id,
                    chunk_idx,
                    Some(chunk.guard.id),
                    &chunk.guard.chunk_hash,
                    &chunk.guard.compression,
                )
                .await?;

                drop(permit);
                Ok(chunk)
            })
        });

        chunk_idx += 1;
    }

    // Confirm that the NAR Hash and Size are correct
    // FIXME: errors
    let (nar_hash, nar_size) = nar_compute.get().unwrap();
    let nar_hash = Hash::Sha256(nar_hash.as_slice().try_into().unwrap());

    if nar_hash != upload_info.nar_hash || *nar_size != upload_info.nar_size {
        return Err(ErrorKind::RequestError(anyhow!("Bad NAR Hash or Size")).into());
    }

    // Wait for all uploads to complete
    let chunks: Vec<UploadChunkResult> = join_all(futures)
        .await
        .into_iter()
        .map(|join_result| join_result.unwrap())
        .collect::<ServerResult<Vec<_>>>()?;

    let (file_size, deduplicated_size) =
        chunks
            .iter()
            .fold((0, 0), |(file_size, deduplicated_size), c| {
                (
                    file_size + c.guard.file_size.unwrap() as usize,
                    if c.deduplicated {
                        deduplicated_size + c.guard.chunk_size as usize
                    } else {
                        deduplicated_size
                    },
                )
            });

    // Begin transaction for final updates
    let txn = database
        .begin_transaction()
        .await
        .map_err(|e| ServerError::database_error(TursoDbError(e.to_string())))?;

    let result = async {
        // Set num_chunks and mark the NAR as Valid
        queries::update_nar(
            database,
            nar_id,
            Some(NarState::Valid),
            Some(chunks.len() as i32),
            None,
        )
        .await?;

        // Create a mapping granting the local cache access to the NAR
        let references_json =
            serde_json::to_string(&upload_info.references).map_err(ServerError::request_error)?;
        let sigs_json =
            serde_json::to_string(&upload_info.sigs).map_err(ServerError::request_error)?;

        queries::insert_object_upsert(
            database,
            cache.id,
            nar_id,
            &upload_info.store_path_hash.to_string(),
            &upload_info.store_path,
            &references_json,
            None, // system
            upload_info.deriver.as_deref(),
            &sigs_json,
            upload_info.ca.as_deref(),
            username.as_deref(),
        )
        .await?;

        Ok::<(), ServerError>(())
    }
    .await;

    match result {
        Ok(()) => {
            txn.commit()
                .await
                .map_err(|e| ServerError::database_error(TursoDbError(e.to_string())))?;
        }
        Err(e) => {
            let _ = txn.rollback().await;
            return Err(e);
        }
    }

    cleanup.cancel();

    Ok(Json(UploadPathResult {
        kind: UploadPathResultKind::Uploaded,
        file_size: Some(file_size),

        // Currently, frac_deduplicated is computed from size before compression
        frac_deduplicated: Some(deduplicated_size as f64 / *nar_size as f64),
    }))
}

/// Uploads a path when there is no matching NAR in the global cache (unchunked).
///
/// We upload the entire NAR as a single chunk.
async fn upload_path_new_unchunked(
    username: Option<String>,
    cache: CacheModel,
    upload_info: UploadPathNarInfo,
    stream: impl AsyncBufRead + Send + Unpin + 'static,
    database: &Arc<TursoConnection>,
    state: &State,
) -> ServerResult<Json<UploadPathResult>> {
    let compression_config = &state.config.compression;
    let compression_type = compression_config.r#type;
    let compression: Compression = compression_type.into();

    // Upload the entire NAR as a single chunk
    let stream = stream.take(upload_info.nar_size as u64);
    let data = ChunkData::Stream(
        Box::new(stream),
        upload_info.nar_hash.clone(),
        upload_info.nar_size,
    );
    let chunk = upload_chunk(
        data,
        compression_type,
        compression_config.level(),
        database.clone(),
        state.clone(),
        state.config.require_proof_of_possession,
    )
    .await?;
    let file_size = chunk.guard.file_size.unwrap() as usize;

    // Begin transaction
    let txn = database
        .begin_transaction()
        .await
        .map_err(|e| ServerError::database_error(TursoDbError(e.to_string())))?;

    let result = async {
        // Create a NAR entry
        let nar = queries::insert_nar(
            database,
            NarState::Valid,
            &upload_info.nar_hash.to_typed_base16(),
            chunk.guard.chunk_size,
            compression.as_str(),
            1,
        )
        .await?;
        let nar_id = nar.id;

        // Create a mapping from the NAR to the chunk
        queries::insert_chunkref(
            database,
            nar_id,
            0,
            Some(chunk.guard.id),
            &upload_info.nar_hash.to_typed_base16(),
            compression.as_str(),
        )
        .await?;

        // Create a mapping granting the local cache access to the NAR
        let references_json =
            serde_json::to_string(&upload_info.references).map_err(ServerError::request_error)?;
        let sigs_json =
            serde_json::to_string(&upload_info.sigs).map_err(ServerError::request_error)?;

        queries::insert_object_upsert(
            database,
            cache.id,
            nar_id,
            &upload_info.store_path_hash.to_string(),
            &upload_info.store_path,
            &references_json,
            None, // system
            upload_info.deriver.as_deref(),
            &sigs_json,
            upload_info.ca.as_deref(),
            username.as_deref(),
        )
        .await?;

        Ok::<(), ServerError>(())
    }
    .await;

    match result {
        Ok(()) => {
            txn.commit()
                .await
                .map_err(|e| ServerError::database_error(TursoDbError(e.to_string())))?;
        }
        Err(e) => {
            let _ = txn.rollback().await;
            return Err(e);
        }
    }

    Ok(Json(UploadPathResult {
        kind: UploadPathResultKind::Uploaded,
        file_size: Some(file_size),
        frac_deduplicated: None,
    }))
}

/// Uploads a chunk with the desired compression.
///
/// This will automatically perform deduplication if the chunk exists.
async fn upload_chunk(
    data: ChunkData,
    compression_type: CompressionType,
    compression_level: CompressionLevel,
    database: Arc<TursoConnection>,
    state: State,
    require_proof_of_possession: bool,
) -> ServerResult<UploadChunkResult> {
    let compression: Compression = compression_type.into();

    let given_chunk_hash = data.hash();
    let given_chunk_size = data.size();

    if let Some(existing_chunk) = database
        .find_and_lock_chunk(&given_chunk_hash, compression)
        .await?
    {
        // There's an existing chunk matching the hash
        if require_proof_of_possession && !data.is_hash_trusted() {
            let stream = data.into_async_buf_read();

            let (mut stream, nar_compute) = HashReader::new(stream, Sha256::new());
            tokio::io::copy(&mut stream, &mut tokio::io::sink())
                .await
                .map_err(ServerError::request_error)?;

            // FIXME: errors
            let (nar_hash, nar_size) = nar_compute.get().unwrap();
            let nar_hash = Hash::Sha256(nar_hash.as_slice().try_into().unwrap());

            // Confirm that the NAR Hash and Size are correct
            if nar_hash.to_typed_base16() != existing_chunk.chunk_hash
                || *nar_size != given_chunk_size
                || *nar_size != existing_chunk.chunk_size as usize
            {
                return Err(ErrorKind::RequestError(anyhow!("Bad chunk hash or size")).into());
            }
        }

        return Ok(UploadChunkResult {
            guard: existing_chunk,
            deduplicated: true,
        });
    }

    let key = format!("{}.chunk", Uuid::new_v4());

    let backend = state.storage().await?;
    let remote_file = backend.make_db_reference(key.clone()).await?;
    let remote_file_id = remote_file.remote_file_id();
    let remote_file_json =
        serde_json::to_string(&remote_file).map_err(ServerError::request_error)?;

    let chunk_size_db = i64::try_from(given_chunk_size).map_err(ServerError::request_error)?;

    let chunk = queries::insert_chunk(
        &database,
        ChunkState::PendingUpload,
        &given_chunk_hash.to_typed_base16(),
        chunk_size_db,
        compression.as_str(),
        &remote_file_json,
        &remote_file_id,
    )
    .await?;
    let chunk_id = chunk.id;

    let cleanup = Finally::new({
        let database = database.clone();
        let backend = backend.clone();
        let key = key.clone();

        async move {
            tracing::warn!("Error occurred - Cleaning up uploaded file and chunk entry");

            if let Err(e) = backend.delete_file(key).await {
                tracing::warn!("Failed to clean up failed upload: {}", e);
            }

            if let Err(e) = queries::delete_chunk(&database, chunk_id).await {
                tracing::warn!("Failed to unregister failed chunk: {}", e);
            }
        }
    });

    // Compress and stream to the storage backend
    let compressor = get_compressor_fn(compression_type, compression_level);
    let mut stream = CompressionStream::new(data.into_async_buf_read(), compressor);

    backend
        .upload_file(key, stream.stream())
        .await
        .map_err(ServerError::storage_error)?;

    // Confirm that the chunk hash is correct
    let (chunk_hash, chunk_size) = stream.nar_hash_and_size().unwrap();
    let (file_hash, file_size) = stream.file_hash_and_size().unwrap();

    let chunk_hash = Hash::Sha256(chunk_hash.as_slice().try_into().unwrap());
    let file_hash = Hash::Sha256(file_hash.as_slice().try_into().unwrap());

    if chunk_hash != given_chunk_hash || *chunk_size != given_chunk_size {
        return Err(ErrorKind::RequestError(anyhow!("Bad chunk hash or size")).into());
    }

    // Begin transaction
    let txn = database
        .begin_transaction()
        .await
        .map_err(|e| ServerError::database_error(TursoDbError(e.to_string())))?;

    let result = async {
        // Update the file hash and size, and set the chunk to valid
        let file_size_db = i64::try_from(*file_size).map_err(ServerError::request_error)?;
        let updated_chunk = queries::update_chunk(
            &database,
            chunk_id,
            Some(ChunkState::Valid),
            Some(&file_hash.to_typed_base16()),
            Some(file_size_db),
            Some(1), // holders_count
        )
        .await?;

        // Also repair broken chunk references pointing at the same chunk
        let repaired = queries::update_many_chunkrefs_by_hash(
            &database,
            chunk_id,
            &chunk_hash.to_typed_base16(),
            compression.as_str(),
        )
        .await?;

        tracing::debug!("Repaired {} chunkrefs", repaired);

        Ok::<ChunkModel, ServerError>(updated_chunk)
    }
    .await;

    match result {
        Ok(chunk) => {
            txn.commit()
                .await
                .map_err(|e| ServerError::database_error(TursoDbError(e.to_string())))?;

            cleanup.cancel();

            let guard = ChunkGuard::from_locked(database.clone(), chunk);

            Ok(UploadChunkResult {
                guard,
                deduplicated: false,
            })
        }
        Err(e) => {
            let _ = txn.rollback().await;
            Err(e)
        }
    }
}

/// Returns a compressor function that takes some stream as input.
fn get_compressor_fn<C: AsyncBufRead + Unpin + Send + 'static>(
    ctype: CompressionType,
    level: CompressionLevel,
) -> CompressorFn<C> {
    match ctype {
        CompressionType::None => Box::new(|c| Box::new(c)),
        CompressionType::Brotli => {
            Box::new(move |s| Box::new(BrotliEncoder::with_quality(s, level)))
        }
        CompressionType::Zstd => Box::new(move |s| Box::new(ZstdEncoder::with_quality(s, level))),
        CompressionType::Xz => Box::new(move |s| Box::new(XzEncoder::with_quality(s, level))),
    }
}

impl ChunkData {
    /// Returns the potentially-incorrect hash of the chunk.
    fn hash(&self) -> Hash {
        match self {
            Self::Bytes(bytes) => {
                let mut hasher = Sha256::new();
                hasher.update(bytes);
                let hash = hasher.finalize();
                Hash::Sha256(hash.as_slice().try_into().unwrap())
            }
            Self::Stream(_, hash, _) => hash.clone(),
        }
    }

    /// Returns the potentially-incorrect size of the chunk.
    fn size(&self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.len(),
            Self::Stream(_, _, size) => *size,
        }
    }

    /// Returns whether the hash is trusted.
    fn is_hash_trusted(&self) -> bool {
        matches!(self, ChunkData::Bytes(_))
    }

    /// Turns the data into an AsyncBufRead.
    fn into_async_buf_read(self) -> Box<dyn AsyncBufRead + Unpin + Send> {
        match self {
            Self::Bytes(bytes) => Box::new(Cursor::new(bytes)),
            Self::Stream(stream, _, _) => stream,
        }
    }
}
