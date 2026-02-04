//! Turso (libSQL) database backend.
//!
//! This module provides an alternative database backend using libSQL/Turso,
//! which offers embedded replicas for low-latency reads and always-on cloud
//! databases without cold starts.

pub mod connection;
pub mod migrations;
pub mod models;
pub mod queries;

use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::task;

use crate::error::ServerResult;

/// A simple error type for Turso database operations that implements std::error::Error.
#[derive(Debug)]
pub struct TursoDbError(pub String);

impl std::fmt::Display for TursoDbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TursoDbError {}
use crate::narinfo::Compression;
use attic::cache::CacheName;
use attic::hash::Hash;
use attic::nix_store::StorePathHash;

use connection::TursoConnection;
use models::{CacheModel, ChunkModel, NarModel, ObjectModel};

/// Trait for database operations.
#[async_trait]
pub trait AtticDatabase: Send + Sync {
    /// Retrieves an object in a binary cache by its store path hash, returning all its chunks.
    async fn find_object_and_chunks_by_store_path_hash(
        &self,
        cache: &CacheName,
        store_path_hash: &StorePathHash,
        include_chunks: bool,
    ) -> ServerResult<(ObjectModel, CacheModel, NarModel, Vec<Option<ChunkModel>>)>;

    /// Retrieves a binary cache.
    async fn find_cache(&self, cache: &CacheName) -> ServerResult<CacheModel>;

    /// Retrieves and locks a valid NAR matching a NAR Hash.
    async fn find_and_lock_nar(&self, nar_hash: &Hash) -> ServerResult<Option<NarGuard>>;

    /// Retrieves and locks a valid chunk matching a chunk Hash.
    async fn find_and_lock_chunk(
        &self,
        chunk_hash: &Hash,
        compression: Compression,
    ) -> ServerResult<Option<ChunkGuard>>;

    /// Bumps the last accessed timestamp of an object.
    async fn bump_object_last_accessed(&self, object_id: i64) -> ServerResult<()>;
}

/// A guard that holds a NAR and decrements its holders_count when dropped.
pub struct NarGuard {
    connection: Arc<TursoConnection>,
    nar: NarModel,
}

/// A guard that holds a chunk and decrements its holders_count when dropped.
pub struct ChunkGuard {
    connection: Arc<TursoConnection>,
    chunk: ChunkModel,
}

impl NarGuard {
    pub fn new(connection: Arc<TursoConnection>, nar: NarModel) -> Self {
        Self { connection, nar }
    }
}

impl Deref for NarGuard {
    type Target = NarModel;

    fn deref(&self) -> &Self::Target {
        &self.nar
    }
}

impl Drop for NarGuard {
    fn drop(&mut self) {
        let connection = self.connection.clone();
        let nar_id = self.nar.id;

        task::spawn(async move {
            tracing::debug!("Unlocking NAR");

            if let Err(e) = queries::decrement_nar_holders(&connection, nar_id).await {
                tracing::warn!("Failed to decrement NAR holders count: {}", e);
            }
        });
    }
}

impl ChunkGuard {
    pub fn new(connection: Arc<TursoConnection>, chunk: ChunkModel) -> Self {
        Self { connection, chunk }
    }

    pub fn from_locked(connection: Arc<TursoConnection>, chunk: ChunkModel) -> Self {
        Self::new(connection, chunk)
    }
}

impl Deref for ChunkGuard {
    type Target = ChunkModel;

    fn deref(&self) -> &Self::Target {
        &self.chunk
    }
}

impl Drop for ChunkGuard {
    fn drop(&mut self) {
        let connection = self.connection.clone();
        let chunk_id = self.chunk.id;

        task::spawn(async move {
            tracing::debug!("Unlocking chunk");

            if let Err(e) = queries::decrement_chunk_holders(&connection, chunk_id).await {
                tracing::warn!("Failed to decrement chunk holders count: {}", e);
            }
        });
    }
}

#[async_trait]
impl AtticDatabase for Arc<TursoConnection> {
    async fn find_object_and_chunks_by_store_path_hash(
        &self,
        cache: &CacheName,
        store_path_hash: &StorePathHash,
        include_chunks: bool,
    ) -> ServerResult<(ObjectModel, CacheModel, NarModel, Vec<Option<ChunkModel>>)> {
        queries::find_object_and_chunks_by_store_path_hash(
            self,
            cache,
            store_path_hash,
            include_chunks,
        )
        .await
    }

    async fn find_cache(&self, cache: &CacheName) -> ServerResult<CacheModel> {
        queries::find_cache(self, cache).await
    }

    async fn find_and_lock_nar(&self, nar_hash: &Hash) -> ServerResult<Option<NarGuard>> {
        queries::find_and_lock_nar(self.clone(), nar_hash).await
    }

    async fn find_and_lock_chunk(
        &self,
        chunk_hash: &Hash,
        compression: Compression,
    ) -> ServerResult<Option<ChunkGuard>> {
        queries::find_and_lock_chunk(self.clone(), chunk_hash, compression).await
    }

    async fn bump_object_last_accessed(&self, object_id: i64) -> ServerResult<()> {
        queries::bump_object_last_accessed(self, object_id).await
    }
}
