//! Garbage collection.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use chrono::{Duration as ChronoDuration, Utc};
use futures::future::join_all;
use tokio::sync::Semaphore;
use tokio::time;
use tracing::instrument;

use super::{State, StateInner};
use crate::config::Config;
use crate::database::queries;

/// Runs garbage collection periodically.
pub async fn run_garbage_collection(config: Config) {
    let interval = config.garbage_collection.interval;

    if interval == Duration::ZERO {
        // disabled
        return;
    }

    loop {
        // We don't stop even if it errors
        if let Err(e) = run_garbage_collection_once(config.clone()).await {
            tracing::warn!("Garbage collection failed: {}", e);
        }

        time::sleep(interval).await;
    }
}

/// Runs garbage collection once.
#[instrument(skip_all)]
pub async fn run_garbage_collection_once(config: Config) -> Result<()> {
    tracing::info!("Running garbage collection...");

    let state = StateInner::new(config).await;
    run_time_based_garbage_collection(&state).await?;
    run_reap_orphan_nars(&state).await?;
    run_reap_orphan_chunks(&state).await?;

    Ok(())
}

#[instrument(skip_all)]
async fn run_time_based_garbage_collection(state: &State) -> Result<()> {
    let db = state.database().await?;
    let now = Utc::now();

    let default_retention_period = state.config.garbage_collection.default_retention_period;
    let default_retention_seconds = default_retention_period.as_secs() as i64;

    // Find caches with retention periods set
    let caches = queries::find_caches_with_retention(db, default_retention_seconds).await?;

    tracing::info!(
        "Found {} caches subject to time-based garbage collection",
        caches.len()
    );

    let mut objects_deleted = 0u64;

    for cache in caches {
        let period = ChronoDuration::seconds(cache.retention_period.into());
        let cutoff = now.checked_sub_signed(period).ok_or_else(|| {
            anyhow!(
                "Somehow subtracting retention period for cache {} underflowed",
                cache.name
            )
        })?;

        let cutoff_str = cutoff.to_rfc3339();
        let deleted =
            queries::delete_objects_by_cache_and_cutoff(db, cache.id, &cutoff_str).await?;

        tracing::info!(
            "Deleted {} objects from {} (ID {})",
            deleted,
            cache.name,
            cache.id
        );
        objects_deleted += deleted;
    }

    tracing::info!("Deleted {} objects in total", objects_deleted);

    Ok(())
}

#[instrument(skip_all)]
async fn run_reap_orphan_nars(state: &State) -> Result<()> {
    let db = state.database().await?;

    // Find all orphan NARs
    let orphan_nar_ids = queries::find_orphan_nar_ids(db).await?;

    if orphan_nar_ids.is_empty() {
        tracing::info!("No orphan NARs found");
        return Ok(());
    }

    // Delete them
    let deleted = queries::delete_nars_by_ids(db, &orphan_nar_ids).await?;

    tracing::info!("Deleted {} orphan NARs", deleted);

    Ok(())
}

#[instrument(skip_all)]
async fn run_reap_orphan_chunks(state: &State) -> Result<()> {
    let db = state.database().await?;
    let storage = state.storage().await?;

    // SQLite default limit
    let orphan_chunk_limit: u64 = 500;

    // Find all orphan chunks
    let orphan_chunk_ids = queries::find_orphan_chunk_ids(db).await?;

    if orphan_chunk_ids.is_empty() {
        tracing::info!("No orphan chunks found");
        return Ok(());
    }

    // Transition their state to Deleted
    let transitioned = queries::transition_chunks_to_deleted(db, &orphan_chunk_ids).await?;
    tracing::debug!("Transitioned {} chunks to Deleted state", transitioned);

    // Find chunks in Deleted state
    let orphan_chunks = queries::find_deleted_chunks(db, orphan_chunk_limit).await?;

    if orphan_chunks.is_empty() {
        return Ok(());
    }

    // Delete the chunks from remote storage
    let delete_limit = Arc::new(Semaphore::new(20)); // TODO: Make this configurable
    let futures: Vec<_> = orphan_chunks
        .into_iter()
        .map(|chunk| {
            let delete_limit = delete_limit.clone();
            let storage = storage.clone();
            async move {
                let permit = delete_limit.acquire().await?;
                storage.delete_file_db(&chunk.remote_file.0).await?;
                drop(permit);
                Result::<_, anyhow::Error>::Ok(chunk.id)
            }
        })
        .collect();

    // Deletions can result in spurious failures, tolerate them
    //
    // Chunks that failed to be deleted from the remote storage will
    // just be stuck in Deleted state.
    //
    // TODO: Maybe have an interactive command to retry deletions?
    let deleted_chunk_ids: Vec<_> = join_all(futures)
        .await
        .into_iter()
        .filter(|r| {
            if let Err(e) = r {
                tracing::warn!("Deletion failed: {}", e);
            }

            r.is_ok()
        })
        .map(|r| r.unwrap())
        .collect();

    // Finally, delete them from the database
    let deleted = queries::delete_chunks_by_ids(db, &deleted_chunk_ids).await?;

    tracing::info!("Deleted {} orphan chunks", deleted);

    Ok(())
}
