//! Cache configuration endpoint.

use anyhow::anyhow;
use axum::extract::{Extension, Json, Path};
use tracing::instrument;

use crate::database::queries;
use crate::error::{ErrorKind, ServerResult};
use crate::{RequestState, State};
use attic::api::v1::cache_config::{
    CacheConfig, CreateCacheRequest, KeypairConfig, RetentionPeriodConfig,
};
use attic::cache::CacheName;
use attic::signing::NixKeypair;

#[instrument(skip_all, fields(cache_name))]
pub(crate) async fn get_cache_config(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Path(cache_name): Path<CacheName>,
) -> ServerResult<Json<CacheConfig>> {
    let database = state.database().await?;
    let cache = req_state
        .auth
        .auth_cache(database, &cache_name, |cache, permission| {
            permission.require_pull()?;
            Ok(cache)
        })
        .await?;

    let public_key = cache.keypair()?.export_public_key();

    let retention_period_config = if let Some(period) = cache.retention_period {
        RetentionPeriodConfig::Period(period as u32)
    } else {
        RetentionPeriodConfig::Global
    };

    Ok(Json(CacheConfig {
        substituter_endpoint: Some(req_state.substituter_endpoint(cache_name)?),
        api_endpoint: Some(req_state.api_endpoint()?),
        keypair: None,
        public_key: Some(public_key),
        is_public: Some(cache.is_public),
        store_dir: Some(cache.store_dir),
        priority: Some(cache.priority),
        upstream_cache_key_names: Some(cache.upstream_cache_key_names.0),
        retention_period: Some(retention_period_config),
    }))
}

#[instrument(skip_all, fields(cache_name, payload))]
pub(crate) async fn configure_cache(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Path(cache_name): Path<CacheName>,
    Json(payload): Json<CacheConfig>,
) -> ServerResult<()> {
    let database = state.database().await?;
    let (cache, permission) = req_state
        .auth
        .auth_cache(database, &cache_name, |cache, permission| {
            permission.require_configure_cache()?;
            Ok((cache, permission.clone()))
        })
        .await?;

    let mut keypair_str = None;
    let mut is_public_val = None;
    let mut store_dir_str = None;
    let mut priority_val = None;
    let mut upstream_json = None;
    let mut retention_period_val: Option<Option<i32>> = None;

    let mut modified = false;

    if let Some(keypair_cfg) = payload.keypair {
        let keypair = match keypair_cfg {
            KeypairConfig::Generate => NixKeypair::generate(cache_name.as_str())?,
            KeypairConfig::Keypair(k) => k,
        };
        keypair_str = Some(keypair.export_keypair());
        modified = true;
    }

    if let Some(is_public) = payload.is_public {
        is_public_val = Some(is_public);
        modified = true;
    }

    if let Some(store_dir) = payload.store_dir {
        store_dir_str = Some(store_dir);
        modified = true;
    }

    if let Some(priority) = payload.priority {
        priority_val = Some(priority);
        modified = true;
    }

    if let Some(upstream_cache_key_names) = payload.upstream_cache_key_names {
        upstream_json = Some(
            serde_json::to_string(&upstream_cache_key_names)
                .map_err(|e| ErrorKind::RequestError(e.into()))?,
        );
        modified = true;
    }

    if let Some(retention_period_config) = payload.retention_period {
        permission.require_configure_cache_retention()?;

        match retention_period_config {
            RetentionPeriodConfig::Global => {
                retention_period_val = Some(None);
            }
            RetentionPeriodConfig::Period(period) => {
                retention_period_val =
                    Some(Some(period.try_into().map_err(|_| {
                        ErrorKind::RequestError(anyhow!("Invalid retention period"))
                    })?));
            }
        }

        modified = true;
    }

    if modified {
        queries::update_cache(
            database,
            cache.id,
            keypair_str.as_deref(),
            is_public_val,
            store_dir_str.as_deref(),
            priority_val,
            upstream_json.as_deref(),
            retention_period_val,
        )
        .await?;

        Ok(())
    } else {
        Err(ErrorKind::RequestError(anyhow!("No modifiable fields were set.")).into())
    }
}

#[instrument(skip_all, fields(cache_name))]
pub(crate) async fn destroy_cache(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Path(cache_name): Path<CacheName>,
) -> ServerResult<()> {
    let database = state.database().await?;
    let cache = req_state
        .auth
        .auth_cache(database, &cache_name, |cache, permission| {
            permission.require_destroy_cache()?;
            Ok(cache)
        })
        .await?;

    if state.config.soft_delete_caches {
        // Perform soft deletion
        let deleted = queries::soft_delete_cache(database, cache.id).await;
        match deleted {
            Ok(()) => Ok(()),
            Err(_) => Err(ErrorKind::NoSuchCache.into()),
        }
    } else {
        // Perform hard deletion
        let deleted = queries::hard_delete_cache(database, cache.id).await;
        match deleted {
            Ok(()) => Ok(()),
            Err(_) => Err(ErrorKind::NoSuchCache.into()),
        }
    }
}

#[instrument(skip_all, fields(cache_name, payload))]
pub(crate) async fn create_cache(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Path(cache_name): Path<CacheName>,
    Json(payload): Json<CreateCacheRequest>,
) -> ServerResult<()> {
    let permission = req_state.auth.get_permission_for_cache(&cache_name, false);
    permission.require_create_cache()?;

    let database = state.database().await?;

    let keypair = match payload.keypair {
        KeypairConfig::Generate => NixKeypair::generate(cache_name.as_str())?,
        KeypairConfig::Keypair(k) => k,
    };

    let upstream_json = serde_json::to_string(&payload.upstream_cache_key_names)
        .map_err(|e| ErrorKind::RequestError(e.into()))?;

    let num_inserted = queries::insert_cache(
        database,
        cache_name.as_str(),
        &keypair.export_keypair(),
        payload.is_public,
        &payload.store_dir,
        payload.priority,
        &upstream_json,
    )
    .await?;

    if num_inserted == 0 {
        // The cache already exists
        Err(ErrorKind::CacheAlreadyExists.into())
    } else {
        Ok(())
    }
}
