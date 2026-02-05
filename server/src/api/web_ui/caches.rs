//! Cache list handler for the web UI.

use askama::Template;
use axum::{
    extract::{Path, State as AxumState},
    http::StatusCode,
    response::{Html, IntoResponse, Redirect},
    Form, Json,
};
use axum_extra::extract::cookie::PrivateCookieJar;
use serde::{de, Deserialize, Deserializer, Serialize};

use super::auth::get_session_user;
use super::dashboard::CacheWithStats;
use super::permissions::get_effective_permissions;
use super::WebUiState;
use crate::database::models::UserCachePermissionModel;
use crate::database::queries;
use attic::signing::NixKeypair;

/// Cache list template (unified for admin and regular users).
#[derive(Template)]
#[template(path = "caches.html")]
struct CachesTemplate {
    user: crate::database::models::UserModel,
    caches: Vec<CacheWithStats>,
    can_create_cache: bool,
    is_admin: bool,
}

/// Deserialize an empty string as None for Option<i32>.
fn empty_string_as_none<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        None => Ok(None),
        Some(s) if s.is_empty() => Ok(None),
        Some(s) => s.parse::<i32>().map(Some).map_err(de::Error::custom),
    }
}

/// Deserialize an empty string as None for Option<String>.
fn empty_string_as_none_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        None => Ok(None),
        Some(s) if s.is_empty() => Ok(None),
        Some(s) => Ok(Some(s)),
    }
}

/// Request to create a new cache.
#[derive(Debug, Deserialize)]
pub struct CreateCacheRequest {
    pub name: String,
    #[serde(default, deserialize_with = "empty_string_as_none_string")]
    pub store_dir: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub priority: Option<i32>,
    pub is_public: Option<String>,
}

/// API result for cache operations.
#[derive(Debug, Serialize)]
pub struct CacheApiResult {
    pub success: bool,
    pub error: Option<String>,
}

/// GET /ui/caches - Show the cache list (role-adaptive).
///
/// Admin sees all caches with delete capability.
/// Users see only caches they have permissions on.
pub async fn list_caches(
    AxumState(web_ui): AxumState<WebUiState>,
    jar: PrivateCookieJar,
) -> impl IntoResponse {
    // Get session user
    let user = match get_session_user(&web_ui, &jar).await {
        Some(user) => user,
        None => return Redirect::to("/ui/login").into_response(),
    };

    let db = match web_ui.app_state.database().await {
        Ok(db) => db,
        Err(_) => {
            return Html("Database error".to_string()).into_response();
        }
    };

    let is_admin = user.is_admin;

    // Get user permissions (not needed for admin but used for permission checks)
    let permissions = if is_admin {
        Vec::new()
    } else {
        queries::get_user_permissions(db, user.id)
            .await
            .unwrap_or_default()
    };

    // Check if user can create caches (admin always can, or user has permission)
    let can_create_cache = is_admin || user_can_create_any_cache(&permissions);

    // Get all caches
    let all_caches = queries::list_all_caches(db).await.unwrap_or_default();

    // Build cache list with stats
    let mut caches_with_stats = Vec::new();

    for cache in all_caches {
        // Check permissions based on role
        let (can_pull, can_push, can_delete) = if is_admin {
            // Admin has all permissions
            (true, true, true)
        } else {
            let effective = get_effective_permissions(&permissions, &cache.name);
            let can_pull = cache.is_public || effective.can_pull;
            let can_push = effective.can_push;
            let can_delete = effective.can_destroy_cache;
            (can_pull, can_push, can_delete)
        };

        // Skip caches the user can't access at all (non-admin only)
        if !is_admin && !can_pull && !can_push {
            continue;
        }

        // Get object count
        let object_count = queries::count_objects_in_cache(db, cache.id)
            .await
            .unwrap_or(0);

        caches_with_stats.push(CacheWithStats {
            cache,
            object_count,
            can_push,
            can_pull,
            can_delete,
        });
    }

    let template = CachesTemplate {
        user,
        caches: caches_with_stats,
        can_create_cache,
        is_admin,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|_| "Template error".to_string()),
    )
    .into_response()
}

/// Checks if user has permission to create any cache.
fn user_can_create_any_cache(permissions: &[UserCachePermissionModel]) -> bool {
    permissions.iter().any(|p| p.can_create_cache)
}

/// POST /ui/caches - Create a new cache (role-adaptive).
///
/// Admin: creates cache without owner tracking.
/// User: creates cache with owner tracking and grants full permissions.
pub async fn create_cache(
    AxumState(web_ui): AxumState<WebUiState>,
    jar: PrivateCookieJar,
    Form(req): Form<CreateCacheRequest>,
) -> impl IntoResponse {
    // Get session user
    let user = match get_session_user(&web_ui, &jar).await {
        Some(user) => user,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(CacheApiResult {
                    success: false,
                    error: Some("Not authenticated".to_string()),
                }),
            )
                .into_response()
        }
    };

    let db = match web_ui.app_state.database().await {
        Ok(db) => db,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(CacheApiResult {
                    success: false,
                    error: Some("Database error".to_string()),
                }),
            )
                .into_response()
        }
    };

    let is_admin = user.is_admin;

    // Check if user can create caches (admin always can)
    if !is_admin {
        let permissions = queries::get_user_permissions(db, user.id)
            .await
            .unwrap_or_default();

        if !user_can_create_any_cache(&permissions) {
            return (
                StatusCode::FORBIDDEN,
                Json(CacheApiResult {
                    success: false,
                    error: Some("You don't have permission to create caches".to_string()),
                }),
            )
                .into_response();
        }
    }

    // Validate cache name
    let name = req.name.trim();
    if name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(CacheApiResult {
                success: false,
                error: Some("Cache name is required".to_string()),
            }),
        )
            .into_response();
    }

    // Basic validation: alphanumeric, hyphens, underscores, must start with letter
    if !name
        .chars()
        .next()
        .map(|c| c.is_ascii_alphabetic())
        .unwrap_or(false)
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(CacheApiResult {
                success: false,
                error: Some("Cache name must start with a letter".to_string()),
            }),
        )
            .into_response();
    }

    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(CacheApiResult {
                success: false,
                error: Some(
                    "Cache name can only contain letters, numbers, hyphens, and underscores"
                        .to_string(),
                ),
            }),
        )
            .into_response();
    }

    // Generate a keypair for the cache
    let keypair = match NixKeypair::generate(name) {
        Ok(kp) => kp,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(CacheApiResult {
                    success: false,
                    error: Some("Failed to generate keypair".to_string()),
                }),
            )
                .into_response()
        }
    };

    let store_dir = req.store_dir.as_deref().unwrap_or("/nix/store");
    let priority = req.priority.unwrap_or(41);
    let is_public = req.is_public.as_deref() == Some("true");

    if is_admin {
        // Admin creates cache without owner tracking
        let result = queries::insert_cache(
            db,
            name,
            &keypair.export_keypair(),
            is_public,
            store_dir,
            priority,
            "[]",
        )
        .await;

        match result {
            Ok(0) => {
                // Cache already exists
                (
                    StatusCode::CONFLICT,
                    Json(CacheApiResult {
                        success: false,
                        error: Some("A cache with this name already exists".to_string()),
                    }),
                )
                    .into_response()
            }
            Ok(_) => Redirect::to("/ui/caches").into_response(),
            Err(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(CacheApiResult {
                    success: false,
                    error: Some("Failed to create cache".to_string()),
                }),
            )
                .into_response(),
        }
    } else {
        // User creates cache with owner tracking
        let result = queries::create_cache_with_owner(
            db,
            name,
            &keypair.export_keypair(),
            is_public,
            store_dir,
            priority,
            &[],
            Some(user.id),
        )
        .await;

        match result {
            Ok(_cache) => {
                // Grant full permissions to the creator
                let perm_result = queries::set_user_permission(
                    db, user.id, name, true, // can_pull
                    true, // can_push
                    true, // can_delete
                    true, // can_create_cache (for sub-patterns if needed)
                    true, // can_configure_cache
                    true, // can_destroy_cache
                )
                .await;

                if let Err(e) = perm_result {
                    tracing::warn!(
                        "Failed to set creator permissions for cache {}: {:?}",
                        name,
                        e
                    );
                    // Don't fail the whole operation - cache was created
                }

                Redirect::to("/ui/caches").into_response()
            }
            Err(e) => {
                // Check if it's a uniqueness constraint violation
                let error_str = format!("{:?}", e);
                if error_str.contains("UNIQUE constraint") || error_str.contains("already exists") {
                    (
                        StatusCode::CONFLICT,
                        Json(CacheApiResult {
                            success: false,
                            error: Some("A cache with this name already exists".to_string()),
                        }),
                    )
                        .into_response()
                } else {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(CacheApiResult {
                            success: false,
                            error: Some("Failed to create cache".to_string()),
                        }),
                    )
                        .into_response()
                }
            }
        }
    }
}

/// DELETE /ui/caches/:name - Delete a cache.
///
/// Admin can delete any cache.
/// User can delete caches they have `can_destroy_cache` permission on.
pub async fn delete_cache(
    AxumState(web_ui): AxumState<WebUiState>,
    jar: PrivateCookieJar,
    Path(cache_name): Path<String>,
) -> impl IntoResponse {
    // Get session user
    let user = match get_session_user(&web_ui, &jar).await {
        Some(user) => user,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(CacheApiResult {
                    success: false,
                    error: Some("Not authenticated".to_string()),
                }),
            )
        }
    };

    let db = match web_ui.app_state.database().await {
        Ok(db) => db,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(CacheApiResult {
                    success: false,
                    error: Some("Database error".to_string()),
                }),
            )
        }
    };

    // Check if user can delete this cache
    if !user.is_admin {
        let permissions = queries::get_user_permissions(db, user.id)
            .await
            .unwrap_or_default();

        let effective = get_effective_permissions(&permissions, &cache_name);

        if !effective.can_destroy_cache {
            return (
                StatusCode::FORBIDDEN,
                Json(CacheApiResult {
                    success: false,
                    error: Some("You don't have permission to delete this cache".to_string()),
                }),
            );
        }
    }

    // Find the cache
    let cache_name_parsed = match cache_name.parse::<attic::cache::CacheName>() {
        Ok(cn) => cn,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(CacheApiResult {
                    success: false,
                    error: Some("Invalid cache name".to_string()),
                }),
            )
        }
    };

    let cache = match queries::find_cache(db, &cache_name_parsed).await {
        Ok(c) => c,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(CacheApiResult {
                    success: false,
                    error: Some("Cache not found".to_string()),
                }),
            )
        }
    };

    // Use soft delete if configured, otherwise hard delete
    let result = if web_ui.app_state.config.soft_delete_caches {
        queries::soft_delete_cache(db, cache.id).await
    } else {
        queries::hard_delete_cache(db, cache.id).await
    };

    match result {
        Ok(()) => (
            StatusCode::OK,
            Json(CacheApiResult {
                success: true,
                error: None,
            }),
        ),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(CacheApiResult {
                success: false,
                error: Some("Failed to delete cache".to_string()),
            }),
        ),
    }
}
