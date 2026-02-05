//! Dashboard handler for the web UI.

use askama::Template;
use axum::{
    extract::State as AxumState,
    response::{Html, IntoResponse, Redirect},
};
use axum_extra::extract::cookie::PrivateCookieJar;

use super::auth::get_session_user;
use super::WebUiState;
use crate::database::models::{CacheModel, UserCachePermissionModel, UserModel};
use crate::database::queries;

/// Dashboard template.
#[derive(Template)]
#[template(path = "dashboard.html")]
struct DashboardTemplate {
    user: UserModel,
    caches: Vec<CacheWithStats>,
    total_objects: i64,
}

/// Cache with statistics for display.
pub struct CacheWithStats {
    pub cache: CacheModel,
    pub object_count: i64,
    pub can_push: bool,
    pub can_pull: bool,
    pub can_delete: bool,
}

/// GET /ui or /ui/dashboard - Show the dashboard.
pub async fn dashboard(
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

    // Get user permissions
    let permissions = queries::get_user_permissions(db, user.id)
        .await
        .unwrap_or_default();

    // Get all caches
    let all_caches = queries::list_all_caches(db).await.unwrap_or_default();

    // Filter caches based on permissions
    let mut caches_with_stats = Vec::new();
    let mut total_objects = 0i64;

    for cache in all_caches {
        // Check if user has access (admin has access to all)
        let (can_pull, can_push) = if user.is_admin {
            (true, true)
        } else if cache.is_public {
            // Public caches are readable by anyone with an account
            let push = has_permission_for_cache(&permissions, &cache.name, |p| p.can_push);
            (true, push)
        } else {
            // Private caches require explicit permission
            let pull = has_permission_for_cache(&permissions, &cache.name, |p| p.can_pull);
            let push = has_permission_for_cache(&permissions, &cache.name, |p| p.can_push);
            (pull, push)
        };

        // Skip caches the user can't access at all
        if !can_pull && !can_push && !user.is_admin {
            continue;
        }

        // Get object count
        let object_count = queries::count_objects_in_cache(db, cache.id)
            .await
            .unwrap_or(0);
        total_objects += object_count;

        caches_with_stats.push(CacheWithStats {
            cache,
            object_count,
            can_push,
            can_pull,
            can_delete: false, // Dashboard doesn't need delete functionality
        });
    }

    let template = DashboardTemplate {
        user,
        caches: caches_with_stats,
        total_objects,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|_| "Template error".to_string()),
    )
    .into_response()
}

/// Checks if a user has a specific permission for a cache.
fn has_permission_for_cache<F>(
    permissions: &[UserCachePermissionModel],
    cache_name: &str,
    check: F,
) -> bool
where
    F: Fn(&UserCachePermissionModel) -> bool,
{
    for perm in permissions {
        // Exact match
        if perm.cache_name == cache_name {
            return check(perm);
        }

        // Wildcard match (e.g., "team-*" matches "team-frontend")
        if perm.cache_name.ends_with('*') {
            let prefix = &perm.cache_name[..perm.cache_name.len() - 1];
            if cache_name.starts_with(prefix) && check(perm) {
                return true;
            }
        }
    }

    false
}
