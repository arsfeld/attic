//! Token management handlers for the web UI (unified for admin and regular users).
//!
//! This module provides role-adaptive token generation:
//! - Admins can create tokens with any subject and permissions
//! - Users can only create tokens with their own subject and limited to their permissions

use askama::Template;
use axum::{
    extract::State as AxumState,
    http::StatusCode,
    response::{Html, IntoResponse, Redirect},
    Json,
};
use axum_extra::extract::cookie::PrivateCookieJar;
use chrono::{Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};

use super::auth::get_session_user;
use super::permissions::{get_effective_permissions, intersect_permissions, EffectivePermissions};
use super::WebUiState;
use crate::database::models::{CacheModel, UserCachePermissionModel, UserModel};
use crate::database::queries;
use attic::cache::CacheNamePattern;
use attic_token::{SignatureType, Token};

/// User's cache permission for display in template.
#[derive(Debug)]
pub struct UserCachePermissionDisplay {
    pub cache_name: String,
    pub can_pull: bool,
    pub can_push: bool,
    pub can_delete: bool,
    pub can_create_cache: bool,
    pub can_configure_cache: bool,
    pub can_destroy_cache: bool,
}

/// Permissions actually granted in a token.
#[derive(Debug, Serialize)]
pub struct GrantedPermissions {
    pub can_pull: bool,
    pub can_push: bool,
    pub can_delete: bool,
    pub can_create_cache: bool,
    pub can_configure_cache: bool,
    pub can_destroy_cache: bool,
}

/// Token management template (unified for admin and regular users).
#[derive(Template)]
#[template(path = "tokens.html")]
struct TokensTemplate {
    user: UserModel,
    caches: Vec<CacheModel>,
    permissions: Vec<UserCachePermissionDisplay>,
    is_admin: bool,
}

/// Request to create a new token.
#[derive(Debug, Deserialize)]
pub struct CreateTokenRequest {
    /// Token subject (admin can specify, users get auto-filled).
    pub subject: Option<String>,
    pub cache_pattern: String,
    pub validity: String,
    #[serde(default)]
    pub can_pull: bool,
    #[serde(default)]
    pub can_push: bool,
    #[serde(default)]
    pub can_delete: bool,
    #[serde(default)]
    pub can_create_cache: bool,
    #[serde(default)]
    pub can_configure_cache: bool,
    #[serde(default)]
    pub can_destroy_cache: bool,
}

/// Response for token creation.
#[derive(Debug, Serialize)]
pub struct TokenApiResult {
    pub success: bool,
    pub token: Option<String>,
    pub error: Option<String>,
    /// Permissions that were actually granted (may be less than requested for non-admins).
    pub granted_permissions: Option<GrantedPermissions>,
}

/// GET /ui/tokens - Show the token management page (role-adaptive).
///
/// Admin: skips permission check, shows all caches.
/// User: shows permissions table and accessible caches.
pub async fn tokens_page(
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

    // Get user's permissions (needed for non-admins)
    let permissions = if is_admin {
        Vec::new()
    } else {
        queries::get_user_permissions(db, user.id)
            .await
            .unwrap_or_default()
    };

    // Convert to display format (only for non-admins)
    let permissions_display: Vec<UserCachePermissionDisplay> = permissions
        .iter()
        .map(|p| UserCachePermissionDisplay {
            cache_name: p.cache_name.clone(),
            can_pull: p.can_pull,
            can_push: p.can_push,
            can_delete: p.can_delete,
            can_create_cache: p.can_create_cache,
            can_configure_cache: p.can_configure_cache,
            can_destroy_cache: p.can_destroy_cache,
        })
        .collect();

    // Get caches for reference
    let all_caches = queries::list_all_caches(db).await.unwrap_or_default();
    let accessible_caches: Vec<CacheModel> = if is_admin {
        all_caches
    } else {
        all_caches
            .into_iter()
            .filter(|cache| {
                cache.is_public || get_effective_permissions(&permissions, &cache.name).has_any()
            })
            .collect()
    };

    let template = TokensTemplate {
        user,
        caches: accessible_caches,
        permissions: permissions_display,
        is_admin,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|_| "Template error".to_string()),
    )
    .into_response()
}

/// POST /ui/tokens - Create a new JWT token (role-adaptive).
///
/// Admin: gets requested permissions directly, can specify any subject.
/// User: gets permission intersection, subject is forced to `user:{username}`.
pub async fn create_token(
    AxumState(web_ui): AxumState<WebUiState>,
    jar: PrivateCookieJar,
    Json(req): Json<CreateTokenRequest>,
) -> impl IntoResponse {
    // Get session user
    let user = match get_session_user(&web_ui, &jar).await {
        Some(user) => user,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(TokenApiResult {
                    success: false,
                    token: None,
                    error: Some("Not authenticated".to_string()),
                    granted_permissions: None,
                }),
            )
        }
    };

    let db = match web_ui.app_state.database().await {
        Ok(db) => db,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(TokenApiResult {
                    success: false,
                    token: None,
                    error: Some("Database error".to_string()),
                    granted_permissions: None,
                }),
            )
        }
    };

    let is_admin = user.is_admin;

    // Determine subject
    let subject = if is_admin {
        // Admin can specify subject or defaults to their username
        req.subject
            .as_ref()
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| user.username.clone())
    } else {
        // User's subject is forced to their username
        format!("user:{}", user.username)
    };

    // Validate cache pattern
    let cache_pattern = req.cache_pattern.trim();
    if cache_pattern.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(TokenApiResult {
                success: false,
                token: None,
                error: Some("Cache pattern is required".to_string()),
                granted_permissions: None,
            }),
        );
    }

    // Parse cache pattern
    let pattern: CacheNamePattern = match cache_pattern.parse() {
        Ok(p) => p,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(TokenApiResult {
                    success: false,
                    token: None,
                    error: Some("Invalid cache pattern".to_string()),
                    granted_permissions: None,
                }),
            )
        }
    };

    // Parse validity period
    let duration = match parse_validity(&req.validity) {
        Some(d) => d,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(TokenApiResult {
                    success: false,
                    token: None,
                    error: Some("Invalid validity period".to_string()),
                    granted_permissions: None,
                }),
            )
        }
    };

    // Calculate expiration
    let exp = match Utc::now().checked_add_signed(duration) {
        Some(e) => e,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(TokenApiResult {
                    success: false,
                    token: None,
                    error: Some("Expiry timestamp overflow".to_string()),
                    granted_permissions: None,
                }),
            )
        }
    };

    // Build requested permissions
    let requested = EffectivePermissions {
        can_pull: req.can_pull,
        can_push: req.can_push,
        can_delete: req.can_delete,
        can_create_cache: req.can_create_cache,
        can_configure_cache: req.can_configure_cache,
        can_destroy_cache: req.can_destroy_cache,
    };

    // Determine granted permissions based on role
    let granted = if is_admin {
        // Admin gets exactly what they requested
        requested
    } else {
        // Get user's actual permissions
        let user_permissions = queries::get_user_permissions(db, user.id)
            .await
            .unwrap_or_default();

        // Determine what permissions the user actually has for this pattern
        let user_effective = get_permissions_for_pattern(&user_permissions, cache_pattern);

        // Check if user has any permissions at all for this pattern
        if !user_effective.has_any() {
            return (
                StatusCode::FORBIDDEN,
                Json(TokenApiResult {
                    success: false,
                    token: None,
                    error: Some(format!(
                        "You don't have any permissions on '{}'",
                        cache_pattern
                    )),
                    granted_permissions: None,
                }),
            );
        }

        // Intersect requested with what user actually has
        intersect_permissions(&requested, &user_effective)
    };

    // Check if any permissions are being granted
    if !granted.has_any() {
        return (
            StatusCode::BAD_REQUEST,
            Json(TokenApiResult {
                success: false,
                token: None,
                error: Some("No valid permissions to grant".to_string()),
                granted_permissions: None,
            }),
        );
    }

    // Create token
    let mut token = Token::new(subject, &exp);

    // Set permissions
    let perm = token.get_or_insert_permission_mut(pattern);
    perm.pull = granted.can_pull;
    perm.push = granted.can_push;
    perm.delete = granted.can_delete;
    perm.create_cache = granted.can_create_cache;
    perm.configure_cache = granted.can_configure_cache;
    perm.destroy_cache = granted.can_destroy_cache;

    // Encode the token
    let signature_type: SignatureType = web_ui.app_state.config.jwt.signing_config.clone().into();

    let encoded = match token.encode(
        &signature_type,
        &web_ui.app_state.config.jwt.token_bound_issuer,
        &web_ui.app_state.config.jwt.token_bound_audiences,
    ) {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("Failed to encode token: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(TokenApiResult {
                    success: false,
                    token: None,
                    error: Some(
                        "Failed to encode token. Check server JWT configuration.".to_string(),
                    ),
                    granted_permissions: None,
                }),
            );
        }
    };

    (
        StatusCode::OK,
        Json(TokenApiResult {
            success: true,
            token: Some(encoded),
            error: None,
            granted_permissions: Some(GrantedPermissions {
                can_pull: granted.can_pull,
                can_push: granted.can_push,
                can_delete: granted.can_delete,
                can_create_cache: granted.can_create_cache,
                can_configure_cache: granted.can_configure_cache,
                can_destroy_cache: granted.can_destroy_cache,
            }),
        }),
    )
}

/// Gets the effective permissions a user has for a given cache pattern.
///
/// For exact cache names, this returns the permissions for that specific cache.
/// For wildcard patterns, this returns the permissions the user has that would
/// cover that pattern.
fn get_permissions_for_pattern(
    user_permissions: &[UserCachePermissionModel],
    pattern: &str,
) -> EffectivePermissions {
    let mut effective = EffectivePermissions::default();

    for perm in user_permissions {
        // Check if the user's permission pattern covers the requested pattern
        let covers = if perm.cache_name == "*" {
            // User has wildcard - covers everything
            true
        } else if perm.cache_name.ends_with('*') {
            let user_prefix = &perm.cache_name[..perm.cache_name.len() - 1];
            if pattern.ends_with('*') {
                // Both are wildcards - user's prefix must match or be prefix of requested
                let req_prefix = &pattern[..pattern.len() - 1];
                req_prefix.starts_with(user_prefix)
            } else {
                // User has wildcard, request is exact - must match prefix
                pattern.starts_with(user_prefix)
            }
        } else {
            // User has exact name
            if pattern.ends_with('*') {
                // Request is wildcard, user has exact - doesn't cover
                false
            } else {
                // Both exact - must match
                perm.cache_name == pattern
            }
        };

        if covers {
            effective.can_pull = effective.can_pull || perm.can_pull;
            effective.can_push = effective.can_push || perm.can_push;
            effective.can_delete = effective.can_delete || perm.can_delete;
            effective.can_create_cache = effective.can_create_cache || perm.can_create_cache;
            effective.can_configure_cache =
                effective.can_configure_cache || perm.can_configure_cache;
            effective.can_destroy_cache = effective.can_destroy_cache || perm.can_destroy_cache;
        }
    }

    effective
}

/// Parse validity string like "1d", "7d", "30d", "365d"
fn parse_validity(validity: &str) -> Option<ChronoDuration> {
    let validity = validity.trim();
    if validity.ends_with('d') {
        let days: i64 = validity[..validity.len() - 1].parse().ok()?;
        ChronoDuration::try_days(days)
    } else if validity.ends_with('h') {
        let hours: i64 = validity[..validity.len() - 1].parse().ok()?;
        ChronoDuration::try_hours(hours)
    } else if validity.ends_with('y') {
        let years: i64 = validity[..validity.len() - 1].parse().ok()?;
        ChronoDuration::try_days(years * 365)
    } else {
        // Try parsing as days
        let days: i64 = validity.parse().ok()?;
        ChronoDuration::try_days(days)
    }
}
