//! Admin user management handlers for the web UI.

use askama::Template;
use axum::{
    extract::{Path, State as AxumState},
    http::StatusCode,
    response::{Html, IntoResponse, Redirect},
    Form, Json,
};
use axum_extra::extract::cookie::PrivateCookieJar;
use serde::{Deserialize, Serialize};

use super::auth::get_session_user;
use super::WebUiState;
use crate::database::models::{CredentialModel, UserCachePermissionModel, UserModel};
use crate::database::queries;

// ============================================================================
// Templates
// ============================================================================

#[derive(Template)]
#[template(path = "admin/users.html")]
struct UsersTemplate {
    user: UserModel,
    users: Vec<UserModel>,
}

#[derive(Template)]
#[template(path = "admin/user_detail.html")]
struct UserDetailTemplate {
    user: UserModel,
    target_user: UserModel,
    permissions: Vec<UserCachePermissionModel>,
    credentials: Vec<CredentialModel>,
}

// ============================================================================
// Request/Response types
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub display_name: Option<String>,
    pub is_admin: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdatePermissionRequest {
    pub cache_name: String,
    pub can_pull: Option<bool>,
    pub can_push: Option<bool>,
    pub can_delete: Option<bool>,
    pub can_create_cache: Option<bool>,
    pub can_configure_cache: Option<bool>,
    pub can_destroy_cache: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct ApiResult {
    pub success: bool,
    pub error: Option<String>,
}

// ============================================================================
// Handlers
// ============================================================================

/// GET /ui/admin/users - List all users.
pub async fn list_users(
    AxumState(web_ui): AxumState<WebUiState>,
    jar: PrivateCookieJar,
) -> impl IntoResponse {
    // Get session user
    let user = match get_session_user(&web_ui, &jar).await {
        Some(user) => user,
        None => return Redirect::to("/ui/login").into_response(),
    };

    // Check if admin
    if !user.is_admin {
        return Redirect::to("/ui").into_response();
    }

    let db = match web_ui.app_state.database().await {
        Ok(db) => db,
        Err(_) => {
            return Html("Database error".to_string()).into_response();
        }
    };

    let users = queries::list_users(db).await.unwrap_or_default();

    let template = UsersTemplate { user, users };

    Html(
        template
            .render()
            .unwrap_or_else(|_| "Template error".to_string()),
    )
    .into_response()
}

/// POST /ui/admin/users - Create a new user.
pub async fn create_user(
    AxumState(web_ui): AxumState<WebUiState>,
    jar: PrivateCookieJar,
    Form(req): Form<CreateUserRequest>,
) -> impl IntoResponse {
    // Get session user
    let user = match get_session_user(&web_ui, &jar).await {
        Some(user) => user,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ApiResult {
                    success: false,
                    error: Some("Not authenticated".to_string()),
                }),
            )
                .into_response()
        }
    };

    // Check if admin
    if !user.is_admin {
        return (
            StatusCode::FORBIDDEN,
            Json(ApiResult {
                success: false,
                error: Some("Admin access required".to_string()),
            }),
        )
            .into_response();
    }

    let db = match web_ui.app_state.database().await {
        Ok(db) => db,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResult {
                    success: false,
                    error: Some("Database error".to_string()),
                }),
            )
                .into_response()
        }
    };

    // Check if username already exists
    if let Ok(Some(_)) = queries::find_user_by_username(db, &req.username).await {
        return (
            StatusCode::CONFLICT,
            Json(ApiResult {
                success: false,
                error: Some("Username already exists".to_string()),
            }),
        )
            .into_response();
    }

    // Create user
    match queries::create_user(
        db,
        &req.username,
        req.display_name.as_deref(),
        req.is_admin.unwrap_or(false),
    )
    .await
    {
        Ok(_) => Redirect::to("/ui/admin/users").into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResult {
                success: false,
                error: Some("Failed to create user".to_string()),
            }),
        )
            .into_response(),
    }
}

/// GET /ui/admin/users/:id - Show user details with permissions.
pub async fn user_detail(
    AxumState(web_ui): AxumState<WebUiState>,
    jar: PrivateCookieJar,
    Path(user_id): Path<i64>,
) -> impl IntoResponse {
    // Get session user
    let user = match get_session_user(&web_ui, &jar).await {
        Some(user) => user,
        None => return Redirect::to("/ui/login").into_response(),
    };

    // Check if admin
    if !user.is_admin {
        return Redirect::to("/ui").into_response();
    }

    let db = match web_ui.app_state.database().await {
        Ok(db) => db,
        Err(_) => {
            return Html("Database error".to_string()).into_response();
        }
    };

    // Get target user
    let target_user = match queries::find_user_by_id(db, user_id).await {
        Ok(Some(u)) => u,
        _ => return Redirect::to("/ui/admin/users").into_response(),
    };

    // Get permissions
    let permissions = queries::get_user_permissions(db, user_id)
        .await
        .unwrap_or_default();

    // Get credentials
    let credentials = queries::find_credentials_by_user(db, user_id)
        .await
        .unwrap_or_default();

    let template = UserDetailTemplate {
        user,
        target_user,
        permissions,
        credentials,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|_| "Template error".to_string()),
    )
    .into_response()
}

/// POST /ui/admin/users/:id/permissions - Update user permissions.
pub async fn update_permissions(
    AxumState(web_ui): AxumState<WebUiState>,
    jar: PrivateCookieJar,
    Path(user_id): Path<i64>,
    Form(req): Form<UpdatePermissionRequest>,
) -> impl IntoResponse {
    // Get session user
    let user = match get_session_user(&web_ui, &jar).await {
        Some(user) => user,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ApiResult {
                    success: false,
                    error: Some("Not authenticated".to_string()),
                }),
            )
                .into_response()
        }
    };

    // Check if admin
    if !user.is_admin {
        return (
            StatusCode::FORBIDDEN,
            Json(ApiResult {
                success: false,
                error: Some("Admin access required".to_string()),
            }),
        )
            .into_response();
    }

    let db = match web_ui.app_state.database().await {
        Ok(db) => db,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResult {
                    success: false,
                    error: Some("Database error".to_string()),
                }),
            )
                .into_response()
        }
    };

    // Set permission
    match queries::set_user_permission(
        db,
        user_id,
        &req.cache_name,
        req.can_pull.unwrap_or(false),
        req.can_push.unwrap_or(false),
        req.can_delete.unwrap_or(false),
        req.can_create_cache.unwrap_or(false),
        req.can_configure_cache.unwrap_or(false),
        req.can_destroy_cache.unwrap_or(false),
    )
    .await
    {
        Ok(_) => Redirect::to(&format!("/ui/admin/users/{}", user_id)).into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResult {
                success: false,
                error: Some("Failed to update permission".to_string()),
            }),
        )
            .into_response(),
    }
}

/// DELETE /ui/admin/users/:id/permissions/:cache_name - Delete a permission.
pub async fn delete_permission(
    AxumState(web_ui): AxumState<WebUiState>,
    jar: PrivateCookieJar,
    Path((user_id, cache_name)): Path<(i64, String)>,
) -> impl IntoResponse {
    // Get session user
    let user = match get_session_user(&web_ui, &jar).await {
        Some(user) => user,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ApiResult {
                    success: false,
                    error: Some("Not authenticated".to_string()),
                }),
            )
        }
    };

    // Check if admin
    if !user.is_admin {
        return (
            StatusCode::FORBIDDEN,
            Json(ApiResult {
                success: false,
                error: Some("Admin access required".to_string()),
            }),
        );
    }

    let db = match web_ui.app_state.database().await {
        Ok(db) => db,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResult {
                    success: false,
                    error: Some("Database error".to_string()),
                }),
            )
        }
    };

    match queries::delete_user_permission(db, user_id, &cache_name).await {
        Ok(_) => (
            StatusCode::OK,
            Json(ApiResult {
                success: true,
                error: None,
            }),
        ),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResult {
                success: false,
                error: Some("Failed to delete permission".to_string()),
            }),
        ),
    }
}

/// DELETE /ui/admin/users/:id - Delete a user.
pub async fn delete_user(
    AxumState(web_ui): AxumState<WebUiState>,
    jar: PrivateCookieJar,
    Path(user_id): Path<i64>,
) -> impl IntoResponse {
    // Get session user
    let user = match get_session_user(&web_ui, &jar).await {
        Some(user) => user,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ApiResult {
                    success: false,
                    error: Some("Not authenticated".to_string()),
                }),
            )
        }
    };

    // Check if admin
    if !user.is_admin {
        return (
            StatusCode::FORBIDDEN,
            Json(ApiResult {
                success: false,
                error: Some("Admin access required".to_string()),
            }),
        );
    }

    // Can't delete yourself
    if user.id == user_id {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResult {
                success: false,
                error: Some("Cannot delete yourself".to_string()),
            }),
        );
    }

    let db = match web_ui.app_state.database().await {
        Ok(db) => db,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResult {
                    success: false,
                    error: Some("Database error".to_string()),
                }),
            )
        }
    };

    // Delete user sessions first
    let _ = queries::delete_user_sessions(db, user_id).await;

    // Delete user (credentials and permissions cascade)
    match queries::delete_user(db, user_id).await {
        Ok(_) => (
            StatusCode::OK,
            Json(ApiResult {
                success: true,
                error: None,
            }),
        ),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResult {
                success: false,
                error: Some("Failed to delete user".to_string()),
            }),
        ),
    }
}
