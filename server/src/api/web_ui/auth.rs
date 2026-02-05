//! Authentication handlers for the web UI.
//!
//! Handles passkey registration, login, and logout.

use askama::Template;
use axum::{
    extract::{Query, State as AxumState},
    http::StatusCode,
    response::{Html, IntoResponse, Redirect},
    Json,
};
use axum_extra::extract::cookie::{Cookie, PrivateCookieJar, SameSite};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use webauthn_rs::prelude::*;

use super::webauthn::{credential_to_passkey, passkey_to_stored_key};
use super::WebUiState;
use crate::config::RegistrationPolicy;
use crate::database::queries;

/// Session cookie name.
pub const SESSION_COOKIE: &str = "attic_session";

// ============================================================================
// Templates
// ============================================================================

#[derive(Template)]
#[template(path = "login.html")]
struct LoginTemplate {}

#[derive(Template)]
#[template(path = "register.html")]
struct RegisterTemplate {
    can_register: bool,
    is_first_user: bool,
}

// ============================================================================
// Request/Response types
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct AuthStartRequest {
    pub username: String,
}

#[derive(Debug, Serialize)]
pub struct AuthStartResponse {
    pub challenge: RequestChallengeResponse,
}

#[derive(Debug, Deserialize)]
pub struct AuthFinishRequest {
    pub username: String,
    pub credential: PublicKeyCredential,
}

#[derive(Debug, Serialize)]
pub struct AuthResult {
    pub success: bool,
    pub redirect: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RegisterStartRequest {
    pub username: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RegisterStartResponse {
    pub challenge: CreationChallengeResponse,
}

#[derive(Debug, Deserialize)]
pub struct RegisterFinishRequest {
    pub username: String,
    pub display_name: Option<String>,
    pub credential: RegisterPublicKeyCredential,
    pub credential_name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RegisterQuery {
    pub invite: Option<String>,
}

// ============================================================================
// Handlers
// ============================================================================

/// GET /ui/login - Show the login page.
pub async fn login_page() -> impl IntoResponse {
    let template = LoginTemplate {};
    Html(
        template
            .render()
            .unwrap_or_else(|_| "Template error".to_string()),
    )
}

/// POST /ui/auth/start - Start passkey authentication.
pub async fn auth_start(
    AxumState(web_ui): AxumState<WebUiState>,
    Json(req): Json<AuthStartRequest>,
) -> Result<Json<AuthStartResponse>, (StatusCode, Json<AuthResult>)> {
    let db = web_ui.app_state.database().await.map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(AuthResult {
                success: false,
                redirect: None,
                error: Some("Database error".to_string()),
            }),
        )
    })?;

    // Find user
    let user = queries::find_user_by_username(db, &req.username)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AuthResult {
                    success: false,
                    redirect: None,
                    error: Some("Database error".to_string()),
                }),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                Json(AuthResult {
                    success: false,
                    redirect: None,
                    error: Some("User not found".to_string()),
                }),
            )
        })?;

    // Get user's credentials
    let credentials = queries::find_credentials_by_user(db, user.id)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AuthResult {
                    success: false,
                    redirect: None,
                    error: Some("Database error".to_string()),
                }),
            )
        })?;

    if credentials.is_empty() {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(AuthResult {
                success: false,
                redirect: None,
                error: Some("No passkeys registered".to_string()),
            }),
        ));
    }

    // Convert stored credentials to Passkeys
    let passkeys: Vec<Passkey> = credentials
        .iter()
        .filter_map(|c| credential_to_passkey(&c.credential_id, &c.public_key, c.counter).ok())
        .collect();

    if passkeys.is_empty() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(AuthResult {
                success: false,
                redirect: None,
                error: Some("Invalid stored credentials".to_string()),
            }),
        ));
    }

    // Start authentication
    let challenge = web_ui
        .webauthn
        .start_authentication(&req.username, passkeys)
        .map_err(|e| {
            tracing::error!("WebAuthn error: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AuthResult {
                    success: false,
                    redirect: None,
                    error: Some("Authentication error".to_string()),
                }),
            )
        })?;

    Ok(Json(AuthStartResponse { challenge }))
}

/// POST /ui/auth/finish - Complete passkey authentication.
pub async fn auth_finish(
    AxumState(web_ui): AxumState<WebUiState>,
    jar: PrivateCookieJar,
    Json(req): Json<AuthFinishRequest>,
) -> Result<(PrivateCookieJar, Json<AuthResult>), (StatusCode, Json<AuthResult>)> {
    let db = web_ui.app_state.database().await.map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(AuthResult {
                success: false,
                redirect: None,
                error: Some("Database error".to_string()),
            }),
        )
    })?;

    // Find user
    let user = queries::find_user_by_username(db, &req.username)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AuthResult {
                    success: false,
                    redirect: None,
                    error: Some("Database error".to_string()),
                }),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                Json(AuthResult {
                    success: false,
                    redirect: None,
                    error: Some("User not found".to_string()),
                }),
            )
        })?;

    // Finish authentication
    let auth_result = web_ui
        .webauthn
        .finish_authentication(&req.username, &req.credential)
        .map_err(|e| {
            tracing::error!("WebAuthn verification failed: {:?}", e);
            (
                StatusCode::UNAUTHORIZED,
                Json(AuthResult {
                    success: false,
                    redirect: None,
                    error: Some("Authentication failed".to_string()),
                }),
            )
        })?;

    // Update credential counter
    let credential_id_bytes = req.credential.id.as_ref();
    if let Ok(Some(cred)) = queries::find_credential_by_credential_id(db, credential_id_bytes).await
    {
        let _ = queries::update_credential_counter(db, cred.id, auth_result.counter()).await;
    }

    // Update last login time
    let _ = queries::update_user_last_login(db, user.id).await;

    // Create session
    let session_id = Uuid::new_v4().to_string();
    let expires_at = Utc::now() + chrono::Duration::seconds(web_ui.session_duration_secs as i64);
    let expires_at_str = expires_at.to_rfc3339();

    queries::create_session(db, user.id, &session_id, &expires_at_str)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AuthResult {
                    success: false,
                    redirect: None,
                    error: Some("Failed to create session".to_string()),
                }),
            )
        })?;

    // Set session cookie
    let cookie = Cookie::build((SESSION_COOKIE, session_id))
        .path("/")
        .http_only(true)
        .same_site(SameSite::Strict)
        .secure(true)
        .max_age(time::Duration::seconds(web_ui.session_duration_secs as i64))
        .build();

    let jar = jar.add(cookie);

    Ok((
        jar,
        Json(AuthResult {
            success: true,
            redirect: Some("/ui".to_string()),
            error: None,
        }),
    ))
}

/// POST /ui/logout - Log out and clear session.
pub async fn logout(
    AxumState(web_ui): AxumState<WebUiState>,
    jar: PrivateCookieJar,
) -> Result<(PrivateCookieJar, Redirect), (StatusCode, &'static str)> {
    // Get session ID from cookie
    if let Some(cookie) = jar.get(SESSION_COOKIE) {
        let session_id = cookie.value();

        // Delete session from database
        if let Ok(db) = web_ui.app_state.database().await {
            let _ = queries::delete_session(db, session_id).await;
        }
    }

    // Remove cookie
    let jar = jar.remove(Cookie::from(SESSION_COOKIE));

    Ok((jar, Redirect::to("/ui/login")))
}

/// GET /ui/register - Show the registration page.
pub async fn register_page(
    AxumState(web_ui): AxumState<WebUiState>,
    Query(query): Query<RegisterQuery>,
) -> impl IntoResponse {
    let (can_register, is_first_user) = match web_ui.app_state.database().await {
        Ok(db) => {
            let user_count = queries::count_users(db).await.unwrap_or(1);
            let is_first = user_count == 0;
            let policy = &web_ui.app_state.config.web_ui.registration;

            let can_register = match policy {
                RegistrationPolicy::FirstUser => is_first,
                RegistrationPolicy::InviteOnly => query.invite.is_some(), // TODO: validate invite
                RegistrationPolicy::Disabled => false,
            };

            (can_register, is_first)
        }
        Err(_) => (false, false),
    };

    let template = RegisterTemplate {
        can_register,
        is_first_user,
    };
    Html(
        template
            .render()
            .unwrap_or_else(|_| "Template error".to_string()),
    )
}

/// POST /ui/register/start - Start passkey registration.
pub async fn register_start(
    AxumState(web_ui): AxumState<WebUiState>,
    Json(req): Json<RegisterStartRequest>,
) -> Result<Json<RegisterStartResponse>, (StatusCode, Json<AuthResult>)> {
    let db = web_ui.app_state.database().await.map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(AuthResult {
                success: false,
                redirect: None,
                error: Some("Database error".to_string()),
            }),
        )
    })?;

    // Check registration policy
    let user_count = queries::count_users(db).await.unwrap_or(1);
    let is_first_user = user_count == 0;
    let policy = &web_ui.app_state.config.web_ui.registration;

    let can_register = match policy {
        RegistrationPolicy::FirstUser => is_first_user,
        RegistrationPolicy::InviteOnly => false, // TODO: validate invite token
        RegistrationPolicy::Disabled => false,
    };

    if !can_register {
        return Err((
            StatusCode::FORBIDDEN,
            Json(AuthResult {
                success: false,
                redirect: None,
                error: Some("Registration is not allowed".to_string()),
            }),
        ));
    }

    // Check if username is already taken
    if queries::find_user_by_username(db, &req.username)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AuthResult {
                    success: false,
                    redirect: None,
                    error: Some("Database error".to_string()),
                }),
            )
        })?
        .is_some()
    {
        return Err((
            StatusCode::CONFLICT,
            Json(AuthResult {
                success: false,
                redirect: None,
                error: Some("Username already taken".to_string()),
            }),
        ));
    }

    // Generate user ID for WebAuthn (will be stored when registration completes)
    let user_uuid = Uuid::new_v4();
    let display_name = req.display_name.as_deref().unwrap_or(&req.username);

    // Start registration
    let challenge = web_ui
        .webauthn
        .start_registration(user_uuid, &req.username, display_name, vec![])
        .map_err(|e| {
            tracing::error!("WebAuthn registration error: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AuthResult {
                    success: false,
                    redirect: None,
                    error: Some("Registration error".to_string()),
                }),
            )
        })?;

    Ok(Json(RegisterStartResponse { challenge }))
}

/// POST /ui/register/finish - Complete passkey registration.
pub async fn register_finish(
    AxumState(web_ui): AxumState<WebUiState>,
    jar: PrivateCookieJar,
    Json(req): Json<RegisterFinishRequest>,
) -> Result<(PrivateCookieJar, Json<AuthResult>), (StatusCode, Json<AuthResult>)> {
    let db = web_ui.app_state.database().await.map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(AuthResult {
                success: false,
                redirect: None,
                error: Some("Database error".to_string()),
            }),
        )
    })?;

    // Check registration policy again
    let user_count = queries::count_users(db).await.unwrap_or(1);
    let is_first_user = user_count == 0;
    let policy = &web_ui.app_state.config.web_ui.registration;

    let can_register = match policy {
        RegistrationPolicy::FirstUser => is_first_user,
        RegistrationPolicy::InviteOnly => false, // TODO: validate invite
        RegistrationPolicy::Disabled => false,
    };

    if !can_register {
        return Err((
            StatusCode::FORBIDDEN,
            Json(AuthResult {
                success: false,
                redirect: None,
                error: Some("Registration is not allowed".to_string()),
            }),
        ));
    }

    // Finish registration
    let passkey = web_ui
        .webauthn
        .finish_registration(&req.username, &req.credential)
        .map_err(|e| {
            tracing::error!("WebAuthn verification failed: {:?}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(AuthResult {
                    success: false,
                    redirect: None,
                    error: Some("Registration failed".to_string()),
                }),
            )
        })?;

    // Create user (first user is admin)
    let display_name = req.display_name.as_deref();
    let user = queries::create_user(db, &req.username, display_name, is_first_user)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AuthResult {
                    success: false,
                    redirect: None,
                    error: Some("Failed to create user".to_string()),
                }),
            )
        })?;

    // Store credential
    let credential_id = passkey.cred_id().as_ref();
    let public_key = passkey_to_stored_key(&passkey).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(AuthResult {
                success: false,
                redirect: None,
                error: Some("Failed to store credential".to_string()),
            }),
        )
    })?;

    queries::create_credential(
        db,
        user.id,
        credential_id,
        &public_key,
        req.credential_name.as_deref(),
    )
    .await
    .map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(AuthResult {
                success: false,
                redirect: None,
                error: Some("Failed to store credential".to_string()),
            }),
        )
    })?;

    // Create session
    let session_id = Uuid::new_v4().to_string();
    let expires_at = Utc::now() + chrono::Duration::seconds(web_ui.session_duration_secs as i64);
    let expires_at_str = expires_at.to_rfc3339();

    queries::create_session(db, user.id, &session_id, &expires_at_str)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AuthResult {
                    success: false,
                    redirect: None,
                    error: Some("Failed to create session".to_string()),
                }),
            )
        })?;

    // Set session cookie
    let cookie = Cookie::build((SESSION_COOKIE, session_id))
        .path("/")
        .http_only(true)
        .same_site(SameSite::Strict)
        .secure(true)
        .max_age(time::Duration::seconds(web_ui.session_duration_secs as i64))
        .build();

    let jar = jar.add(cookie);

    Ok((
        jar,
        Json(AuthResult {
            success: true,
            redirect: Some("/ui".to_string()),
            error: None,
        }),
    ))
}

/// Extracts session user from cookie.
pub async fn get_session_user(
    web_ui: &WebUiState,
    jar: &PrivateCookieJar,
) -> Option<crate::database::models::UserModel> {
    let cookie = jar.get(SESSION_COOKIE)?;
    let session_id = cookie.value();

    let db = web_ui.app_state.database().await.ok()?;
    let (session, user) = queries::find_session(db, session_id).await.ok()??;

    // Check if session is expired
    if session.is_expired() {
        let _ = queries::delete_session(db, session_id).await;
        return None;
    }

    Some(user)
}
