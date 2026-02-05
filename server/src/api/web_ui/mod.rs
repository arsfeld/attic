//! Web UI for the Attic binary cache server.
//!
//! This module provides a web-based user interface with passkey (WebAuthn) authentication.
//! The web UI is separate from the API authentication (JWT) - web UI uses session cookies
//! while the API continues to use JWT for CLI tools and CI.

pub mod auth;
pub mod caches;
pub mod dashboard;
pub mod permissions;
pub mod tokens;
pub mod users;
pub mod webauthn;

use std::sync::Arc;

use axum::{
    extract::FromRef,
    routing::{delete, get, post},
    Router,
};
use axum_extra::extract::cookie::Key;

use crate::config::WebUiConfig;
use crate::State;
use webauthn::WebAuthnState;

/// Web UI state shared across all handlers.
#[derive(Clone)]
pub struct WebUiState {
    /// WebAuthn state for passkey authentication.
    pub webauthn: Arc<WebAuthnState>,

    /// Cookie signing key.
    pub cookie_key: Key,

    /// Session duration in seconds.
    pub session_duration_secs: u64,

    /// Reference to the main application state.
    pub app_state: State,
}

// Allow extracting Key from WebUiState for PrivateCookieJar
impl FromRef<WebUiState> for Key {
    fn from_ref(state: &WebUiState) -> Self {
        state.cookie_key.clone()
    }
}

// Allow extracting main State from WebUiState
impl FromRef<WebUiState> for State {
    fn from_ref(state: &WebUiState) -> Self {
        state.app_state.clone()
    }
}

impl WebUiState {
    /// Creates a new WebUI state from configuration.
    pub fn new(config: &WebUiConfig, app_state: State) -> Option<Self> {
        if !config.enabled {
            return None;
        }

        let rp_id = config.rp_id.as_ref()?;
        let rp_origin = config
            .rp_origin
            .clone()
            .unwrap_or_else(|| format!("https://{}", rp_id));

        let webauthn = WebAuthnState::new(rp_id, &rp_origin).ok()?;

        // Use provided key or generate a random one
        let cookie_key = if let Some(key_b64) = &config.cookie_key_base64 {
            use base64::{engine::general_purpose::STANDARD, Engine};
            let key_bytes = STANDARD.decode(key_b64).ok()?;
            Key::try_from(&key_bytes[..]).ok()?
        } else {
            tracing::warn!("No cookie-key-base64 configured, generating random key. Sessions won't persist across restarts.");
            Key::generate()
        };

        Some(Self {
            webauthn: Arc::new(webauthn),
            cookie_key,
            session_duration_secs: config.session_duration.as_secs(),
            app_state,
        })
    }
}

/// Returns the web UI router.
///
/// Returns None if the web UI is not configured.
pub fn get_router(config: &WebUiConfig, app_state: State) -> Option<Router<()>> {
    let state = WebUiState::new(config, app_state)?;

    let router = Router::new()
        // Public routes (no auth required)
        .route("/ui/login", get(auth::login_page))
        .route("/ui/auth/start", post(auth::auth_start))
        .route("/ui/auth/finish", post(auth::auth_finish))
        .route("/ui/logout", post(auth::logout))
        // Registration routes (conditional based on policy)
        .route("/ui/register", get(auth::register_page))
        .route("/ui/register/start", post(auth::register_start))
        .route("/ui/register/finish", post(auth::register_finish))
        // Authenticated routes (role-adaptive)
        .route("/ui", get(dashboard::dashboard))
        .route("/ui/dashboard", get(dashboard::dashboard))
        .route(
            "/ui/caches",
            get(caches::list_caches).post(caches::create_cache),
        )
        .route("/ui/caches/:name", delete(caches::delete_cache))
        .route(
            "/ui/tokens",
            get(tokens::tokens_page).post(tokens::create_token),
        )
        // Admin-only routes (user management remains admin-only)
        .route(
            "/ui/admin/users",
            get(users::list_users).post(users::create_user),
        )
        .route(
            "/ui/admin/users/:id",
            get(users::user_detail).delete(users::delete_user),
        )
        .route(
            "/ui/admin/users/:id/permissions",
            post(users::update_permissions),
        )
        .route(
            "/ui/admin/users/:id/permissions/:cache_name",
            delete(users::delete_permission),
        )
        // Set the state - this makes Key extractable via FromRef
        .with_state(state);

    Some(router)
}
