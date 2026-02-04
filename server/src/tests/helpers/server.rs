//! Test server infrastructure.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::Extension;
use axum::http::{Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use tempfile::TempDir;
use tower::ServiceExt;

use attic_token::HS256Key;

use crate::access::http::apply_auth;
use crate::config::Config;
use crate::database::connection::{TursoConfig, TursoConnection};
use crate::database::migrations::run_migrations;
use crate::database::models::CacheModel;
use crate::database::queries;
use crate::{State, StateInner};

use super::config::TestConfigBuilder;
use super::jwt::TestTokenBuilder;

/// A test server with all necessary infrastructure for integration testing.
pub struct TestServer {
    /// The server configuration.
    pub config: Config,
    /// The global server state.
    pub state: State,
    /// The Axum router.
    pub router: Router,
    /// The JWT signing secret (for creating test tokens).
    pub jwt_secret: HS256Key,
    /// Temporary directory for storage (kept alive for test duration).
    _temp_dir: TempDir,
}

impl TestServer {
    /// Creates a new test server with a fresh database and storage.
    pub async fn new() -> Self {
        Self::with_config_builder(|builder| builder).await
    }

    /// Creates a new test server with soft delete enabled.
    pub async fn with_soft_delete() -> Self {
        Self::with_config_builder(|builder| builder.with_soft_delete()).await
    }

    /// Creates a new test server with proof of possession enabled.
    pub async fn with_proof_of_possession() -> Self {
        Self::with_config_builder(|builder| builder.with_proof_of_possession()).await
    }

    /// Creates a new test server with chunking enabled.
    pub async fn with_chunking(threshold: usize) -> Self {
        Self::with_config_builder(|builder| builder.with_chunking_threshold(threshold)).await
    }

    /// Creates a new test server with a custom config builder function.
    pub async fn with_config_builder<F>(config_fn: F) -> Self
    where
        F: FnOnce(TestConfigBuilder) -> TestConfigBuilder,
    {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let storage_path = temp_dir.path().join("storage");
        let db_path = temp_dir.path().join("test.db");
        let database_url = format!("sqlite://{}", db_path.display());

        // Generate a random JWT secret for this test
        let jwt_secret = HS256Key::generate();

        let builder =
            TestConfigBuilder::new(storage_path, database_url.clone(), jwt_secret.clone());
        let config = config_fn(builder).build();

        // Create state
        let state = StateInner::new(config.clone()).await;

        // Run migrations
        let turso_config = TursoConfig::from_database_config(&config.database);
        let conn = TursoConnection::connect(turso_config)
            .await
            .expect("Failed to connect to test database");
        run_migrations(&conn)
            .await
            .expect("Failed to run migrations");

        // Create router with all middleware
        // We import the api module through the crate root
        let router = create_test_router(state.clone());

        Self {
            config,
            state,
            router,
            jwt_secret,
            _temp_dir: temp_dir,
        }
    }

    /// Returns a clone of the router for making requests.
    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Creates a new test token builder.
    pub fn token(&self, subject: &str) -> TestTokenBuilder {
        TestTokenBuilder::new(subject)
    }

    /// Builds a token string using this server's JWT secret.
    pub fn build_token(&self, builder: TestTokenBuilder) -> String {
        builder.build(&self.jwt_secret)
    }

    /// Creates a cache directly in the database (bypassing API).
    pub async fn create_cache(&self, name: &str, is_public: bool) -> CacheModel {
        let database = self.state.database().await.expect("Failed to get database");

        // Generate a test keypair
        let keypair =
            attic::signing::NixKeypair::generate(name).expect("Failed to generate keypair");

        queries::create_cache(
            database,
            name,
            &keypair.export_keypair(),
            is_public,
            "/nix/store",
            40,
            &[],
        )
        .await
        .expect("Failed to create cache")
    }

    /// Gets the database connection.
    pub async fn database(&self) -> &Arc<TursoConnection> {
        self.state.database().await.expect("Failed to get database")
    }

    /// Makes a request to the test server.
    pub async fn request(&self, request: Request<Body>) -> TestResponse {
        let response = self
            .router()
            .oneshot(request)
            .await
            .expect("Request failed");

        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("Failed to read body")
            .to_bytes()
            .to_vec();

        TestResponse {
            status,
            headers,
            body,
        }
    }

    /// Makes a GET request.
    pub async fn get(&self, uri: &str) -> TestResponse {
        let request = Request::builder()
            .method("GET")
            .uri(uri)
            .header("Host", "localhost")
            .body(Body::empty())
            .unwrap();
        self.request(request).await
    }

    /// Makes a GET request with authorization.
    pub async fn get_with_token(&self, uri: &str, token: &str) -> TestResponse {
        let request = Request::builder()
            .method("GET")
            .uri(uri)
            .header("Host", "localhost")
            .header("Authorization", format!("Bearer {}", token))
            .body(Body::empty())
            .unwrap();
        self.request(request).await
    }

    /// Makes a POST request with JSON body.
    pub async fn post_json(&self, uri: &str, body: &impl serde::Serialize) -> TestResponse {
        let body_bytes = serde_json::to_vec(body).unwrap();
        let request = Request::builder()
            .method("POST")
            .uri(uri)
            .header("Host", "localhost")
            .header("Content-Type", "application/json")
            .body(Body::from(body_bytes))
            .unwrap();
        self.request(request).await
    }

    /// Makes a POST request with JSON body and authorization.
    pub async fn post_json_with_token(
        &self,
        uri: &str,
        body: &impl serde::Serialize,
        token: &str,
    ) -> TestResponse {
        let body_bytes = serde_json::to_vec(body).unwrap();
        let request = Request::builder()
            .method("POST")
            .uri(uri)
            .header("Host", "localhost")
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", token))
            .body(Body::from(body_bytes))
            .unwrap();
        self.request(request).await
    }

    /// Makes a PATCH request with JSON body and authorization.
    pub async fn patch_json_with_token(
        &self,
        uri: &str,
        body: &impl serde::Serialize,
        token: &str,
    ) -> TestResponse {
        let body_bytes = serde_json::to_vec(body).unwrap();
        let request = Request::builder()
            .method("PATCH")
            .uri(uri)
            .header("Host", "localhost")
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", token))
            .body(Body::from(body_bytes))
            .unwrap();
        self.request(request).await
    }

    /// Makes a DELETE request with authorization.
    pub async fn delete_with_token(&self, uri: &str, token: &str) -> TestResponse {
        let request = Request::builder()
            .method("DELETE")
            .uri(uri)
            .header("Host", "localhost")
            .header("Authorization", format!("Bearer {}", token))
            .body(Body::empty())
            .unwrap();
        self.request(request).await
    }
}

/// Creates a test router with all middleware configured.
fn create_test_router(state: State) -> Router {
    use crate::api;
    use crate::middleware::{init_request_state, restrict_host, set_visibility_header};

    Router::new()
        .merge(api::get_router())
        .layer(axum::middleware::from_fn(apply_auth))
        .layer(axum::middleware::from_fn(set_visibility_header))
        .layer(axum::middleware::from_fn(init_request_state))
        .layer(axum::middleware::from_fn(restrict_host))
        .layer(Extension(state))
}

/// A test response with helper methods.
pub struct TestResponse {
    pub status: StatusCode,
    pub headers: axum::http::HeaderMap,
    pub body: Vec<u8>,
}

impl TestResponse {
    /// Returns the body as a string.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }

    /// Parses the body as JSON.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> T {
        serde_json::from_slice(&self.body).expect("Failed to parse JSON response")
    }

    /// Asserts the status code is OK (200).
    pub fn assert_ok(&self) {
        assert_eq!(
            self.status,
            StatusCode::OK,
            "Expected 200 OK, got {} with body: {}",
            self.status,
            self.text()
        );
    }

    /// Asserts the status code is the expected value.
    pub fn assert_status(&self, expected: StatusCode) {
        assert_eq!(
            self.status,
            expected,
            "Expected {}, got {} with body: {}",
            expected,
            self.status,
            self.text()
        );
    }

    /// Asserts the status code is 401 Unauthorized.
    pub fn assert_unauthorized(&self) {
        assert_eq!(
            self.status,
            StatusCode::UNAUTHORIZED,
            "Expected 401 Unauthorized, got {} with body: {}",
            self.status,
            self.text()
        );
    }

    /// Asserts the status code is 403 Forbidden.
    pub fn assert_forbidden(&self) {
        assert_eq!(
            self.status,
            StatusCode::FORBIDDEN,
            "Expected 403 Forbidden, got {} with body: {}",
            self.status,
            self.text()
        );
    }

    /// Asserts the status code is 404 Not Found.
    pub fn assert_not_found(&self) {
        assert_eq!(
            self.status,
            StatusCode::NOT_FOUND,
            "Expected 404 Not Found, got {} with body: {}",
            self.status,
            self.text()
        );
    }

    /// Asserts the status code is 409 Conflict.
    pub fn assert_conflict(&self) {
        assert_eq!(
            self.status,
            StatusCode::CONFLICT,
            "Expected 409 Conflict, got {} with body: {}",
            self.status,
            self.text()
        );
    }
}
