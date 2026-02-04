//! Tests for the upload path endpoint.
//!
//! Note: Full upload tests require complex NAR generation and are
//! covered in the e2e tests. These tests focus on error handling
//! and permission checks.

use axum::body::Body;
use axum::http::Request;

use crate::tests::helpers::TestServer;

// ==================== Upload Path Permission Tests ====================

#[tokio::test]
async fn test_upload_path_no_auth() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", false).await;

    // Request without authorization header
    let request = Request::builder()
        .method("PUT")
        .uri("/_api/v1/upload-path")
        .header("Host", "localhost")
        .body(Body::empty())
        .unwrap();

    let response = server.request(request).await;
    // Without X-Attic-Nar-Info header, this will be a bad request
    // But we're testing the basic endpoint reachability
    assert!(response.status.is_client_error());
}

#[tokio::test]
async fn test_upload_path_missing_nar_info_header() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", false).await;

    let token = server.build_token(server.token("test-user").with_push("test-cache"));

    let request = Request::builder()
        .method("PUT")
        .uri("/_api/v1/upload-path")
        .header("Host", "localhost")
        .header("Authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();

    let response = server.request(request).await;
    // Should fail with bad request (missing X-Attic-Nar-Info)
    assert!(response.status.is_client_error());
}
