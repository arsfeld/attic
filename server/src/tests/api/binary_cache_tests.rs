//! Tests for the Nix binary cache protocol endpoints.

use crate::tests::helpers::TestServer;

// ==================== Nix Cache Info Tests ====================

#[tokio::test]
async fn test_nix_cache_info_public_cache() {
    let server = TestServer::new().await;
    server.create_cache("public-cache", true).await;

    let response = server.get("/public-cache/nix-cache-info").await;
    response.assert_ok();

    let body = response.text();
    assert!(body.contains("StoreDir: /nix/store"));
    assert!(body.contains("WantMassQuery: 1"));
    assert!(body.contains("Priority: 40"));
}

#[tokio::test]
async fn test_nix_cache_info_private_cache_no_auth() {
    let server = TestServer::new().await;
    server.create_cache("private-cache", false).await;

    let response = server.get("/private-cache/nix-cache-info").await;
    response.assert_unauthorized();
}

#[tokio::test]
async fn test_nix_cache_info_private_cache_with_auth() {
    let server = TestServer::new().await;
    server.create_cache("private-cache", false).await;

    let token = server.build_token(server.token("test-user").with_pull("private-cache"));

    let response = server
        .get_with_token("/private-cache/nix-cache-info", &token)
        .await;
    response.assert_ok();
}

#[tokio::test]
async fn test_nix_cache_info_not_found() {
    let server = TestServer::new().await;
    let token = server.build_token(server.token("test-user").with_pull("nonexistent"));

    let response = server
        .get_with_token("/nonexistent/nix-cache-info", &token)
        .await;
    response.assert_not_found();
}

// ==================== Narinfo Tests ====================

// Note: Testing narinfo retrieval requires uploading objects first,
// which is covered in the upload tests. Here we test the basic endpoint behavior.

#[tokio::test]
async fn test_narinfo_not_found() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", true).await;

    // Valid store path hash format but doesn't exist
    let response = server
        .get("/test-cache/00000000000000000000000000000000.narinfo")
        .await;
    response.assert_not_found();
}

#[tokio::test]
async fn test_narinfo_invalid_format() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", true).await;

    // Invalid path format (wrong extension)
    let response = server.get("/test-cache/invalid.txt").await;
    response.assert_not_found();
}

#[tokio::test]
async fn test_narinfo_private_cache_no_auth() {
    let server = TestServer::new().await;
    server.create_cache("private-cache", false).await;

    let response = server
        .get("/private-cache/00000000000000000000000000000000.narinfo")
        .await;
    // The object doesn't exist, so we get 404 (NoSuchObject) rather than 401
    // This is because the cache requires the object to exist before checking permissions
    response.assert_not_found();
}

// ==================== NAR Download Tests ====================

#[tokio::test]
async fn test_nar_not_found() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", true).await;

    let response = server
        .get("/test-cache/nar/00000000000000000000000000000000.nar")
        .await;
    response.assert_not_found();
}

#[tokio::test]
async fn test_nar_invalid_format() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", true).await;

    // Invalid extension
    let response = server.get("/test-cache/nar/invalid.txt").await;
    response.assert_not_found();
}

#[tokio::test]
async fn test_nar_private_cache_no_auth() {
    let server = TestServer::new().await;
    server.create_cache("private-cache", false).await;

    let response = server
        .get("/private-cache/nar/00000000000000000000000000000000.nar")
        .await;
    // The object doesn't exist, so we get 404 (NoSuchObject) rather than 401
    response.assert_not_found();
}

// ==================== Cache Visibility Header Tests ====================

#[tokio::test]
async fn test_public_cache_visibility_header() {
    let server = TestServer::new().await;
    server.create_cache("public-cache", true).await;

    let response = server.get("/public-cache/nix-cache-info").await;
    response.assert_ok();

    // Check for X-Attic-Cache-Visibility header
    let visibility = response
        .headers
        .get("x-attic-cache-visibility")
        .map(|v| v.to_str().unwrap());
    assert_eq!(visibility, Some("public"));
}
