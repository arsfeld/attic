//! Tests for permission checking in API handlers.

use crate::tests::helpers::TestServer;

// ==================== Permission Discovery Tests ====================

#[tokio::test]
async fn test_discovery_permission_allows_not_found() {
    let server = TestServer::new().await;

    // Token has pull permission for the cache, so it can discover
    let token = server.build_token(server.token("test-user").with_pull("nonexistent"));

    let response = server
        .get_with_token("/_api/v1/cache-config/nonexistent", &token)
        .await;

    // Should return 404 Not Found (not 401) because user has discovery permission
    response.assert_not_found();
}

#[tokio::test]
async fn test_no_discovery_permission_returns_unauthorized() {
    let server = TestServer::new().await;
    server.create_cache("secret-cache", false).await;

    // Token for a completely different cache
    let token = server.build_token(server.token("test-user").with_pull("other-cache"));

    let response = server
        .get_with_token("/_api/v1/cache-config/secret-cache", &token)
        .await;

    // Should return 401 Unauthorized because user cannot discover this cache
    response.assert_unauthorized();
}

#[tokio::test]
async fn test_public_cache_grants_discovery() {
    let server = TestServer::new().await;
    server.create_cache("public-cache", true).await;

    // No token needed for public cache
    let response = server.get("/_api/v1/cache-config/public-cache").await;
    response.assert_ok();
}

// ==================== Permission Escalation Tests ====================

#[tokio::test]
async fn test_pull_only_cannot_push() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", false).await;

    // Token with only pull permission
    let token = server.build_token(server.token("test-user").with_pull("test-cache"));

    // Try to configure cache (requires configure_cache permission)
    let config = serde_json::json!({
        "is_public": true
    });

    let response = server
        .patch_json_with_token("/_api/v1/cache-config/test-cache", &config, &token)
        .await;
    response.assert_forbidden();
}

#[tokio::test]
async fn test_configure_without_retention_permission() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", true).await;

    // Token with configure_cache but NOT configure_cache_retention
    let token = server.build_token(server.token("test-user").with_configure_cache("test-cache"));

    // Try to set retention period (requires configure_cache_retention)
    let config = serde_json::json!({
        "retention_period": { "Period": 3600 }
    });

    let response = server
        .patch_json_with_token("/_api/v1/cache-config/test-cache", &config, &token)
        .await;
    response.assert_forbidden();
}
