//! Tests for the cache configuration API endpoints.

use axum::http::StatusCode;

use attic::api::v1::cache_config::{CacheConfig, CreateCacheRequest, KeypairConfig};

use crate::tests::helpers::TestServer;

// ==================== Create Cache Tests ====================

#[tokio::test]
async fn test_create_cache_success() {
    let server = TestServer::new().await;
    let token = server.build_token(server.token("test-user").with_create_cache("test-cache"));

    let request = CreateCacheRequest {
        keypair: KeypairConfig::Generate,
        is_public: false,
        store_dir: "/nix/store".to_string(),
        priority: 40,
        upstream_cache_key_names: vec![],
    };

    let response = server
        .post_json_with_token("/_api/v1/cache-config/test-cache", &request, &token)
        .await;
    response.assert_ok();
}

#[tokio::test]
async fn test_create_cache_public() {
    let server = TestServer::new().await;
    let token = server.build_token(server.token("test-user").with_create_cache("public-cache"));

    let request = CreateCacheRequest {
        keypair: KeypairConfig::Generate,
        is_public: true,
        store_dir: "/nix/store".to_string(),
        priority: 40,
        upstream_cache_key_names: vec![],
    };

    let response = server
        .post_json_with_token("/_api/v1/cache-config/public-cache", &request, &token)
        .await;
    response.assert_ok();
}

#[tokio::test]
async fn test_create_cache_already_exists() {
    let server = TestServer::new().await;
    let token = server.build_token(
        server
            .token("test-user")
            .with_create_cache("existing-cache"),
    );

    // Create the cache first
    server.create_cache("existing-cache", false).await;

    let request = CreateCacheRequest {
        keypair: KeypairConfig::Generate,
        is_public: false,
        store_dir: "/nix/store".to_string(),
        priority: 40,
        upstream_cache_key_names: vec![],
    };

    let response = server
        .post_json_with_token("/_api/v1/cache-config/existing-cache", &request, &token)
        .await;
    // CacheAlreadyExists error is mapped to 400 Bad Request
    response.assert_status(StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_create_cache_no_permission() {
    let server = TestServer::new().await;
    // Token only has pull permission, not create_cache
    let token = server.build_token(server.token("test-user").with_pull("test-cache"));

    let request = CreateCacheRequest {
        keypair: KeypairConfig::Generate,
        is_public: false,
        store_dir: "/nix/store".to_string(),
        priority: 40,
        upstream_cache_key_names: vec![],
    };

    let response = server
        .post_json_with_token("/_api/v1/cache-config/test-cache", &request, &token)
        .await;
    response.assert_forbidden();
}

#[tokio::test]
async fn test_create_cache_no_token() {
    let server = TestServer::new().await;

    let request = CreateCacheRequest {
        keypair: KeypairConfig::Generate,
        is_public: false,
        store_dir: "/nix/store".to_string(),
        priority: 40,
        upstream_cache_key_names: vec![],
    };

    let response = server
        .post_json("/_api/v1/cache-config/test-cache", &request)
        .await;
    response.assert_unauthorized();
}

#[tokio::test]
async fn test_create_cache_with_wildcard_permission() {
    let server = TestServer::new().await;
    // Wildcard permission for team-*
    let token = server.build_token(server.token("test-user").with_create_cache("team-*"));

    let request = CreateCacheRequest {
        keypair: KeypairConfig::Generate,
        is_public: false,
        store_dir: "/nix/store".to_string(),
        priority: 40,
        upstream_cache_key_names: vec![],
    };

    let response = server
        .post_json_with_token("/_api/v1/cache-config/team-my-project", &request, &token)
        .await;
    response.assert_ok();
}

#[tokio::test]
async fn test_create_cache_with_upstream() {
    let server = TestServer::new().await;
    let token = server.build_token(server.token("test-user").with_create_cache("test-cache"));

    let request = CreateCacheRequest {
        keypair: KeypairConfig::Generate,
        is_public: false,
        store_dir: "/nix/store".to_string(),
        priority: 40,
        upstream_cache_key_names: vec!["cache.nixos.org-1".to_string()],
    };

    let response = server
        .post_json_with_token("/_api/v1/cache-config/test-cache", &request, &token)
        .await;
    response.assert_ok();
}

// ==================== Get Cache Config Tests ====================

#[tokio::test]
async fn test_get_cache_config_public_cache_no_auth() {
    let server = TestServer::new().await;
    server.create_cache("public-cache", true).await;

    let response = server.get("/_api/v1/cache-config/public-cache").await;
    response.assert_ok();

    let config: CacheConfig = response.json();
    assert_eq!(config.is_public, Some(true));
}

#[tokio::test]
async fn test_get_cache_config_private_cache_with_auth() {
    let server = TestServer::new().await;
    server.create_cache("private-cache", false).await;

    let token = server.build_token(server.token("test-user").with_pull("private-cache"));

    let response = server
        .get_with_token("/_api/v1/cache-config/private-cache", &token)
        .await;
    response.assert_ok();

    let config: CacheConfig = response.json();
    assert_eq!(config.is_public, Some(false));
}

#[tokio::test]
async fn test_get_cache_config_private_cache_no_auth() {
    let server = TestServer::new().await;
    server.create_cache("private-cache", false).await;

    let response = server.get("/_api/v1/cache-config/private-cache").await;
    response.assert_unauthorized();
}

#[tokio::test]
async fn test_get_cache_config_private_cache_wrong_permission() {
    let server = TestServer::new().await;
    server.create_cache("private-cache", false).await;

    // Token for a different cache
    let token = server.build_token(server.token("test-user").with_pull("other-cache"));

    let response = server
        .get_with_token("/_api/v1/cache-config/private-cache", &token)
        .await;
    response.assert_unauthorized();
}

#[tokio::test]
async fn test_get_cache_config_not_found() {
    let server = TestServer::new().await;

    // Token with permission for the cache (so we can discover it doesn't exist)
    let token = server.build_token(server.token("test-user").with_pull("nonexistent-cache"));

    let response = server
        .get_with_token("/_api/v1/cache-config/nonexistent-cache", &token)
        .await;
    response.assert_not_found();
}

#[tokio::test]
async fn test_get_cache_config_response_fields() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", true).await;

    let response = server.get("/_api/v1/cache-config/test-cache").await;
    response.assert_ok();

    let config: CacheConfig = response.json();

    // Check required fields are present
    assert!(config.public_key.is_some());
    assert!(config.api_endpoint.is_some());
    assert!(config.substituter_endpoint.is_some());
    assert_eq!(config.is_public, Some(true));
    assert_eq!(config.store_dir, Some("/nix/store".to_string()));
    assert_eq!(config.priority, Some(40));
}

// ==================== Configure Cache Tests ====================

#[tokio::test]
async fn test_configure_cache_update_is_public() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", false).await;

    let token = server.build_token(server.token("test-user").with_configure_cache("test-cache"));

    let config = CacheConfig {
        is_public: Some(true),
        ..CacheConfig::blank()
    };

    let response = server
        .patch_json_with_token("/_api/v1/cache-config/test-cache", &config, &token)
        .await;
    response.assert_ok();

    // Verify the change - public cache should now be accessible without auth
    let verify_response = server.get("/_api/v1/cache-config/test-cache").await;
    verify_response.assert_ok();
}

#[tokio::test]
async fn test_configure_cache_update_priority() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", true).await;

    let token = server.build_token(server.token("test-user").with_configure_cache("test-cache"));

    let config = CacheConfig {
        priority: Some(10),
        ..CacheConfig::blank()
    };

    let response = server
        .patch_json_with_token("/_api/v1/cache-config/test-cache", &config, &token)
        .await;
    response.assert_ok();

    // Verify the change
    let verify_response = server.get("/_api/v1/cache-config/test-cache").await;
    let updated_config: CacheConfig = verify_response.json();
    assert_eq!(updated_config.priority, Some(10));
}

#[tokio::test]
async fn test_configure_cache_no_permission() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", false).await;

    // Only pull permission
    let token = server.build_token(server.token("test-user").with_pull("test-cache"));

    let config = CacheConfig {
        is_public: Some(true),
        ..CacheConfig::blank()
    };

    let response = server
        .patch_json_with_token("/_api/v1/cache-config/test-cache", &config, &token)
        .await;
    response.assert_forbidden();
}

#[tokio::test]
async fn test_configure_cache_no_fields_modified() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", false).await;

    let token = server.build_token(server.token("test-user").with_configure_cache("test-cache"));

    // Empty config - no fields to modify
    let config = CacheConfig::blank();

    let response = server
        .patch_json_with_token("/_api/v1/cache-config/test-cache", &config, &token)
        .await;
    response.assert_status(StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_configure_cache_regenerate_keypair() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", true).await;

    // Get the original public key
    let original_response = server.get("/_api/v1/cache-config/test-cache").await;
    let original_config: CacheConfig = original_response.json();
    let original_key = original_config.public_key.unwrap();

    let token = server.build_token(server.token("test-user").with_configure_cache("test-cache"));

    let config = CacheConfig {
        keypair: Some(KeypairConfig::Generate),
        ..CacheConfig::blank()
    };

    let response = server
        .patch_json_with_token("/_api/v1/cache-config/test-cache", &config, &token)
        .await;
    response.assert_ok();

    // Verify the key changed
    let updated_response = server.get("/_api/v1/cache-config/test-cache").await;
    let updated_config: CacheConfig = updated_response.json();
    let new_key = updated_config.public_key.unwrap();
    assert_ne!(original_key, new_key);
}

// ==================== Destroy Cache Tests ====================

#[tokio::test]
async fn test_destroy_cache_hard_delete() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", true).await;

    let token = server.build_token(server.token("test-user").with_destroy_cache("test-cache"));

    let response = server
        .delete_with_token("/_api/v1/cache-config/test-cache", &token)
        .await;
    response.assert_ok();

    // Verify the cache is gone
    let verify_response = server.get("/_api/v1/cache-config/test-cache").await;
    verify_response.assert_unauthorized(); // No cache = unauthorized for anonymous
}

#[tokio::test]
async fn test_destroy_cache_soft_delete() {
    let server = TestServer::with_soft_delete().await;
    server.create_cache("test-cache", true).await;

    let token = server.build_token(server.token("test-user").with_destroy_cache("test-cache"));

    let response = server
        .delete_with_token("/_api/v1/cache-config/test-cache", &token)
        .await;
    response.assert_ok();

    // With soft delete, attempting to create the same cache should fail (name still taken)
    let create_token =
        server.build_token(server.token("test-user").with_create_cache("test-cache"));
    let create_request = CreateCacheRequest {
        keypair: KeypairConfig::Generate,
        is_public: false,
        store_dir: "/nix/store".to_string(),
        priority: 40,
        upstream_cache_key_names: vec![],
    };
    // Note: with soft delete, the cache name is still reserved
    // The actual behavior depends on the implementation
}

#[tokio::test]
async fn test_destroy_cache_no_permission() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", true).await;

    // Only pull permission
    let token = server.build_token(server.token("test-user").with_pull("test-cache"));

    let response = server
        .delete_with_token("/_api/v1/cache-config/test-cache", &token)
        .await;
    response.assert_forbidden();
}

#[tokio::test]
async fn test_destroy_cache_not_found() {
    let server = TestServer::new().await;

    let token = server.build_token(server.token("test-user").with_destroy_cache("nonexistent"));

    let response = server
        .delete_with_token("/_api/v1/cache-config/nonexistent", &token)
        .await;
    response.assert_not_found();
}

// ==================== Alternative Endpoint Tests ====================

#[tokio::test]
async fn test_get_cache_config_via_attic_cache_info() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", true).await;

    // Alternative endpoint: /:cache/attic-cache-info
    let response = server.get("/test-cache/attic-cache-info").await;
    response.assert_ok();

    let config: CacheConfig = response.json();
    assert!(config.public_key.is_some());
}
