//! Tests for the get missing paths endpoint.

use attic::api::v1::get_missing_paths::{GetMissingPathsRequest, GetMissingPathsResponse};
use attic::nix_store::StorePathHash;

use crate::tests::helpers::TestServer;

// ==================== Get Missing Paths Tests ====================

#[tokio::test]
async fn test_get_missing_paths_all_missing() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", false).await;

    let token = server.build_token(server.token("test-user").with_push("test-cache"));

    let request = GetMissingPathsRequest {
        cache: "test-cache".parse().unwrap(),
        store_path_hashes: vec![
            StorePathHash::new("00000000000000000000000000000000".to_string()).unwrap(),
            StorePathHash::new("11111111111111111111111111111111".to_string()).unwrap(),
        ],
    };

    let response = server
        .post_json_with_token("/_api/v1/get-missing-paths", &request, &token)
        .await;
    response.assert_ok();

    let result: GetMissingPathsResponse = response.json();
    assert_eq!(result.missing_paths.len(), 2);
}

#[tokio::test]
async fn test_get_missing_paths_empty_request() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", false).await;

    let token = server.build_token(server.token("test-user").with_push("test-cache"));

    let request = GetMissingPathsRequest {
        cache: "test-cache".parse().unwrap(),
        store_path_hashes: vec![],
    };

    let response = server
        .post_json_with_token("/_api/v1/get-missing-paths", &request, &token)
        .await;
    response.assert_ok();

    let result: GetMissingPathsResponse = response.json();
    assert_eq!(result.missing_paths.len(), 0);
}

#[tokio::test]
async fn test_get_missing_paths_no_permission() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", false).await;

    // Only pull permission, not push
    let token = server.build_token(server.token("test-user").with_pull("test-cache"));

    let request = GetMissingPathsRequest {
        cache: "test-cache".parse().unwrap(),
        store_path_hashes: vec![
            StorePathHash::new("00000000000000000000000000000000".to_string()).unwrap(),
        ],
    };

    let response = server
        .post_json_with_token("/_api/v1/get-missing-paths", &request, &token)
        .await;
    response.assert_forbidden();
}

#[tokio::test]
async fn test_get_missing_paths_no_auth() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", false).await;

    let request = GetMissingPathsRequest {
        cache: "test-cache".parse().unwrap(),
        store_path_hashes: vec![
            StorePathHash::new("00000000000000000000000000000000".to_string()).unwrap(),
        ],
    };

    let response = server
        .post_json("/_api/v1/get-missing-paths", &request)
        .await;
    response.assert_unauthorized();
}

#[tokio::test]
async fn test_get_missing_paths_cache_not_found() {
    let server = TestServer::new().await;

    let token = server.build_token(server.token("test-user").with_push("nonexistent"));

    let request = GetMissingPathsRequest {
        cache: "nonexistent".parse().unwrap(),
        store_path_hashes: vec![
            StorePathHash::new("00000000000000000000000000000000".to_string()).unwrap(),
        ],
    };

    let response = server
        .post_json_with_token("/_api/v1/get-missing-paths", &request, &token)
        .await;
    response.assert_not_found();
}
