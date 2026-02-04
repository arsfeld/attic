//! Tests for NAR deduplication behavior.

use axum::body::Body;
use axum::http::Request;

use attic::api::v1::upload_path::{
    UploadPathNarInfo, UploadPathResult, UploadPathResultKind, ATTIC_NAR_INFO,
};
use attic::nix_store::StorePathHash;

use crate::tests::helpers::{minimal_nar, minimal_nar_hash, TestServer};

#[tokio::test]
async fn test_upload_same_nar_to_same_cache_twice() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", false).await;

    let token = server.build_token(
        server
            .token("test-user")
            .with_push("test-cache")
            .with_pull("test-cache"),
    );

    let nar_data = minimal_nar();
    let nar_hash = minimal_nar_hash();

    // First upload - use valid base32 characters (no e, o, u, t)
    let store_path_hash1 =
        StorePathHash::new("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()).unwrap();
    let upload_info1 = UploadPathNarInfo {
        cache: "test-cache".parse().unwrap(),
        store_path_hash: store_path_hash1,
        store_path: "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test1".to_string(),
        references: vec![],
        system: None,
        deriver: None,
        sigs: vec![],
        ca: None,
        nar_hash: nar_hash.clone(),
        nar_size: nar_data.len(),
    };

    let request1 = Request::builder()
        .method("PUT")
        .uri("/_api/v1/upload-path")
        .header("Host", "localhost")
        .header("Authorization", format!("Bearer {}", token))
        .header(
            ATTIC_NAR_INFO,
            serde_json::to_string(&upload_info1).unwrap(),
        )
        .body(Body::from(nar_data.clone()))
        .unwrap();

    let response1 = server.request(request1).await;
    response1.assert_ok();

    let result1: UploadPathResult = response1.json();
    assert!(matches!(result1.kind, UploadPathResultKind::Uploaded));

    // Second upload with same NAR but different store path
    let store_path_hash2 =
        StorePathHash::new("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()).unwrap();
    let upload_info2 = UploadPathNarInfo {
        cache: "test-cache".parse().unwrap(),
        store_path_hash: store_path_hash2,
        store_path: "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-test2".to_string(),
        references: vec![],
        system: None,
        deriver: None,
        sigs: vec![],
        ca: None,
        nar_hash: nar_hash.clone(),
        nar_size: nar_data.len(),
    };

    let request2 = Request::builder()
        .method("PUT")
        .uri("/_api/v1/upload-path")
        .header("Host", "localhost")
        .header("Authorization", format!("Bearer {}", token))
        .header(
            ATTIC_NAR_INFO,
            serde_json::to_string(&upload_info2).unwrap(),
        )
        .body(Body::from(nar_data))
        .unwrap();

    let response2 = server.request(request2).await;
    response2.assert_ok();

    let result2: UploadPathResult = response2.json();
    // Second upload should be deduplicated
    assert!(matches!(result2.kind, UploadPathResultKind::Deduplicated));
}

#[tokio::test]
async fn test_upload_same_nar_to_different_caches() {
    let server = TestServer::new().await;
    server.create_cache("cache-a", false).await;
    server.create_cache("cache-b", false).await;

    let token = server.build_token(
        server
            .token("test-user")
            .with_push("cache-a")
            .with_push("cache-b"),
    );

    let nar_data = minimal_nar();
    let nar_hash = minimal_nar_hash();
    let store_path_hash =
        StorePathHash::new("cccccccccccccccccccccccccccccccc".to_string()).unwrap();

    // Upload to cache-a
    let upload_info_a = UploadPathNarInfo {
        cache: "cache-a".parse().unwrap(),
        store_path_hash: store_path_hash.clone(),
        store_path: "/nix/store/cccccccccccccccccccccccccccccccc-test".to_string(),
        references: vec![],
        system: None,
        deriver: None,
        sigs: vec![],
        ca: None,
        nar_hash: nar_hash.clone(),
        nar_size: nar_data.len(),
    };

    let request_a = Request::builder()
        .method("PUT")
        .uri("/_api/v1/upload-path")
        .header("Host", "localhost")
        .header("Authorization", format!("Bearer {}", token))
        .header(
            ATTIC_NAR_INFO,
            serde_json::to_string(&upload_info_a).unwrap(),
        )
        .body(Body::from(nar_data.clone()))
        .unwrap();

    let response_a = server.request(request_a).await;
    response_a.assert_ok();

    // Upload same NAR to cache-b (global deduplication)
    let upload_info_b = UploadPathNarInfo {
        cache: "cache-b".parse().unwrap(),
        store_path_hash: store_path_hash.clone(),
        store_path: "/nix/store/cccccccccccccccccccccccccccccccc-test".to_string(),
        references: vec![],
        system: None,
        deriver: None,
        sigs: vec![],
        ca: None,
        nar_hash: nar_hash.clone(),
        nar_size: nar_data.len(),
    };

    let request_b = Request::builder()
        .method("PUT")
        .uri("/_api/v1/upload-path")
        .header("Host", "localhost")
        .header("Authorization", format!("Bearer {}", token))
        .header(
            ATTIC_NAR_INFO,
            serde_json::to_string(&upload_info_b).unwrap(),
        )
        .body(Body::from(nar_data))
        .unwrap();

    let response_b = server.request(request_b).await;
    response_b.assert_ok();

    let result_b: UploadPathResult = response_b.json();
    // Should be deduplicated across caches (global deduplication)
    assert!(matches!(result_b.kind, UploadPathResultKind::Deduplicated));
}
