//! End-to-end upload and download tests.
//!
//! These tests verify complete workflows from uploading NAR files
//! to downloading them through the binary cache protocol.

use axum::body::Body;
use axum::http::Request;

use attic::api::v1::upload_path::{
    UploadPathNarInfo, ATTIC_NAR_INFO, ATTIC_NAR_INFO_PREAMBLE_SIZE,
};
use attic::nix_store::StorePathHash;

use crate::tests::helpers::{minimal_nar, minimal_nar_hash, TestServer};

// ==================== Full Upload/Download Workflow ====================

#[tokio::test]
async fn test_upload_unchunked_nar() {
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
    let store_path_hash =
        StorePathHash::new("00000000000000000000000000000000".to_string()).unwrap();

    let upload_info = UploadPathNarInfo {
        cache: "test-cache".parse().unwrap(),
        store_path_hash: store_path_hash.clone(),
        store_path: "/nix/store/00000000000000000000000000000000-test".to_string(),
        references: vec![],
        system: None,
        deriver: None,
        sigs: vec![],
        ca: None,
        nar_hash: nar_hash.clone(),
        nar_size: nar_data.len(),
    };

    let upload_info_json = serde_json::to_vec(&upload_info).unwrap();

    // Combine preamble + NAR data
    let mut body = upload_info_json.clone();
    body.extend_from_slice(&nar_data);

    let request = Request::builder()
        .method("PUT")
        .uri("/_api/v1/upload-path")
        .header("Host", "localhost")
        .header("Authorization", format!("Bearer {}", token))
        .header(
            ATTIC_NAR_INFO_PREAMBLE_SIZE,
            upload_info_json.len().to_string(),
        )
        .body(Body::from(body))
        .unwrap();

    let response = server.request(request).await;
    response.assert_ok();

    // Now try to fetch the narinfo
    let narinfo_response = server
        .get_with_token(
            "/test-cache/00000000000000000000000000000000.narinfo",
            &token,
        )
        .await;
    narinfo_response.assert_ok();

    let narinfo_text = narinfo_response.text();
    assert!(narinfo_text.contains("StorePath: /nix/store/00000000000000000000000000000000-test"));
}

#[tokio::test]
async fn test_upload_with_header_nar_info() {
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
    let store_path_hash =
        StorePathHash::new("11111111111111111111111111111111".to_string()).unwrap();

    let upload_info = UploadPathNarInfo {
        cache: "test-cache".parse().unwrap(),
        store_path_hash: store_path_hash.clone(),
        store_path: "/nix/store/11111111111111111111111111111111-test".to_string(),
        references: vec![],
        system: None,
        deriver: None,
        sigs: vec![],
        ca: None,
        nar_hash: nar_hash.clone(),
        nar_size: nar_data.len(),
    };

    let upload_info_json = serde_json::to_string(&upload_info).unwrap();

    // Use X-Attic-Nar-Info header instead of preamble
    let request = Request::builder()
        .method("PUT")
        .uri("/_api/v1/upload-path")
        .header("Host", "localhost")
        .header("Authorization", format!("Bearer {}", token))
        .header(ATTIC_NAR_INFO, upload_info_json)
        .body(Body::from(nar_data))
        .unwrap();

    let response = server.request(request).await;
    response.assert_ok();
}

#[tokio::test]
async fn test_upload_bad_nar_hash() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", false).await;

    let token = server.build_token(server.token("test-user").with_push("test-cache"));

    let nar_data = minimal_nar();
    // Use a wrong hash
    let wrong_hash = attic::hash::Hash::Sha256([0u8; 32]);
    let store_path_hash =
        StorePathHash::new("22222222222222222222222222222222".to_string()).unwrap();

    let upload_info = UploadPathNarInfo {
        cache: "test-cache".parse().unwrap(),
        store_path_hash: store_path_hash.clone(),
        store_path: "/nix/store/22222222222222222222222222222222-test".to_string(),
        references: vec![],
        system: None,
        deriver: None,
        sigs: vec![],
        ca: None,
        nar_hash: wrong_hash,
        nar_size: nar_data.len(),
    };

    let upload_info_json = serde_json::to_string(&upload_info).unwrap();

    let request = Request::builder()
        .method("PUT")
        .uri("/_api/v1/upload-path")
        .header("Host", "localhost")
        .header("Authorization", format!("Bearer {}", token))
        .header(ATTIC_NAR_INFO, upload_info_json)
        .body(Body::from(nar_data))
        .unwrap();

    let response = server.request(request).await;
    // Should fail due to hash mismatch
    assert!(response.status.is_client_error());
}

#[tokio::test]
async fn test_upload_bad_nar_size() {
    let server = TestServer::new().await;
    server.create_cache("test-cache", false).await;

    let token = server.build_token(server.token("test-user").with_push("test-cache"));

    let nar_data = minimal_nar();
    let nar_hash = minimal_nar_hash();
    let store_path_hash =
        StorePathHash::new("33333333333333333333333333333333".to_string()).unwrap();

    let upload_info = UploadPathNarInfo {
        cache: "test-cache".parse().unwrap(),
        store_path_hash: store_path_hash.clone(),
        store_path: "/nix/store/33333333333333333333333333333333-test".to_string(),
        references: vec![],
        system: None,
        deriver: None,
        sigs: vec![],
        ca: None,
        nar_hash,
        nar_size: nar_data.len() + 100, // Wrong size
    };

    let upload_info_json = serde_json::to_string(&upload_info).unwrap();

    let request = Request::builder()
        .method("PUT")
        .uri("/_api/v1/upload-path")
        .header("Host", "localhost")
        .header("Authorization", format!("Bearer {}", token))
        .header(ATTIC_NAR_INFO, upload_info_json)
        .body(Body::from(nar_data))
        .unwrap();

    let response = server.request(request).await;
    // Should fail due to size mismatch
    assert!(response.status.is_client_error());
}
