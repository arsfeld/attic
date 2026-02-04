//! Tests for the local storage backend.

use std::io::Cursor;

use tempfile::TempDir;
use tokio::io::AsyncReadExt;

use crate::storage::{Download, LocalBackend, LocalStorageConfig, StorageBackend};

// ==================== Basic Storage Operations ====================

#[tokio::test]
async fn test_local_storage_upload_and_download() {
    let temp_dir = TempDir::new().unwrap();
    let config = LocalStorageConfig::new_for_test(temp_dir.path().to_path_buf());
    let backend = LocalBackend::new(config).await.unwrap();

    // Upload some data
    let data = b"hello world";
    let mut cursor = Cursor::new(data.to_vec());
    let remote_file = backend
        .upload_file("test-file".to_string(), &mut cursor)
        .await
        .unwrap();

    // Download and verify
    let download = backend.download_file_db(&remote_file, false).await.unwrap();
    match download {
        Download::AsyncRead(mut reader) => {
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, data);
        }
        Download::Url(_) => panic!("Expected AsyncRead for local storage"),
    }
}

#[tokio::test]
async fn test_local_storage_delete() {
    let temp_dir = TempDir::new().unwrap();
    let config = LocalStorageConfig::new_for_test(temp_dir.path().to_path_buf());
    let backend = LocalBackend::new(config).await.unwrap();

    // Upload some data
    let data = b"to be deleted";
    let mut cursor = Cursor::new(data.to_vec());
    let remote_file = backend
        .upload_file("delete-me".to_string(), &mut cursor)
        .await
        .unwrap();

    // Verify it exists
    let download = backend.download_file_db(&remote_file, false).await;
    assert!(download.is_ok());

    // Delete it
    backend.delete_file_db(&remote_file).await.unwrap();

    // Verify it's gone
    let download_after = backend.download_file_db(&remote_file, false).await;
    assert!(download_after.is_err());
}

#[tokio::test]
async fn test_local_storage_make_db_reference() {
    let temp_dir = TempDir::new().unwrap();
    let config = LocalStorageConfig::new_for_test(temp_dir.path().to_path_buf());
    let backend = LocalBackend::new(config).await.unwrap();

    let reference = backend
        .make_db_reference("some-file".to_string())
        .await
        .unwrap();

    // Should be a Local variant
    assert!(matches!(reference, crate::storage::RemoteFile::Local(_)));
}

#[tokio::test]
async fn test_local_storage_nested_directory_structure() {
    let temp_dir = TempDir::new().unwrap();
    let config = LocalStorageConfig::new_for_test(temp_dir.path().to_path_buf());
    let backend = LocalBackend::new(config).await.unwrap();

    // Files should be stored in nested directories based on their name
    let data = b"nested file";
    let mut cursor = Cursor::new(data.to_vec());
    let remote_file = backend
        .upload_file("abcdef123456".to_string(), &mut cursor)
        .await
        .unwrap();

    // Should succeed
    let download = backend.download_file_db(&remote_file, false).await;
    assert!(download.is_ok());
}

#[tokio::test]
async fn test_local_storage_version_file_created() {
    let temp_dir = TempDir::new().unwrap();
    let config = LocalStorageConfig::new_for_test(temp_dir.path().to_path_buf());
    let _backend = LocalBackend::new(config).await.unwrap();

    // VERSION file should be created
    let version_path = temp_dir.path().join("VERSION");
    assert!(version_path.exists());
}

#[tokio::test]
async fn test_local_storage_remote_file_id() {
    let temp_dir = TempDir::new().unwrap();
    let config = LocalStorageConfig::new_for_test(temp_dir.path().to_path_buf());
    let backend = LocalBackend::new(config).await.unwrap();

    let reference = backend
        .make_db_reference("test-file".to_string())
        .await
        .unwrap();
    let file_id = reference.remote_file_id();

    // Should start with "local:"
    assert!(file_id.starts_with("local:"));
}
