//! Test configuration builder.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use attic_token::HS256Key;

use crate::config::{
    ChunkingConfig, CompressionConfig, CompressionType, Config, DatabaseConfig,
    GarbageCollectionConfig, JWTConfig, JWTSigningConfig, StorageConfig, WebUiConfig,
};
use crate::storage::LocalStorageConfig;

/// Builder for creating test configurations.
pub struct TestConfigBuilder {
    listen: SocketAddr,
    allowed_hosts: Vec<String>,
    api_endpoint: Option<String>,
    substituter_endpoint: Option<String>,
    soft_delete_caches: bool,
    require_proof_of_possession: bool,
    max_nar_info_size: usize,
    database_url: String,
    storage_path: PathBuf,
    jwt_secret: HS256Key,
    nar_size_threshold: usize,
}

impl TestConfigBuilder {
    /// Creates a new test config builder with sensible defaults.
    pub fn new(storage_path: PathBuf, database_url: String, jwt_secret: HS256Key) -> Self {
        Self {
            listen: "[::]:8080".parse().unwrap(),
            allowed_hosts: vec![],
            api_endpoint: Some("http://localhost:8080/".to_string()),
            substituter_endpoint: None,
            soft_delete_caches: false,
            require_proof_of_possession: false,
            max_nar_info_size: 1024 * 1024,
            database_url,
            storage_path,
            jwt_secret,
            nar_size_threshold: 0, // Disable chunking by default for simpler tests
        }
    }

    /// Enable soft delete for caches.
    pub fn with_soft_delete(mut self) -> Self {
        self.soft_delete_caches = true;
        self
    }

    /// Enable proof of possession requirement.
    pub fn with_proof_of_possession(mut self) -> Self {
        self.require_proof_of_possession = true;
        self
    }

    /// Set the NAR size threshold for chunking.
    pub fn with_chunking_threshold(mut self, threshold: usize) -> Self {
        self.nar_size_threshold = threshold;
        self
    }

    /// Build the configuration.
    pub fn build(self) -> Config {
        Config {
            listen: self.listen,
            allowed_hosts: self.allowed_hosts,
            api_endpoint: self.api_endpoint,
            substituter_endpoint: self.substituter_endpoint,
            soft_delete_caches: self.soft_delete_caches,
            require_proof_of_possession: self.require_proof_of_possession,
            max_nar_info_size: self.max_nar_info_size,
            database: DatabaseConfig {
                url: self.database_url,
                heartbeat: false,
                auth_token: None,
                local_replica_path: None,
                sync_interval: None,
            },
            storage: StorageConfig::Local(LocalStorageConfig::new_for_test(self.storage_path)),
            chunking: ChunkingConfig {
                nar_size_threshold: self.nar_size_threshold,
                min_size: 16 * 1024,
                avg_size: 64 * 1024,
                max_size: 256 * 1024,
            },
            compression: CompressionConfig {
                r#type: CompressionType::None,
                level: None,
            },
            garbage_collection: GarbageCollectionConfig {
                interval: Duration::from_secs(0),
                default_retention_period: Duration::ZERO,
            },
            jwt: JWTConfig {
                token_bound_issuer: None,
                token_bound_audiences: None,
                signing_config: JWTSigningConfig::HS256SignAndVerify(self.jwt_secret),
            },
            web_ui: WebUiConfig::default(),
            _depreated_token_hs256_secret: None,
        }
    }
}
