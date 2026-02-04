//! Turso/libSQL connection handling.
//!
//! Supports both local SQLite databases and remote Turso databases with
//! optional embedded replicas for low-latency reads.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use libsql::{params::IntoParams, Builder, Connection, Database};
use tokio::sync::RwLock;

use crate::config::DatabaseConfig;

/// Configuration for Turso connection.
#[derive(Debug, Clone)]
pub struct TursoConfig {
    /// Database URL (sqlite:// or libsql://)
    pub url: String,
    /// Auth token for Turso (required for libsql:// URLs)
    pub auth_token: Option<String>,
    /// Path to local embedded replica (optional)
    pub local_replica_path: Option<PathBuf>,
    /// Sync interval for embedded replicas (default: 60 seconds)
    pub sync_interval: Duration,
}

impl TursoConfig {
    pub fn from_database_config(config: &DatabaseConfig) -> Self {
        Self {
            url: config.url.clone(),
            auth_token: config.auth_token.clone(),
            local_replica_path: config.local_replica_path.clone(),
            sync_interval: config.sync_interval.unwrap_or(Duration::from_secs(60)),
        }
    }

    /// Returns true if this is a remote Turso URL.
    pub fn is_remote(&self) -> bool {
        self.url.starts_with("libsql://") || self.url.starts_with("https://")
    }
}

/// A connection to a Turso/libSQL database.
///
/// This wrapper handles both local SQLite and remote Turso connections,
/// including embedded replicas.
pub struct TursoConnection {
    database: Database,
    connection: RwLock<Connection>,
    config: TursoConfig,
}

impl std::fmt::Debug for TursoConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TursoConnection")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl TursoConnection {
    /// Creates a new Turso connection.
    pub async fn connect(config: TursoConfig) -> Result<Arc<Self>> {
        let database = if config.is_remote() {
            let auth_token = config
                .auth_token
                .as_ref()
                .ok_or_else(|| anyhow!("auth_token is required for remote Turso URLs"))?;

            if let Some(ref replica_path) = config.local_replica_path {
                // Use embedded replica for faster reads
                tracing::info!(
                    "Connecting to Turso with embedded replica at {:?}",
                    replica_path
                );
                Builder::new_remote_replica(
                    replica_path.to_string_lossy().to_string(),
                    config.url.clone(),
                    auth_token.clone(),
                )
                .sync_interval(config.sync_interval)
                .build()
                .await?
            } else {
                // Pure remote connection
                tracing::info!("Connecting to Turso remote database");
                Builder::new_remote(config.url.clone(), auth_token.clone())
                    .build()
                    .await?
            }
        } else {
            // Local SQLite database
            let path = config
                .url
                .strip_prefix("sqlite://")
                .or_else(|| config.url.strip_prefix("sqlite:"))
                .unwrap_or(&config.url);

            tracing::info!("Connecting to local SQLite database at {}", path);
            Builder::new_local(path).build().await?
        };

        let connection = database.connect()?;

        // Apply SQLite optimizations for local databases
        if !config.is_remote() || config.local_replica_path.is_some() {
            // These pragmas improve performance for SQLite
            // We ignore errors as these are optimizations, not requirements
            let pragmas = [
                "PRAGMA journal_mode=WAL",
                "PRAGMA synchronous=normal",
                "PRAGMA temp_store=memory",
                "PRAGMA mmap_size=30000000000",
            ];

            for pragma in pragmas {
                if let Err(e) = connection.execute(pragma, ()).await {
                    tracing::debug!("Failed to set pragma ({}): {}", pragma, e);
                }
            }
        }

        // Sync the embedded replica if configured
        if config.is_remote() && config.local_replica_path.is_some() {
            tracing::info!("Performing initial sync of embedded replica");
            database.sync().await?;
        }

        Ok(Arc::new(Self {
            database,
            connection: RwLock::new(connection),
            config,
        }))
    }

    /// Returns a reference to the underlying connection.
    ///
    /// Note: libsql connections are not thread-safe for concurrent writes,
    /// so callers should use this carefully.
    pub async fn conn(&self) -> tokio::sync::RwLockReadGuard<'_, Connection> {
        self.connection.read().await
    }

    /// Returns a mutable reference to the underlying connection.
    pub async fn conn_mut(&self) -> tokio::sync::RwLockWriteGuard<'_, Connection> {
        self.connection.write().await
    }

    /// Executes a query and returns the number of affected rows.
    pub async fn execute<P: IntoParams>(&self, sql: &str, params: P) -> Result<u64> {
        let conn = self.connection.read().await;
        Ok(conn.execute(sql, params).await?)
    }

    /// Executes a query and returns the results.
    pub async fn query<P: IntoParams>(&self, sql: &str, params: P) -> Result<libsql::Rows> {
        let conn = self.connection.read().await;
        Ok(conn.query(sql, params).await?)
    }

    /// Syncs the embedded replica with the remote database.
    ///
    /// This is a no-op for local databases or remote connections without replicas.
    pub async fn sync(&self) -> Result<()> {
        if self.config.is_remote() && self.config.local_replica_path.is_some() {
            self.database.sync().await?;
        }
        Ok(())
    }

    /// Returns the database configuration.
    pub fn config(&self) -> &TursoConfig {
        &self.config
    }

    /// Begins a transaction.
    pub async fn begin_transaction(&self) -> Result<()> {
        self.execute("BEGIN IMMEDIATE", ()).await?;
        Ok(())
    }

    /// Commits the current transaction.
    pub async fn commit(&self) -> Result<()> {
        self.execute("COMMIT", ()).await?;
        Ok(())
    }

    /// Rolls back the current transaction.
    pub async fn rollback(&self) -> Result<()> {
        self.execute("ROLLBACK", ()).await?;
        Ok(())
    }
}
