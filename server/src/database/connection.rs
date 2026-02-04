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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // ==================== TursoConfig Tests ====================

    #[test]
    fn test_is_remote_libsql_url() {
        let config = TursoConfig {
            url: "libsql://my-db.turso.io".to_string(),
            auth_token: Some("token".to_string()),
            local_replica_path: None,
            sync_interval: Duration::from_secs(60),
        };
        assert!(config.is_remote());
    }

    #[test]
    fn test_is_remote_https_url() {
        let config = TursoConfig {
            url: "https://my-db.turso.io".to_string(),
            auth_token: Some("token".to_string()),
            local_replica_path: None,
            sync_interval: Duration::from_secs(60),
        };
        assert!(config.is_remote());
    }

    #[test]
    fn test_is_not_remote_sqlite_url() {
        let config = TursoConfig {
            url: "sqlite:///path/to/db.sqlite".to_string(),
            auth_token: None,
            local_replica_path: None,
            sync_interval: Duration::from_secs(60),
        };
        assert!(!config.is_remote());
    }

    #[test]
    fn test_is_not_remote_sqlite_short_url() {
        let config = TursoConfig {
            url: "sqlite:/path/to/db.sqlite".to_string(),
            auth_token: None,
            local_replica_path: None,
            sync_interval: Duration::from_secs(60),
        };
        assert!(!config.is_remote());
    }

    #[test]
    fn test_is_not_remote_plain_path() {
        let config = TursoConfig {
            url: "/path/to/db.sqlite".to_string(),
            auth_token: None,
            local_replica_path: None,
            sync_interval: Duration::from_secs(60),
        };
        assert!(!config.is_remote());
    }

    #[test]
    fn test_is_not_remote_memory_db() {
        let config = TursoConfig {
            url: ":memory:".to_string(),
            auth_token: None,
            local_replica_path: None,
            sync_interval: Duration::from_secs(60),
        };
        assert!(!config.is_remote());
    }

    #[test]
    fn test_config_with_embedded_replica() {
        let config = TursoConfig {
            url: "libsql://my-db.turso.io".to_string(),
            auth_token: Some("token".to_string()),
            local_replica_path: Some(PathBuf::from("/tmp/replica.db")),
            sync_interval: Duration::from_secs(30),
        };
        assert!(config.is_remote());
        assert!(config.local_replica_path.is_some());
        assert_eq!(config.sync_interval, Duration::from_secs(30));
    }

    // ==================== TursoConnection Tests ====================

    /// Helper to create a temporary database for testing.
    async fn create_temp_db() -> (Arc<TursoConnection>, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test.db");

        let config = TursoConfig {
            url: format!("sqlite://{}", db_path.display()),
            auth_token: None,
            local_replica_path: None,
            sync_interval: Duration::from_secs(60),
        };

        let conn = TursoConnection::connect(config)
            .await
            .expect("Failed to connect");
        (conn, temp_dir)
    }

    #[tokio::test]
    async fn test_connect_local_sqlite() {
        let (conn, _temp_dir) = create_temp_db().await;

        // Verify connection is usable with a query (execute returns row count, not results)
        let mut rows = conn.query("SELECT 1", ()).await.expect("Query failed");
        assert!(rows.next().await.expect("Next failed").is_some());
    }

    #[tokio::test]
    async fn test_connect_memory_database() {
        let config = TursoConfig {
            url: ":memory:".to_string(),
            auth_token: None,
            local_replica_path: None,
            sync_interval: Duration::from_secs(60),
        };

        let conn = TursoConnection::connect(config)
            .await
            .expect("Failed to connect to memory db");

        // Verify connection is usable with a query
        let mut rows = conn.query("SELECT 1", ()).await.expect("Query failed");
        assert!(rows.next().await.expect("Next failed").is_some());
    }

    #[tokio::test]
    async fn test_remote_connection_requires_auth_token() {
        let config = TursoConfig {
            url: "libsql://test.turso.io".to_string(),
            auth_token: None, // Missing auth token
            local_replica_path: None,
            sync_interval: Duration::from_secs(60),
        };

        let result = TursoConnection::connect(config).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("auth_token is required"));
    }

    #[tokio::test]
    async fn test_execute_create_table() {
        let (conn, _temp_dir) = create_temp_db().await;

        let result = conn
            .execute(
                "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)",
                (),
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_insert_and_count() {
        let (conn, _temp_dir) = create_temp_db().await;

        conn.execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)",
            (),
        )
        .await
        .expect("Create table failed");

        // Insert a row
        let affected = conn
            .execute("INSERT INTO test_table (name) VALUES (?1)", ["Alice"])
            .await
            .expect("Insert failed");
        assert_eq!(affected, 1);

        // Insert multiple rows
        conn.execute("INSERT INTO test_table (name) VALUES (?1)", ["Bob"])
            .await
            .expect("Insert failed");
        conn.execute("INSERT INTO test_table (name) VALUES (?1)", ["Charlie"])
            .await
            .expect("Insert failed");

        // Query count
        let mut rows = conn
            .query("SELECT COUNT(*) FROM test_table", ())
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let count: i64 = row.get(0).expect("Get failed");
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_query_with_parameters() {
        let (conn, _temp_dir) = create_temp_db().await;

        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
            (),
        )
        .await
        .expect("Create table failed");

        conn.execute(
            "INSERT INTO users (name, age) VALUES (?1, ?2)",
            ("Alice", 30),
        )
        .await
        .expect("Insert failed");

        conn.execute("INSERT INTO users (name, age) VALUES (?1, ?2)", ("Bob", 25))
            .await
            .expect("Insert failed");

        // Query with parameter
        let mut rows = conn
            .query("SELECT name, age FROM users WHERE age > ?1", [26])
            .await
            .expect("Query failed");

        let row = rows.next().await.expect("Next failed").expect("No row");
        let name: String = row.get(0).expect("Get name failed");
        let age: i64 = row.get(1).expect("Get age failed");

        assert_eq!(name, "Alice");
        assert_eq!(age, 30);

        // Should be no more rows
        assert!(rows.next().await.expect("Next failed").is_none());
    }

    #[tokio::test]
    async fn test_transaction_commit() {
        let (conn, _temp_dir) = create_temp_db().await;

        conn.execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)",
            (),
        )
        .await
        .expect("Create table failed");

        // Start transaction
        conn.begin_transaction().await.expect("Begin failed");

        conn.execute("INSERT INTO test_table (value) VALUES (?1)", ["in-txn"])
            .await
            .expect("Insert failed");

        // Commit
        conn.commit().await.expect("Commit failed");

        // Verify data persisted
        let mut rows = conn
            .query("SELECT value FROM test_table", ())
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let value: String = row.get(0).expect("Get failed");
        assert_eq!(value, "in-txn");
    }

    #[tokio::test]
    async fn test_transaction_rollback() {
        let (conn, _temp_dir) = create_temp_db().await;

        conn.execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)",
            (),
        )
        .await
        .expect("Create table failed");

        // Insert initial data outside transaction
        conn.execute("INSERT INTO test_table (value) VALUES (?1)", ["before"])
            .await
            .expect("Insert failed");

        // Start transaction
        conn.begin_transaction().await.expect("Begin failed");

        conn.execute("INSERT INTO test_table (value) VALUES (?1)", ["in-txn"])
            .await
            .expect("Insert failed");

        // Rollback
        conn.rollback().await.expect("Rollback failed");

        // Verify only pre-txn data exists
        let mut rows = conn
            .query("SELECT COUNT(*) FROM test_table", ())
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let count: i64 = row.get(0).expect("Get failed");
        assert_eq!(count, 1); // Only "before" row should exist
    }

    #[tokio::test]
    async fn test_config_accessor() {
        let (conn, _temp_dir) = create_temp_db().await;

        let config = conn.config();
        assert!(!config.is_remote());
        assert!(config.auth_token.is_none());
    }

    #[tokio::test]
    async fn test_sync_noop_for_local() {
        let (conn, _temp_dir) = create_temp_db().await;

        // sync() should be a no-op for local databases
        let result = conn.sync().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_conn_read_access() {
        let (conn, _temp_dir) = create_temp_db().await;

        conn.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY)", ())
            .await
            .expect("Create failed");

        // Use conn() for read access
        let guard = conn.conn().await;
        let mut rows = guard
            .query("SELECT 1", ())
            .await
            .expect("Query via guard failed");
        assert!(rows.next().await.expect("Next failed").is_some());
    }

    #[tokio::test]
    async fn test_sqlite_url_prefix_stripping() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test.db");

        // Test with sqlite:// prefix
        let config1 = TursoConfig {
            url: format!("sqlite://{}", db_path.display()),
            auth_token: None,
            local_replica_path: None,
            sync_interval: Duration::from_secs(60),
        };
        let conn1 = TursoConnection::connect(config1).await;
        assert!(conn1.is_ok());
        drop(conn1);

        // Clean up and test with sqlite: prefix (single slash)
        let db_path2 = temp_dir.path().join("test2.db");
        let config2 = TursoConfig {
            url: format!("sqlite:{}", db_path2.display()),
            auth_token: None,
            local_replica_path: None,
            sync_interval: Duration::from_secs(60),
        };
        let conn2 = TursoConnection::connect(config2).await;
        assert!(conn2.is_ok());
    }

    #[tokio::test]
    async fn test_wal_mode_enabled_for_local() {
        let (conn, _temp_dir) = create_temp_db().await;

        // Check that WAL mode was set
        let mut rows = conn
            .query("PRAGMA journal_mode", ())
            .await
            .expect("PRAGMA query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let mode: String = row.get(0).expect("Get failed");
        assert_eq!(mode.to_lowercase(), "wal");
    }

    #[tokio::test]
    async fn test_debug_formatting() {
        let (conn, _temp_dir) = create_temp_db().await;

        let debug_str = format!("{:?}", conn);
        assert!(debug_str.contains("TursoConnection"));
        assert!(debug_str.contains("config"));
    }

    #[tokio::test]
    async fn test_concurrent_reads() {
        let (conn, _temp_dir) = create_temp_db().await;

        conn.execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value INTEGER)",
            (),
        )
        .await
        .expect("Create failed");

        for i in 0..10 {
            conn.execute("INSERT INTO test_table (value) VALUES (?1)", [i])
                .await
                .expect("Insert failed");
        }

        // Spawn multiple concurrent read tasks
        let conn_clone = conn.clone();
        let handles: Vec<_> = (0..5)
            .map(|_| {
                let c = conn_clone.clone();
                tokio::spawn(async move {
                    let mut rows = c
                        .query("SELECT SUM(value) FROM test_table", ())
                        .await
                        .expect("Query failed");
                    let row = rows.next().await.expect("Next failed").expect("No row");
                    let sum: i64 = row.get(0).expect("Get failed");
                    sum
                })
            })
            .collect();

        // All should return the same sum (0+1+2+...+9 = 45)
        for handle in handles {
            let sum = handle.await.expect("Task failed");
            assert_eq!(sum, 45);
        }
    }

    // ==================== Error Handling Tests ====================

    #[tokio::test]
    async fn test_query_error_invalid_sql() {
        let (conn, _temp_dir) = create_temp_db().await;

        let result = conn.query("INVALID SQL SYNTAX", ()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_error_invalid_table() {
        let (conn, _temp_dir) = create_temp_db().await;

        let result = conn
            .execute("INSERT INTO nonexistent_table (col) VALUES (1)", ())
            .await;
        assert!(result.is_err());
    }
}
