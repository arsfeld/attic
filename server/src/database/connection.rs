//! Turso/libSQL connection handling.
//!
//! Supports both local SQLite databases and remote Turso databases with
//! optional embedded replicas for low-latency reads.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use libsql::{params::IntoParams, Builder, Connection, Database};
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};

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
    /// Mutex to serialize transaction operations.
    /// SQLite/libsql connections only support one active transaction at a time,
    /// so we need to ensure only one transaction is active across all concurrent requests.
    transaction_lock: Arc<Mutex<()>>,
}

/// A guard that holds a database transaction and releases the lock when dropped.
///
/// This ensures that only one transaction can be active at a time on the connection.
/// The transaction is automatically rolled back if not explicitly committed.
///
/// IMPORTANT: The mutex guard is held until both:
/// 1. The transaction is committed or rolled back
/// 2. This guard is dropped
///
/// When dropped without explicit commit/rollback, the ROLLBACK is executed
/// asynchronously, but the mutex is held until the ROLLBACK completes.
pub struct TransactionGuard {
    connection: Arc<TursoConnection>,
    /// The mutex guard that serializes transactions.
    /// This is wrapped in Option so we can take ownership in Drop and pass it
    /// to the spawned rollback task, ensuring the mutex isn't released until
    /// the rollback completes.
    guard: Option<OwnedMutexGuard<()>>,
    committed: bool,
}

impl TransactionGuard {
    /// Commits the transaction.
    pub async fn commit(mut self) -> Result<()> {
        self.connection.execute("COMMIT", ()).await?;
        self.committed = true;
        // guard will be dropped here, releasing the mutex
        Ok(())
    }

    /// Rolls back the transaction.
    pub async fn rollback(mut self) -> Result<()> {
        self.connection.execute("ROLLBACK", ()).await?;
        self.committed = true; // Mark as handled so Drop doesn't try to rollback again
                               // guard will be dropped here, releasing the mutex
        Ok(())
    }

    /// Returns a reference to the underlying connection for executing queries within the transaction.
    pub fn connection(&self) -> &Arc<TursoConnection> {
        &self.connection
    }
}

impl Drop for TransactionGuard {
    fn drop(&mut self) {
        if !self.committed {
            // Transaction was not committed, we should rollback.
            // Since we can't do async in Drop, we spawn a task.
            //
            // CRITICAL: We take the mutex guard and pass it to the spawned task.
            // This ensures the mutex is NOT released until after the ROLLBACK completes.
            // Without this, a new transaction could start before the ROLLBACK finishes,
            // causing "cannot start a transaction within a transaction" errors.
            let connection = self.connection.clone();
            let guard = self.guard.take();
            tokio::spawn(async move {
                if let Err(e) = connection.execute("ROLLBACK", ()).await {
                    tracing::warn!("Failed to rollback transaction on drop: {}", e);
                }
                // Now the guard is dropped, releasing the mutex
                drop(guard);
            });
        }
        // If committed, the guard is dropped here (or was already dropped if taken)
    }
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
            transaction_lock: Arc::new(Mutex::new(())),
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

    /// Begins a transaction and returns a guard that ensures proper serialization.
    ///
    /// Only one transaction can be active at a time on the connection.
    /// The guard must be used to commit or rollback the transaction.
    /// If the guard is dropped without committing, the transaction is rolled back.
    pub async fn begin_transaction(self: &Arc<Self>) -> Result<TransactionGuard> {
        let guard = Arc::clone(&self.transaction_lock).lock_owned().await;
        self.execute("BEGIN IMMEDIATE", ()).await?;
        Ok(TransactionGuard {
            connection: Arc::clone(self),
            guard: Some(guard),
            committed: false,
        })
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
        let txn = conn.begin_transaction().await.expect("Begin failed");

        conn.execute("INSERT INTO test_table (value) VALUES (?1)", ["in-txn"])
            .await
            .expect("Insert failed");

        // Commit
        txn.commit().await.expect("Commit failed");

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
        let txn = conn.begin_transaction().await.expect("Begin failed");

        conn.execute("INSERT INTO test_table (value) VALUES (?1)", ["in-txn"])
            .await
            .expect("Insert failed");

        // Rollback
        txn.rollback().await.expect("Rollback failed");

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

    // ==================== Concurrency and Transaction Edge Case Tests ====================
    // These tests reproduce issues seen in production with Turso/Hrana:
    // - "SQLite error: cannot commit - no transaction is active"
    // - "stream not found: generation mismatch"

    /// Test that simulates the upload_path_new_chunked flow where multiple
    /// concurrent chunk uploads each start their own transaction.
    ///
    /// This reproduces: "SQLite error: cannot commit - no transaction is active"
    #[tokio::test]
    async fn test_concurrent_chunk_upload_transactions() {
        let (conn, _temp_dir) = create_temp_db().await;

        conn.execute(
            "CREATE TABLE chunks (id INTEGER PRIMARY KEY, hash TEXT, state TEXT)",
            (),
        )
        .await
        .expect("Create table failed");

        // Insert some "pending" chunks
        for i in 0..5 {
            conn.execute(
                "INSERT INTO chunks (hash, state) VALUES (?1, 'pending')",
                [format!("hash{}", i)],
            )
            .await
            .expect("Insert failed");
        }

        // Simulate concurrent "upload_chunk" operations that each start a transaction
        let handles: Vec<_> = (1..=5)
            .map(|id| {
                let c = conn.clone();
                tokio::spawn(async move {
                    // Each "upload_chunk" starts its own transaction
                    let txn = c.begin_transaction().await?;

                    // Update the chunk state
                    c.execute(
                        "UPDATE chunks SET state = 'valid' WHERE id = ?1",
                        [id as i64],
                    )
                    .await?;

                    // Commit
                    txn.commit().await?;

                    Ok::<_, anyhow::Error>(id)
                })
            })
            .collect();

        // All should succeed, serialized by the mutex
        for handle in handles {
            let result = handle.await.expect("Task panicked");
            assert!(result.is_ok(), "Transaction failed: {:?}", result.err());
        }

        // Verify all chunks are valid
        let mut rows = conn
            .query("SELECT COUNT(*) FROM chunks WHERE state = 'valid'", ())
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let count: i64 = row.get(0).expect("Get failed");
        assert_eq!(count, 5);
    }

    /// Test that a failed query inside a transaction doesn't leave the transaction
    /// in an inconsistent state that causes "cannot commit" errors.
    #[tokio::test]
    async fn test_transaction_recovery_after_query_failure() {
        let (conn, _temp_dir) = create_temp_db().await;

        conn.execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .await
        .expect("Create table failed");

        // Start a transaction
        let txn = conn.begin_transaction().await.expect("Begin failed");

        // Execute a successful query
        conn.execute("INSERT INTO test_table (value) VALUES ('first')", ())
            .await
            .expect("First insert should succeed");

        // Execute a failing query (NOT NULL constraint violation)
        let fail_result = conn
            .execute("INSERT INTO test_table (value) VALUES (NULL)", ())
            .await;
        assert!(
            fail_result.is_err(),
            "Should fail due to NOT NULL constraint"
        );

        // The transaction should still be active and committable
        // (In SQLite, a failed statement doesn't auto-rollback the transaction)
        let commit_result = txn.commit().await;
        assert!(
            commit_result.is_ok(),
            "Commit should succeed even after a failed query: {:?}",
            commit_result.err()
        );

        // The successful insert should be committed
        let mut rows = conn
            .query("SELECT COUNT(*) FROM test_table", ())
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let count: i64 = row.get(0).expect("Get failed");
        assert_eq!(count, 1, "The successful insert should be committed");
    }

    /// Test that dropping a TransactionGuard without committing properly
    /// rolls back and doesn't leave stale transaction state.
    #[tokio::test]
    async fn test_transaction_guard_drop_rollback() {
        let (conn, _temp_dir) = create_temp_db().await;

        conn.execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)",
            (),
        )
        .await
        .expect("Create table failed");

        conn.execute("INSERT INTO test_table (value) VALUES ('before')", ())
            .await
            .expect("Insert failed");

        // Start and drop a transaction without committing
        {
            let txn = conn.begin_transaction().await.expect("Begin failed");
            conn.execute("INSERT INTO test_table (value) VALUES ('in-txn')", ())
                .await
                .expect("Insert in txn failed");
            // txn is dropped here without commit - should trigger rollback
            drop(txn);
        }

        // Give the async rollback a moment to execute
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Should be able to start a new transaction
        let txn2 = conn.begin_transaction().await.expect("Second begin failed");
        conn.execute("INSERT INTO test_table (value) VALUES ('after')", ())
            .await
            .expect("Insert after failed");
        txn2.commit().await.expect("Second commit failed");

        // Only 'before' and 'after' should exist (in-txn was rolled back)
        let mut rows = conn
            .query("SELECT COUNT(*) FROM test_table", ())
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let count: i64 = row.get(0).expect("Get failed");
        assert_eq!(count, 2, "Only 2 rows should exist after rollback");
    }

    /// Test rapid sequential transactions to ensure proper cleanup between them.
    /// This can expose "cannot commit - no transaction is active" issues.
    #[tokio::test]
    async fn test_rapid_sequential_transactions() {
        let (conn, _temp_dir) = create_temp_db().await;

        conn.execute(
            "CREATE TABLE counter (id INTEGER PRIMARY KEY, count INTEGER)",
            (),
        )
        .await
        .expect("Create table failed");

        conn.execute("INSERT INTO counter (id, count) VALUES (1, 0)", ())
            .await
            .expect("Insert failed");

        // Rapidly start and commit many transactions
        for i in 0..50 {
            let txn = conn
                .begin_transaction()
                .await
                .expect(&format!("Begin {} failed", i));

            conn.execute("UPDATE counter SET count = count + 1 WHERE id = 1", ())
                .await
                .expect(&format!("Update {} failed", i));

            txn.commit().await.expect(&format!("Commit {} failed", i));
        }

        // Verify final count
        let mut rows = conn
            .query("SELECT count FROM counter WHERE id = 1", ())
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let count: i64 = row.get(0).expect("Get failed");
        assert_eq!(count, 50);
    }

    /// Test that simulates the upload_path flow with nested operations:
    /// 1. Multiple concurrent "upload_chunk" calls that each have their own transaction
    /// 2. A final transaction that wraps NAR update and object insert
    ///
    /// This is the exact pattern that causes issues in production.
    #[tokio::test]
    async fn test_upload_path_chunked_flow() {
        let (conn, _temp_dir) = create_temp_db().await;

        // Create schema
        conn.execute(
            "CREATE TABLE nar (id INTEGER PRIMARY KEY, state TEXT, holders_count INTEGER)",
            (),
        )
        .await
        .expect("Create nar table failed");

        conn.execute(
            "CREATE TABLE chunk (id INTEGER PRIMARY KEY, state TEXT, hash TEXT)",
            (),
        )
        .await
        .expect("Create chunk table failed");

        conn.execute(
            "CREATE TABLE chunkref (id INTEGER PRIMARY KEY, nar_id INTEGER, chunk_id INTEGER, seq INTEGER)",
            (),
        )
        .await
        .expect("Create chunkref table failed");

        conn.execute(
            "CREATE TABLE object (id INTEGER PRIMARY KEY, nar_id INTEGER, path TEXT)",
            (),
        )
        .await
        .expect("Create object table failed");

        // Step 1: Insert pending NAR (outside transaction)
        conn.execute(
            "INSERT INTO nar (state, holders_count) VALUES ('pending', 0)",
            (),
        )
        .await
        .expect("Insert NAR failed");

        let nar_id: i64 = {
            let mut rows = conn
                .query("SELECT id FROM nar WHERE state = 'pending'", ())
                .await
                .expect("Query failed");
            let row = rows.next().await.expect("Next failed").expect("No row");
            row.get(0).expect("Get failed")
        };

        // Step 2: Concurrent chunk uploads (each with its own transaction)
        let num_chunks = 5;
        let handles: Vec<_> = (0..num_chunks)
            .map(|seq| {
                let c = conn.clone();
                let nar = nar_id;
                tokio::spawn(async move {
                    // Insert chunk (outside transaction)
                    c.execute(
                        "INSERT INTO chunk (state, hash) VALUES ('pending', ?1)",
                        [format!("hash{}", seq)],
                    )
                    .await?;

                    let chunk_id: i64 = {
                        let mut rows = c
                            .query(
                                "SELECT id FROM chunk WHERE hash = ?1",
                                [format!("hash{}", seq)],
                            )
                            .await?;
                        let row = rows.next().await?.expect("No row");
                        row.get(0)?
                    };

                    // Begin transaction for chunk finalization
                    let txn = c.begin_transaction().await?;

                    // Update chunk state
                    c.execute("UPDATE chunk SET state = 'valid' WHERE id = ?1", [chunk_id])
                        .await?;

                    // Insert chunkref
                    c.execute(
                        "INSERT INTO chunkref (nar_id, chunk_id, seq) VALUES (?1, ?2, ?3)",
                        (nar, chunk_id, seq as i64),
                    )
                    .await?;

                    txn.commit().await?;

                    Ok::<_, anyhow::Error>(chunk_id)
                })
            })
            .collect();

        // Wait for all chunks to complete
        let chunk_ids: Vec<i64> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.expect("Task panicked").expect("Chunk upload failed"))
            .collect();

        assert_eq!(chunk_ids.len(), num_chunks);

        // Step 3: Final transaction to update NAR and create object
        let final_txn = conn.begin_transaction().await.expect("Final begin failed");

        conn.execute("UPDATE nar SET state = 'valid' WHERE id = ?1", [nar_id])
            .await
            .expect("Update NAR failed");

        conn.execute(
            "INSERT INTO object (nar_id, path) VALUES (?1, '/nix/store/test')",
            [nar_id],
        )
        .await
        .expect("Insert object failed");

        final_txn.commit().await.expect("Final commit failed");

        // Verify everything is correct
        let mut rows = conn
            .query("SELECT COUNT(*) FROM chunk WHERE state = 'valid'", ())
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let count: i64 = row.get(0).expect("Get failed");
        assert_eq!(count, num_chunks as i64);

        let mut rows = conn
            .query("SELECT COUNT(*) FROM chunkref WHERE nar_id = ?1", [nar_id])
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let count: i64 = row.get(0).expect("Get failed");
        assert_eq!(count, num_chunks as i64);
    }

    /// Test concurrent transactions with one that fails and rolls back.
    /// Ensures subsequent transactions aren't affected by the failed one.
    #[tokio::test]
    async fn test_concurrent_transactions_with_failure() {
        let (conn, _temp_dir) = create_temp_db().await;

        conn.execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT UNIQUE)",
            (),
        )
        .await
        .expect("Create table failed");

        // Insert an initial value that will cause a conflict
        conn.execute("INSERT INTO test_table (value) VALUES ('conflict')", ())
            .await
            .expect("Initial insert failed");

        let conn1 = conn.clone();
        let conn2 = conn.clone();

        // Task 1: Will succeed
        let handle1 = tokio::spawn(async move {
            let txn = conn1.begin_transaction().await?;
            conn1
                .execute("INSERT INTO test_table (value) VALUES ('success1')", ())
                .await?;
            txn.commit().await?;
            Ok::<_, anyhow::Error>("success1")
        });

        // Task 2: Will fail due to unique constraint
        let handle2 = tokio::spawn(async move {
            let txn = conn2.begin_transaction().await?;
            // This will fail
            let result = conn2
                .execute("INSERT INTO test_table (value) VALUES ('conflict')", ())
                .await;
            if result.is_err() {
                // Rollback on failure
                txn.rollback().await?;
                return Err(anyhow::anyhow!("Expected failure"));
            }
            txn.commit().await?;
            Ok::<_, anyhow::Error>("success2")
        });

        // Task 1 should succeed
        let result1 = handle1.await.expect("Task 1 panicked");
        assert!(result1.is_ok(), "Task 1 should succeed: {:?}", result1);

        // Task 2 should fail
        let result2 = handle2.await.expect("Task 2 panicked");
        assert!(result2.is_err(), "Task 2 should fail");

        // A subsequent transaction should work fine
        let txn = conn.begin_transaction().await.expect("Begin failed");
        conn.execute(
            "INSERT INTO test_table (value) VALUES ('after_failure')",
            (),
        )
        .await
        .expect("Insert after failure should work");
        txn.commit()
            .await
            .expect("Commit after failure should work");

        // Verify final state
        let mut rows = conn
            .query("SELECT COUNT(*) FROM test_table", ())
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let count: i64 = row.get(0).expect("Get failed");
        assert_eq!(count, 3); // conflict, success1, after_failure
    }

    /// Test that demonstrates the "nested transaction" scenario that can cause issues.
    /// In the actual code, this happens when:
    /// 1. upload_path_dedup/upload_path_new starts a transaction
    /// 2. A query inside calls another function that also starts a transaction
    #[tokio::test]
    async fn test_nested_transaction_attempt() {
        let (conn, _temp_dir) = create_temp_db().await;

        conn.execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)",
            (),
        )
        .await
        .expect("Create table failed");

        // Start first transaction
        let txn1 = conn.begin_transaction().await.expect("Begin 1 failed");

        // Try to start a second transaction - this should block due to mutex
        let conn_clone = conn.clone();
        let handle = tokio::spawn(async move {
            // This will wait for txn1 to release the mutex
            let result = tokio::time::timeout(
                tokio::time::Duration::from_millis(200),
                conn_clone.begin_transaction(),
            )
            .await;
            result
        });

        // The second transaction should timeout waiting for the mutex
        let result = handle.await.expect("Task panicked");
        assert!(
            result.is_err(),
            "Second transaction should timeout while first is active"
        );

        // First transaction should still work
        conn.execute("INSERT INTO test_table (value) VALUES ('from_txn1')", ())
            .await
            .expect("Insert in txn1 failed");
        txn1.commit().await.expect("Commit txn1 failed");

        // Now a new transaction should work
        let txn2 = conn.begin_transaction().await.expect("Begin 2 failed");
        conn.execute("INSERT INTO test_table (value) VALUES ('from_txn2')", ())
            .await
            .expect("Insert in txn2 failed");
        txn2.commit().await.expect("Commit txn2 failed");
    }

    /// Test that TransactionGuard::Drop properly holds the mutex until ROLLBACK completes.
    ///
    /// Previously there was a bug where:
    /// 1. The mutex guard was released immediately on drop
    /// 2. The ROLLBACK was spawned asynchronously
    /// 3. A new transaction could start before ROLLBACK finished
    /// 4. This caused "cannot start a transaction within a transaction" errors
    ///
    /// The fix ensures the mutex is passed to the spawned rollback task,
    /// so it's not released until after ROLLBACK completes.
    #[tokio::test]
    async fn test_transaction_guard_drop_race_condition() {
        let (conn, _temp_dir) = create_temp_db().await;

        conn.execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)",
            (),
        )
        .await
        .expect("Create table failed");

        // Start a transaction and drop it without committing
        // This simulates an error path where the guard goes out of scope
        {
            let _txn = conn.begin_transaction().await.expect("Begin failed");
            conn.execute(
                "INSERT INTO test_table (value) VALUES ('from_dropped_txn')",
                (),
            )
            .await
            .expect("Insert failed");
            // Dropped here - ROLLBACK is spawned, mutex is held by the spawned task
        }

        // Start another transaction - this should block until the ROLLBACK completes
        // With the fix, this works correctly because the mutex is held until ROLLBACK finishes
        let txn2 = conn
            .begin_transaction()
            .await
            .expect("Begin 2 should succeed after ROLLBACK completes");

        conn.execute("INSERT INTO test_table (value) VALUES ('from_txn2')", ())
            .await
            .expect("Insert in txn2 failed");

        txn2.commit().await.expect("Commit should succeed");

        // Verify only 'from_txn2' exists (the first transaction was rolled back)
        let mut rows = conn
            .query("SELECT COUNT(*) FROM test_table", ())
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let count: i64 = row.get(0).expect("Get failed");
        assert_eq!(
            count, 1,
            "Only txn2's insert should exist (txn1 was rolled back)"
        );

        let mut rows = conn
            .query("SELECT value FROM test_table", ())
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let value: String = row.get(0).expect("Get failed");
        assert_eq!(value, "from_txn2");
    }

    /// Test that demonstrates queries executing outside a transaction
    /// while another transaction is active (non-transactional queries interleaving).
    #[tokio::test]
    async fn test_concurrent_queries_with_active_transaction() {
        let (conn, _temp_dir) = create_temp_db().await;

        conn.execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT, counter INTEGER)",
            (),
        )
        .await
        .expect("Create table failed");

        conn.execute(
            "INSERT INTO test_table (value, counter) VALUES ('init', 0)",
            (),
        )
        .await
        .expect("Insert failed");

        // Start a transaction
        let txn = conn.begin_transaction().await.expect("Begin failed");

        conn.execute(
            "UPDATE test_table SET counter = counter + 1 WHERE id = 1",
            (),
        )
        .await
        .expect("Update in txn failed");

        // While the transaction is active, spawn tasks that execute queries
        // (not transactions, just regular queries)
        let conn_clone = conn.clone();
        let handle = tokio::spawn(async move {
            // This query executes while txn is active
            // It should see the uncommitted changes (within the same connection)
            conn_clone
                .execute(
                    "UPDATE test_table SET counter = counter + 10 WHERE id = 1",
                    (),
                )
                .await
        });

        // Wait for the spawned query
        let result = handle.await.expect("Task panicked");
        assert!(result.is_ok(), "Query should succeed: {:?}", result.err());

        // Commit the original transaction
        txn.commit().await.expect("Commit failed");

        // Verify the final counter value
        // Should be 11 (1 from txn + 10 from concurrent query)
        let mut rows = conn
            .query("SELECT counter FROM test_table WHERE id = 1", ())
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let counter: i64 = row.get(0).expect("Get failed");
        assert_eq!(counter, 11);
    }

    /// Test the exact scenario from upload_chunk where the transaction guard
    /// might be dropped during error handling.
    #[tokio::test]
    async fn test_transaction_guard_early_return() {
        let (conn, _temp_dir) = create_temp_db().await;

        conn.execute(
            "CREATE TABLE chunks (id INTEGER PRIMARY KEY, state TEXT, file_size INTEGER)",
            (),
        )
        .await
        .expect("Create table failed");

        // Insert a chunk
        conn.execute(
            "INSERT INTO chunks (state, file_size) VALUES ('pending', NULL)",
            (),
        )
        .await
        .expect("Insert failed");

        // Simulate upload_chunk's transaction handling
        async fn simulate_upload_chunk(
            conn: &Arc<TursoConnection>,
            should_fail: bool,
        ) -> anyhow::Result<()> {
            let txn = conn.begin_transaction().await?;

            let result = async {
                // Update chunk
                conn.execute(
                    "UPDATE chunks SET state = 'valid', file_size = 100 WHERE id = 1",
                    (),
                )
                .await?;

                if should_fail {
                    return Err(anyhow::anyhow!("Simulated failure"));
                }

                Ok::<_, anyhow::Error>(())
            }
            .await;

            match result {
                Ok(()) => {
                    txn.commit().await?;
                }
                Err(e) => {
                    let _ = txn.rollback().await;
                    return Err(e);
                }
            }

            Ok(())
        }

        // Test failure case - should rollback
        conn.execute(
            "UPDATE chunks SET state = 'pending', file_size = NULL WHERE id = 1",
            (),
        )
        .await
        .expect("Reset failed");

        let result = simulate_upload_chunk(&conn, true).await;
        assert!(result.is_err());

        // Give rollback time to execute
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Chunk should still be pending (rollback worked)
        let mut rows = conn
            .query("SELECT state FROM chunks WHERE id = 1", ())
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let state: String = row.get(0).expect("Get failed");
        assert_eq!(state, "pending", "Rollback should have reverted the state");

        // Test success case
        let result = simulate_upload_chunk(&conn, false).await;
        assert!(result.is_ok());

        // Chunk should be valid now
        let mut rows = conn
            .query("SELECT state FROM chunks WHERE id = 1", ())
            .await
            .expect("Query failed");
        let row = rows.next().await.expect("Next failed").expect("No row");
        let state: String = row.get(0).expect("Get failed");
        assert_eq!(state, "valid");
    }
}
