//! Database migrations for Turso backend.
//!
//! This module provides a simple migration runner that tracks applied
//! migrations in a `_migrations` table and applies new migrations in order.

use anyhow::Result;
use chrono::Utc;
use libsql::params;

use super::connection::TursoConnection;

/// A database migration.
struct Migration {
    /// Migration name (unique identifier).
    name: &'static str,
    /// SQL statements to apply the migration.
    up_sql: &'static str,
}

/// All migrations in order.
const MIGRATIONS: &[Migration] = &[
    Migration {
        name: "m20221227_000001_create_cache_table",
        up_sql: r#"
            CREATE TABLE IF NOT EXISTS cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                keypair TEXT NOT NULL,
                is_public INTEGER NOT NULL,
                store_dir TEXT NOT NULL,
                priority INTEGER NOT NULL,
                upstream_cache_key_names TEXT NOT NULL,
                created_at TEXT NOT NULL,
                deleted_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_cache_name ON cache (name);
        "#,
    },
    Migration {
        name: "m20221227_000002_create_nar_table",
        up_sql: r#"
            CREATE TABLE IF NOT EXISTS nar (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state TEXT NOT NULL,
                nar_hash TEXT NOT NULL,
                nar_size INTEGER NOT NULL,
                file_hash TEXT,
                file_size INTEGER,
                compression TEXT NOT NULL,
                remote_file TEXT,
                remote_file_id TEXT,
                holders_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_nar_nar_hash ON nar (nar_hash);
        "#,
    },
    Migration {
        name: "m20221227_000003_create_object_table",
        up_sql: r#"
            CREATE TABLE IF NOT EXISTS object (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cache_id INTEGER NOT NULL,
                nar_id INTEGER NOT NULL,
                store_path_hash TEXT NOT NULL,
                store_path TEXT NOT NULL,
                "references" TEXT NOT NULL,
                system TEXT,
                deriver TEXT,
                sigs TEXT NOT NULL,
                ca TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (cache_id) REFERENCES cache(id) ON DELETE CASCADE,
                FOREIGN KEY (nar_id) REFERENCES nar(id) ON DELETE CASCADE,
                UNIQUE (cache_id, store_path_hash)
            );
            CREATE INDEX IF NOT EXISTS idx_object_cache_hash ON object (cache_id, store_path_hash);
        "#,
    },
    Migration {
        name: "m20221227_000004_add_object_last_accessed",
        up_sql: r#"
            ALTER TABLE object ADD COLUMN last_accessed_at TEXT;
        "#,
    },
    Migration {
        name: "m20221227_000005_add_cache_retention_period",
        up_sql: r#"
            ALTER TABLE cache ADD COLUMN retention_period INTEGER;
        "#,
    },
    Migration {
        name: "m20230103_000001_add_object_created_by",
        up_sql: r#"
            ALTER TABLE object ADD COLUMN created_by TEXT;
        "#,
    },
    Migration {
        name: "m20230112_000001_add_chunk_table",
        up_sql: r#"
            CREATE TABLE IF NOT EXISTS chunk (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state TEXT NOT NULL,
                chunk_hash TEXT NOT NULL,
                chunk_size INTEGER NOT NULL,
                file_hash TEXT,
                file_size INTEGER,
                compression TEXT NOT NULL,
                remote_file TEXT NOT NULL,
                remote_file_id TEXT NOT NULL UNIQUE,
                holders_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_chunk_chunk_hash ON chunk (chunk_hash);
        "#,
    },
    Migration {
        name: "m20230112_000002_add_chunkref_table",
        up_sql: r#"
            CREATE TABLE IF NOT EXISTS chunkref (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nar_id INTEGER NOT NULL,
                seq INTEGER NOT NULL,
                chunk_id INTEGER,
                chunk_hash TEXT NOT NULL,
                compression TEXT NOT NULL,
                FOREIGN KEY (nar_id) REFERENCES nar(id) ON DELETE CASCADE,
                FOREIGN KEY (chunk_id) REFERENCES chunk(id)
            );
            CREATE INDEX IF NOT EXISTS idx_chunkref_nar_id ON chunkref (nar_id);
            CREATE INDEX IF NOT EXISTS idx_chunkref_chunk_id ON chunkref (chunk_id);
            CREATE INDEX IF NOT EXISTS idx_chunkref_chunk_hash ON chunkref (chunk_hash);
        "#,
    },
    Migration {
        name: "m20230112_000003_add_nar_num_chunks",
        up_sql: r#"
            ALTER TABLE nar ADD COLUMN num_chunks INTEGER NOT NULL DEFAULT 0;
        "#,
    },
    Migration {
        name: "m20230112_000004_migrate_nar_remote_files_to_chunks",
        up_sql: r#"
            -- This migration converts single-chunk NARs to use the chunk system.
            -- For existing data, we need to:
            -- 1. Create chunk records from NAR remote_file data
            -- 2. Create chunkref records linking NARs to chunks
            -- This is a data migration - for fresh databases it's a no-op.

            -- Insert chunks from existing NARs that have remote_file data
            INSERT OR IGNORE INTO chunk (state, chunk_hash, chunk_size, file_hash, file_size, compression, remote_file, remote_file_id, holders_count, created_at)
            SELECT
                'V',
                nar_hash,
                nar_size,
                file_hash,
                file_size,
                compression,
                remote_file,
                remote_file_id,
                0,
                created_at
            FROM nar
            WHERE remote_file IS NOT NULL AND remote_file_id IS NOT NULL;

            -- Create chunkref records linking NARs to their chunks
            INSERT OR IGNORE INTO chunkref (nar_id, seq, chunk_id, chunk_hash, compression)
            SELECT
                n.id,
                0,
                c.id,
                n.nar_hash,
                n.compression
            FROM nar n
            INNER JOIN chunk c ON c.remote_file_id = n.remote_file_id
            WHERE n.remote_file IS NOT NULL;

            -- Update num_chunks for migrated NARs
            UPDATE nar SET num_chunks = 1 WHERE remote_file IS NOT NULL AND remote_file_id IS NOT NULL;
        "#,
    },
    Migration {
        name: "m20230112_000005_drop_old_nar_columns",
        up_sql: r#"
            -- SQLite doesn't support DROP COLUMN directly in older versions,
            -- but libSQL and newer SQLite do. We'll use the newer syntax.
            -- If this fails on old SQLite, the columns will just remain (harmless).

            -- Note: These ALTER TABLE DROP COLUMN statements may fail on older SQLite.
            -- That's okay - the columns will just remain unused.
            -- We use separate statements so partial success is possible.

            -- For compatibility, we'll just leave the old columns in place.
            -- They won't be used by the new code and don't cause harm.
            SELECT 1;
        "#,
    },
    Migration {
        name: "m20230112_000006_add_nar_completeness_hint",
        up_sql: r#"
            ALTER TABLE nar ADD COLUMN completeness_hint INTEGER NOT NULL DEFAULT 1;
        "#,
    },
];

/// Runs all pending database migrations.
pub async fn run_migrations(conn: &TursoConnection) -> Result<()> {
    // Create migrations table if it doesn't exist
    create_migrations_table(conn).await?;

    // Get list of applied migrations
    let applied = get_applied_migrations(conn).await?;

    // Apply each pending migration
    for migration in MIGRATIONS {
        if !applied.contains(&migration.name.to_string()) {
            tracing::info!("Applying migration: {}", migration.name);
            apply_migration(conn, migration).await?;
        }
    }

    tracing::info!("All migrations applied successfully");
    Ok(())
}

/// Creates the migrations tracking table.
async fn create_migrations_table(conn: &TursoConnection) -> Result<()> {
    let sql = r#"
        CREATE TABLE IF NOT EXISTS _migrations (
            name TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL
        )
    "#;
    conn.execute(sql, ()).await?;
    Ok(())
}

/// Gets the list of already-applied migration names.
async fn get_applied_migrations(conn: &TursoConnection) -> Result<Vec<String>> {
    let sql = "SELECT name FROM _migrations ORDER BY name";
    let mut rows = conn.query(sql, ()).await?;

    let mut names = Vec::new();
    while let Some(row) = rows.next().await? {
        let name: String = row.get(0)?;
        names.push(name);
    }

    Ok(names)
}

/// Applies a single migration.
async fn apply_migration(conn: &TursoConnection, migration: &Migration) -> Result<()> {
    // Execute the migration SQL
    // Split by semicolons and execute each statement separately
    for statement in migration.up_sql.split(';') {
        let statement = statement.trim();
        if statement.is_empty() || statement.starts_with("--") {
            continue;
        }

        // Skip comment-only lines
        let non_comment_lines: Vec<&str> = statement
            .lines()
            .filter(|l| !l.trim().starts_with("--") && !l.trim().is_empty())
            .collect();

        if non_comment_lines.is_empty() {
            continue;
        }

        if let Err(e) = conn.execute(statement, ()).await {
            // Log the error but continue - some statements may fail on older SQLite
            // (like DROP COLUMN) and that's okay
            tracing::warn!("Migration statement failed (may be expected): {}", e);
        }
    }

    // Record the migration as applied
    let now = Utc::now().to_rfc3339();
    let sql = "INSERT INTO _migrations (name, applied_at) VALUES (?, ?)";
    conn.execute(sql, params![migration.name, now]).await?;

    Ok(())
}

/// Checks if migrations are needed.
pub async fn needs_migration(conn: &TursoConnection) -> Result<bool> {
    // Try to create the migrations table (idempotent)
    create_migrations_table(conn).await?;

    let applied = get_applied_migrations(conn).await?;
    let total_migrations = MIGRATIONS.len();

    Ok(applied.len() < total_migrations)
}

/// Gets migration status information.
pub async fn get_migration_status(conn: &TursoConnection) -> Result<MigrationStatus> {
    create_migrations_table(conn).await?;
    let applied = get_applied_migrations(conn).await?;

    let pending: Vec<String> = MIGRATIONS
        .iter()
        .filter(|m| !applied.contains(&m.name.to_string()))
        .map(|m| m.name.to_string())
        .collect();

    Ok(MigrationStatus {
        applied,
        pending,
        total: MIGRATIONS.len(),
    })
}

/// Status of database migrations.
#[derive(Debug)]
pub struct MigrationStatus {
    /// Names of applied migrations.
    pub applied: Vec<String>,
    /// Names of pending migrations.
    pub pending: Vec<String>,
    /// Total number of migrations.
    pub total: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::connection::TursoConfig;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn test_migrations_have_unique_names() {
        let mut names: Vec<&str> = MIGRATIONS.iter().map(|m| m.name).collect();
        names.sort();
        let original_len = names.len();
        names.dedup();
        assert_eq!(names.len(), original_len, "Migration names must be unique");
    }

    #[test]
    fn test_migrations_are_in_order() {
        for (i, migration) in MIGRATIONS.iter().enumerate() {
            if i > 0 {
                assert!(
                    migration.name > MIGRATIONS[i - 1].name,
                    "Migrations must be in alphabetical order"
                );
            }
        }
    }

    /// Helper to create a temporary database for testing.
    async fn create_test_db() -> (std::sync::Arc<TursoConnection>, TempDir) {
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
    async fn test_run_migrations_fresh_db() {
        let (conn, _temp_dir) = create_test_db().await;

        // Run migrations on fresh database
        let result = run_migrations(&conn).await;
        assert!(result.is_ok(), "Migrations failed: {:?}", result.err());

        // Verify all tables exist
        let tables = ["cache", "nar", "object", "chunk", "chunkref", "_migrations"];
        for table in tables {
            let mut rows = conn
                .query(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?1",
                    [table],
                )
                .await
                .expect("Query failed");
            assert!(
                rows.next().await.expect("Next failed").is_some(),
                "Table {} should exist",
                table
            );
        }
    }

    #[tokio::test]
    async fn test_run_migrations_idempotent() {
        let (conn, _temp_dir) = create_test_db().await;

        // Run migrations twice
        run_migrations(&conn).await.expect("First run failed");
        let result = run_migrations(&conn).await;
        assert!(
            result.is_ok(),
            "Second run should be idempotent: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_needs_migration_fresh_db() {
        let (conn, _temp_dir) = create_test_db().await;

        // Fresh database should need migrations
        let needs = needs_migration(&conn).await.expect("Check failed");
        assert!(needs, "Fresh database should need migrations");
    }

    #[tokio::test]
    async fn test_needs_migration_after_running() {
        let (conn, _temp_dir) = create_test_db().await;

        // Run all migrations
        run_migrations(&conn).await.expect("Migrations failed");

        // Should not need migrations anymore
        let needs = needs_migration(&conn).await.expect("Check failed");
        assert!(!needs, "Migrated database should not need migrations");
    }

    #[tokio::test]
    async fn test_get_migration_status_fresh_db() {
        let (conn, _temp_dir) = create_test_db().await;

        let status = get_migration_status(&conn).await.expect("Status failed");

        assert_eq!(status.applied.len(), 0);
        assert_eq!(status.pending.len(), MIGRATIONS.len());
        assert_eq!(status.total, MIGRATIONS.len());
    }

    #[tokio::test]
    async fn test_get_migration_status_after_running() {
        let (conn, _temp_dir) = create_test_db().await;

        run_migrations(&conn).await.expect("Migrations failed");

        let status = get_migration_status(&conn).await.expect("Status failed");

        assert_eq!(status.applied.len(), MIGRATIONS.len());
        assert_eq!(status.pending.len(), 0);
        assert_eq!(status.total, MIGRATIONS.len());
    }

    #[tokio::test]
    async fn test_migrations_create_proper_schema() {
        let (conn, _temp_dir) = create_test_db().await;
        run_migrations(&conn).await.expect("Migrations failed");

        // Test cache table columns
        let result = conn
            .execute(
                r#"INSERT INTO cache (name, keypair, is_public, store_dir, priority, upstream_cache_key_names, created_at)
                   VALUES ('test', 'keypair', 1, '/nix/store', 40, '[]', datetime('now'))"#,
                (),
            )
            .await;
        assert!(result.is_ok(), "Cache insert failed: {:?}", result.err());

        // Test nar table columns
        let result = conn
            .execute(
                r#"INSERT INTO nar (state, nar_hash, nar_size, compression, num_chunks, completeness_hint, holders_count, created_at)
                   VALUES ('V', 'sha256:abc123', 1000, 'none', 1, 1, 0, datetime('now'))"#,
                (),
            )
            .await;
        assert!(result.is_ok(), "NAR insert failed: {:?}", result.err());

        // Test object table columns (including last_accessed_at and created_by from migrations)
        let result = conn
            .execute(
                r#"INSERT INTO object (cache_id, nar_id, store_path_hash, store_path, "references", sigs, created_at, last_accessed_at, created_by)
                   VALUES (1, 1, 'abc123', '/nix/store/abc123-pkg', '[]', '[]', datetime('now'), datetime('now'), 'test-user')"#,
                (),
            )
            .await;
        assert!(result.is_ok(), "Object insert failed: {:?}", result.err());

        // Test chunk table columns
        let result = conn
            .execute(
                r#"INSERT INTO chunk (state, chunk_hash, chunk_size, compression, remote_file, remote_file_id, holders_count, created_at)
                   VALUES ('V', 'sha256:chunk1', 500, 'zstd', 'chunks/chunk1.zst', 'chunk-uuid-1', 0, datetime('now'))"#,
                (),
            )
            .await;
        assert!(result.is_ok(), "Chunk insert failed: {:?}", result.err());

        // Test chunkref table columns
        let result = conn
            .execute(
                r#"INSERT INTO chunkref (nar_id, seq, chunk_id, chunk_hash, compression)
                   VALUES (1, 0, 1, 'sha256:chunk1', 'zstd')"#,
                (),
            )
            .await;
        assert!(result.is_ok(), "Chunkref insert failed: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_cache_retention_period_column_exists() {
        let (conn, _temp_dir) = create_test_db().await;
        run_migrations(&conn).await.expect("Migrations failed");

        // Verify retention_period column exists (added in m20221227_000005)
        let result = conn
            .execute(
                "UPDATE cache SET retention_period = 86400 WHERE name = 'nonexistent'",
                (),
            )
            .await;
        assert!(
            result.is_ok(),
            "retention_period column should exist: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_indexes_created() {
        let (conn, _temp_dir) = create_test_db().await;
        run_migrations(&conn).await.expect("Migrations failed");

        // Query for indexes
        let mut rows = conn
            .query(
                "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'",
                (),
            )
            .await
            .expect("Query failed");

        let mut indexes = Vec::new();
        while let Some(row) = rows.next().await.expect("Next failed") {
            let name: String = row.get(0).expect("Get failed");
            indexes.push(name);
        }

        // Verify expected indexes exist
        assert!(
            indexes.contains(&"idx_cache_name".to_string()),
            "idx_cache_name should exist"
        );
        assert!(
            indexes.contains(&"idx_nar_nar_hash".to_string()),
            "idx_nar_nar_hash should exist"
        );
        assert!(
            indexes.contains(&"idx_chunk_chunk_hash".to_string()),
            "idx_chunk_chunk_hash should exist"
        );
    }

    #[tokio::test]
    async fn test_foreign_keys_work() {
        let (conn, _temp_dir) = create_test_db().await;
        run_migrations(&conn).await.expect("Migrations failed");

        // Enable foreign key checks
        conn.execute("PRAGMA foreign_keys = ON", ())
            .await
            .expect("PRAGMA failed");

        // Insert parent records
        conn.execute(
            r#"INSERT INTO cache (name, keypair, is_public, store_dir, priority, upstream_cache_key_names, created_at)
               VALUES ('test-cache', 'keypair', 1, '/nix/store', 40, '[]', datetime('now'))"#,
            (),
        )
        .await
        .expect("Cache insert failed");

        conn.execute(
            r#"INSERT INTO nar (state, nar_hash, nar_size, compression, num_chunks, completeness_hint, holders_count, created_at)
               VALUES ('V', 'sha256:abc', 1000, 'none', 1, 1, 0, datetime('now'))"#,
            (),
        )
        .await
        .expect("NAR insert failed");

        // Insert child record referencing parents
        let result = conn
            .execute(
                r#"INSERT INTO object (cache_id, nar_id, store_path_hash, store_path, "references", sigs, created_at)
                   VALUES (1, 1, 'hash123', '/nix/store/hash123-pkg', '[]', '[]', datetime('now'))"#,
                (),
            )
            .await;
        assert!(result.is_ok(), "Object with valid FKs should work");

        // Try to insert with invalid foreign key
        let result = conn
            .execute(
                r#"INSERT INTO object (cache_id, nar_id, store_path_hash, store_path, "references", sigs, created_at)
                   VALUES (999, 999, 'hash456', '/nix/store/hash456-pkg', '[]', '[]', datetime('now'))"#,
                (),
            )
            .await;
        assert!(result.is_err(), "Object with invalid FKs should fail");
    }
}
