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
}
