//! Migrate data from SQLite to Turso.
//!
//! This command copies all data from a local SQLite database to a Turso database.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use clap::Parser;

use crate::Opts;
use attic_server::config::Config;
use attic_server::database_turso::connection::{TursoConfig, TursoConnection};
use attic_server::database_turso::migrations;

/// Migrate data from SQLite to Turso.
///
/// This command copies all data from a local SQLite database to a Turso database.
/// It will run migrations on the Turso database first, then copy data table by table.
///
/// Example:
/// $ atticadm migrate-to-turso \
///     --source /data/attic.db \
///     --turso-url libsql://xxx.turso.io \
///     --auth-token $TURSO_AUTH_TOKEN
#[derive(Debug, Parser)]
pub struct MigrateToTurso {
    /// Path to the source SQLite database.
    #[clap(long)]
    source: PathBuf,

    /// Turso database URL (libsql://xxx.turso.io).
    #[clap(long)]
    turso_url: String,

    /// Authentication token for Turso.
    #[clap(long)]
    auth_token: String,

    /// Batch size for copying data (default: 100).
    #[clap(long, default_value = "100")]
    batch_size: usize,

    /// Continue from a specific table (for resuming interrupted migrations).
    #[clap(long)]
    resume_from: Option<String>,

    /// Skip running migrations (useful if already run).
    #[clap(long)]
    skip_migrations: bool,

    /// Dry run - show what would be done without actually copying data.
    #[clap(long)]
    dry_run: bool,
}

/// Tables in order of dependency (referential integrity).
const TABLES: &[(&str, &[&str])] = &[
    ("cache", &["id", "name", "keypair", "is_public", "store_dir", "priority", "upstream_cache_key_names", "created_at", "deleted_at", "retention_period"]),
    ("nar", &["id", "state", "nar_hash", "nar_size", "compression", "num_chunks", "completeness_hint", "holders_count", "created_at"]),
    ("chunk", &["id", "state", "chunk_hash", "chunk_size", "file_hash", "file_size", "compression", "remote_file", "remote_file_id", "holders_count", "created_at"]),
    ("object", &["id", "cache_id", "nar_id", "store_path_hash", "store_path", "references", "system", "deriver", "sigs", "ca", "created_at", "last_accessed_at", "created_by"]),
    ("chunkref", &["id", "nar_id", "seq", "chunk_id", "chunk_hash", "compression"]),
];

pub async fn run(_config: Config, opts: Opts) -> Result<()> {
    let sub = opts.command.as_migrate_to_turso().unwrap();

    eprintln!("Attic Data Migration: SQLite -> Turso");
    eprintln!("======================================");
    eprintln!();

    // Validate source exists
    if !sub.source.exists() {
        return Err(anyhow!("Source database not found: {:?}", sub.source));
    }

    eprintln!("Source: {:?}", sub.source);
    eprintln!("Target: {}", sub.turso_url);
    eprintln!("Batch size: {}", sub.batch_size);
    if let Some(ref table) = sub.resume_from {
        eprintln!("Resuming from table: {}", table);
    }
    eprintln!();

    if sub.dry_run {
        eprintln!("[DRY RUN - No data will be modified]");
        eprintln!();
    }

    // Connect to source SQLite
    eprintln!("Connecting to source SQLite database...");
    let source_config = TursoConfig {
        url: format!("sqlite://{}", sub.source.display()),
        auth_token: None,
        local_replica_path: None,
        sync_interval: std::time::Duration::from_secs(60),
    };
    let source = TursoConnection::connect(source_config).await?;
    eprintln!("  Connected.");

    // Connect to Turso
    eprintln!("Connecting to Turso database...");
    let turso_config = TursoConfig {
        url: sub.turso_url.clone(),
        auth_token: Some(sub.auth_token.clone()),
        local_replica_path: None, // Don't use embedded replica during migration
        sync_interval: std::time::Duration::from_secs(60),
    };
    let turso = TursoConnection::connect(turso_config).await?;
    eprintln!("  Connected.");

    // Run migrations on Turso
    if !sub.skip_migrations && !sub.dry_run {
        eprintln!();
        eprintln!("Running migrations on Turso...");
        migrations::run_migrations(&turso).await?;
        eprintln!("  Migrations complete.");
    }

    eprintln!();
    eprintln!("Starting data migration...");
    eprintln!();

    // Find starting table
    let start_idx = if let Some(ref resume_table) = sub.resume_from {
        TABLES.iter()
            .position(|(name, _)| *name == resume_table)
            .ok_or_else(|| anyhow!("Unknown table: {}", resume_table))?
    } else {
        0
    };

    // Migrate each table
    for (table_name, columns) in &TABLES[start_idx..] {
        if sub.dry_run {
            let count = count_rows(&source, table_name).await?;
            eprintln!("  [DRY RUN] Would copy {} rows from table '{}'", count, table_name);
        } else {
            migrate_table(&source, &turso, table_name, columns, sub.batch_size).await?;
        }
    }

    eprintln!();
    eprintln!("Migration complete!");

    if !sub.dry_run {
        eprintln!();
        eprintln!("Verification:");
        for (table_name, _) in TABLES {
            let source_count = count_rows(&source, table_name).await?;
            let turso_count = count_rows(&turso, table_name).await?;
            let status = if source_count == turso_count { "✓" } else { "✗" };
            eprintln!("  {} {}: {} -> {}", status, table_name, source_count, turso_count);
        }
    }

    Ok(())
}

async fn count_rows(conn: &Arc<TursoConnection>, table: &str) -> Result<i64> {
    let sql = format!("SELECT COUNT(*) FROM {}", table);
    let mut rows = conn.query(&sql, ()).await?;
    match rows.next().await? {
        Some(row) => Ok(row.get::<i64>(0)?),
        None => Ok(0),
    }
}

async fn migrate_table(
    source: &Arc<TursoConnection>,
    turso: &Arc<TursoConnection>,
    table_name: &str,
    columns: &[&str],
    batch_size: usize,
) -> Result<()> {
    let total = count_rows(source, table_name).await?;
    eprintln!("Migrating table '{}' ({} rows)...", table_name, total);

    if total == 0 {
        eprintln!("  Skipped (empty table).");
        return Ok(());
    }

    let column_list = columns.join(", ");
    let placeholders: Vec<_> = (1..=columns.len())
        .map(|i| format!("?{}", i))
        .collect();
    let placeholder_list = placeholders.join(", ");

    let select_sql = format!(
        "SELECT {} FROM {} ORDER BY id LIMIT {} OFFSET ?",
        column_list, table_name, batch_size
    );

    let insert_sql = format!(
        "INSERT OR REPLACE INTO {} ({}) VALUES ({})",
        table_name, column_list, placeholder_list
    );

    let mut offset = 0i64;
    let mut migrated = 0i64;

    loop {
        // Fetch batch from source
        let mut rows = source.query(&select_sql, [offset]).await?;
        let mut batch_count = 0;

        while let Some(row) = rows.next().await? {
            // Build dynamic INSERT for this row
            // We use raw SQL since we don't know the types at compile time
            let values = build_insert_values(&row, columns.len())?;

            // Execute the insert - for simplicity, we insert one at a time
            // (libsql doesn't support batch insert with different values easily)
            turso.execute(&insert_sql, values).await
                .map_err(|e| anyhow!("Failed to insert into {}: {}", table_name, e))?;

            batch_count += 1;
            migrated += 1;
        }

        if batch_count == 0 {
            break;
        }

        eprintln!("  Progress: {}/{} ({:.1}%)", migrated, total, (migrated as f64 / total as f64) * 100.0);
        offset += batch_size as i64;
    }

    eprintln!("  Completed: {} rows migrated.", migrated);
    Ok(())
}

/// Build parameter values from a row for INSERT.
/// Returns a Vec that can be converted to params.
fn build_insert_values(row: &libsql::Row, num_columns: usize) -> Result<Vec<libsql::Value>> {
    let mut values = Vec::with_capacity(num_columns);
    for i in 0..num_columns {
        // Try to get the value as various types
        // libsql::Row doesn't have a "get any type" method, so we try each type
        let value = if let Ok(v) = row.get::<Option<i64>>(i as i32) {
            match v {
                Some(n) => libsql::Value::Integer(n),
                None => libsql::Value::Null,
            }
        } else if let Ok(v) = row.get::<Option<f64>>(i as i32) {
            match v {
                Some(n) => libsql::Value::Real(n),
                None => libsql::Value::Null,
            }
        } else if let Ok(v) = row.get::<Option<String>>(i as i32) {
            match v {
                Some(s) => libsql::Value::Text(s),
                None => libsql::Value::Null,
            }
        } else if let Ok(v) = row.get::<Option<Vec<u8>>>(i as i32) {
            match v {
                Some(b) => libsql::Value::Blob(b),
                None => libsql::Value::Null,
            }
        } else {
            libsql::Value::Null
        };
        values.push(value);
    }
    Ok(values)
}
