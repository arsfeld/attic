//! Model definitions for Turso database backend.
//!
//! These models are designed to work without SeaORM derives, using manual
//! parsing from libsql::Row values.

use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use libsql::Row;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::error::{ServerError, ServerResult};
use crate::narinfo::{Compression, NarInfo};
use crate::storage::RemoteFile;
use attic::error::AtticResult;
use attic::hash::Hash;
use attic::signing::NixKeypair;

/// A value stored as JSON in the database.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Json<T>(pub T);

impl<T: Serialize + DeserializeOwned> Json<T> {
    pub fn from_str(s: &str) -> Result<Self> {
        Ok(Json(serde_json::from_str(s)?))
    }

    pub fn to_string(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.0)?)
    }
}

/// The state of a NAR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NarState {
    /// The NAR can be used.
    Valid,
    /// The NAR is a pending upload.
    PendingUpload,
    /// The NAR can be deleted because it already exists.
    ConfirmedDeduplicated,
    /// The NAR is being deleted.
    Deleted,
}

impl NarState {
    pub fn from_db_value(s: &str) -> Result<Self> {
        match s {
            "V" => Ok(Self::Valid),
            "P" => Ok(Self::PendingUpload),
            "C" => Ok(Self::ConfirmedDeduplicated),
            "D" => Ok(Self::Deleted),
            _ => Err(anyhow!("Invalid NAR state: {}", s)),
        }
    }

    pub fn to_db_value(&self) -> &'static str {
        match self {
            Self::Valid => "V",
            Self::PendingUpload => "P",
            Self::ConfirmedDeduplicated => "C",
            Self::Deleted => "D",
        }
    }
}

/// The state of a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkState {
    /// The chunk can be used.
    Valid,
    /// The chunk is a pending upload.
    PendingUpload,
    /// The chunk can be deleted because it already exists.
    ConfirmedDeduplicated,
    /// The chunk is being deleted.
    Deleted,
}

impl ChunkState {
    pub fn from_db_value(s: &str) -> Result<Self> {
        match s {
            "V" => Ok(Self::Valid),
            "P" => Ok(Self::PendingUpload),
            "C" => Ok(Self::ConfirmedDeduplicated),
            "D" => Ok(Self::Deleted),
            _ => Err(anyhow!("Invalid chunk state: {}", s)),
        }
    }

    pub fn to_db_value(&self) -> &'static str {
        match self {
            Self::Valid => "V",
            Self::PendingUpload => "P",
            Self::ConfirmedDeduplicated => "C",
            Self::Deleted => "D",
        }
    }
}

/// A binary cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheModel {
    pub id: i64,
    pub name: String,
    pub keypair: String,
    pub is_public: bool,
    pub store_dir: String,
    pub priority: i32,
    pub upstream_cache_key_names: Json<Vec<String>>,
    pub created_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub retention_period: Option<i32>,
}

impl CacheModel {
    /// Parses a CacheModel from a database row.
    pub fn from_row(row: &Row) -> Result<Self> {
        Self::from_row_at(row, 0)
    }

    /// Parses a CacheModel from a row starting at the given index.
    pub fn from_row_at(row: &Row, start: i32) -> Result<Self> {
        Ok(Self {
            id: row.get::<i64>(start)?,
            name: row.get::<String>(start + 1)?,
            keypair: row.get::<String>(start + 2)?,
            is_public: row.get::<i64>(start + 3)? != 0,
            store_dir: row.get::<String>(start + 4)?,
            priority: row.get::<i64>(start + 5)? as i32,
            upstream_cache_key_names: Json::from_str(&row.get::<String>(start + 6)?)?,
            created_at: parse_datetime(&row.get::<String>(start + 7)?)?,
            deleted_at: row
                .get::<Option<String>>(start + 8)?
                .map(|s| parse_datetime(&s))
                .transpose()?,
            retention_period: row.get::<Option<i64>>(start + 9)?.map(|v| v as i32),
        })
    }

    /// Parses a CacheModel from a row with a column prefix (for joins).
    pub fn from_row_prefixed(row: &Row, _prefix: &str, start_idx: usize) -> Result<Self> {
        Self::from_row_at(row, start_idx as i32)
    }

    /// Returns the number of columns in this model.
    pub const fn column_count() -> usize {
        10
    }

    /// Returns the signing keypair for this cache.
    pub fn keypair(&self) -> AtticResult<NixKeypair> {
        NixKeypair::from_str(&self.keypair)
    }
}

/// A content-addressed NAR in the global cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NarModel {
    pub id: i64,
    pub state: NarState,
    pub nar_hash: String,
    pub nar_size: i64,
    pub compression: String,
    pub num_chunks: i32,
    pub completeness_hint: bool,
    pub holders_count: i32,
    pub created_at: DateTime<Utc>,
}

impl NarModel {
    /// Parses a NarModel from a database row.
    pub fn from_row(row: &Row) -> Result<Self> {
        Self::from_row_at(row, 0)
    }

    /// Parses a NarModel from a row starting at the given index.
    pub fn from_row_at(row: &Row, start: i32) -> Result<Self> {
        Ok(Self {
            id: row.get::<i64>(start)?,
            state: NarState::from_db_value(&row.get::<String>(start + 1)?)?,
            nar_hash: row.get::<String>(start + 2)?,
            nar_size: row.get::<i64>(start + 3)?,
            compression: row.get::<String>(start + 4)?,
            num_chunks: row.get::<i64>(start + 5)? as i32,
            completeness_hint: row.get::<i64>(start + 6)? != 0,
            holders_count: row.get::<i64>(start + 7)? as i32,
            created_at: parse_datetime(&row.get::<String>(start + 8)?)?,
        })
    }

    /// Parses a NarModel from a row with a column prefix (for joins).
    pub fn from_row_prefixed(row: &Row, _prefix: &str, start_idx: usize) -> Result<Self> {
        Self::from_row_at(row, start_idx as i32)
    }

    /// Returns the number of columns in this model.
    pub const fn column_count() -> usize {
        9
    }
}

/// An object in a binary cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectModel {
    pub id: i64,
    pub cache_id: i64,
    pub nar_id: i64,
    pub store_path_hash: String,
    pub store_path: String,
    pub references: Json<Vec<String>>,
    pub system: Option<String>,
    pub deriver: Option<String>,
    pub sigs: Json<Vec<String>>,
    pub ca: Option<String>,
    pub created_at: DateTime<Utc>,
    pub last_accessed_at: Option<DateTime<Utc>>,
    pub created_by: Option<String>,
}

impl ObjectModel {
    /// Parses an ObjectModel from a database row.
    pub fn from_row(row: &Row) -> Result<Self> {
        Self::from_row_at(row, 0)
    }

    /// Parses an ObjectModel from a row starting at the given index.
    pub fn from_row_at(row: &Row, start: i32) -> Result<Self> {
        Ok(Self {
            id: row.get::<i64>(start)?,
            cache_id: row.get::<i64>(start + 1)?,
            nar_id: row.get::<i64>(start + 2)?,
            store_path_hash: row.get::<String>(start + 3)?,
            store_path: row.get::<String>(start + 4)?,
            references: Json::from_str(&row.get::<String>(start + 5)?)?,
            system: row.get::<Option<String>>(start + 6)?,
            deriver: row.get::<Option<String>>(start + 7)?,
            sigs: Json::from_str(&row.get::<String>(start + 8)?)?,
            ca: row.get::<Option<String>>(start + 9)?,
            created_at: parse_datetime(&row.get::<String>(start + 10)?)?,
            last_accessed_at: row
                .get::<Option<String>>(start + 11)?
                .map(|s| parse_datetime(&s))
                .transpose()?,
            created_by: row.get::<Option<String>>(start + 12)?,
        })
    }

    /// Parses an ObjectModel from a row with a column prefix (for joins).
    pub fn from_row_prefixed(row: &Row, _prefix: &str, start_idx: usize) -> Result<Self> {
        Self::from_row_at(row, start_idx as i32)
    }

    /// Returns the number of columns in this model.
    pub const fn column_count() -> usize {
        13
    }

    /// Converts this object to a NarInfo.
    pub fn to_nar_info(&self, nar: &NarModel) -> ServerResult<NarInfo> {
        let nar_size = nar
            .nar_size
            .try_into()
            .map_err(ServerError::database_error)?;

        Ok(NarInfo {
            store_path: PathBuf::from(self.store_path.to_owned()),
            url: format!("nar/{}.nar", self.store_path_hash.as_str()),
            compression: Compression::from_str(&nar.compression)?,
            file_hash: None,
            file_size: None,
            nar_hash: Hash::from_typed(&nar.nar_hash)?,
            nar_size,
            system: self.system.to_owned(),
            references: self.references.0.to_owned(),
            deriver: self.deriver.to_owned(),
            signature: None,
            ca: self.ca.to_owned(),
        })
    }
}

/// A content-addressed chunk in the global cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkModel {
    pub id: i64,
    pub state: ChunkState,
    pub chunk_hash: String,
    pub chunk_size: i64,
    pub file_hash: Option<String>,
    pub file_size: Option<i64>,
    pub compression: String,
    pub remote_file: Json<RemoteFile>,
    pub remote_file_id: String,
    pub holders_count: i32,
    pub created_at: DateTime<Utc>,
}

impl ChunkModel {
    /// Parses a ChunkModel from a database row.
    pub fn from_row(row: &Row) -> Result<Self> {
        Self::from_row_at(row, 0)
    }

    /// Parses a ChunkModel from a row starting at the given index.
    pub fn from_row_at(row: &Row, start: i32) -> Result<Self> {
        Ok(Self {
            id: row.get::<i64>(start)?,
            state: ChunkState::from_db_value(&row.get::<String>(start + 1)?)?,
            chunk_hash: row.get::<String>(start + 2)?,
            chunk_size: row.get::<i64>(start + 3)?,
            file_hash: row.get::<Option<String>>(start + 4)?,
            file_size: row.get::<Option<i64>>(start + 5)?,
            compression: row.get::<String>(start + 6)?,
            remote_file: Json::from_str(&row.get::<String>(start + 7)?)?,
            remote_file_id: row.get::<String>(start + 8)?,
            holders_count: row.get::<i64>(start + 9)? as i32,
            created_at: parse_datetime(&row.get::<String>(start + 10)?)?,
        })
    }

    /// Parses a ChunkModel from a row with a column prefix (for joins).
    pub fn from_row_prefixed(row: &Row, _prefix: &str, start_idx: usize) -> Result<Self> {
        Self::from_row_at(row, start_idx as i32)
    }

    /// Tries to parse a ChunkModel from a row, returning None if the id column is NULL.
    pub fn try_from_row_prefixed(row: &Row, _prefix: &str, start_idx: usize) -> Result<Option<Self>> {
        let start = start_idx as i32;
        // Check if the id column is NULL (indicating a missing chunk due to LEFT JOIN)
        if row.get::<Option<i64>>(start)?.is_none() {
            return Ok(None);
        }
        Ok(Some(Self::from_row_at(row, start)?))
    }

    /// Returns the number of columns in this model.
    pub const fn column_count() -> usize {
        11
    }
}

/// A reference binding a NAR to a chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkRefModel {
    pub id: i64,
    pub nar_id: i64,
    pub seq: i32,
    pub chunk_id: Option<i64>,
    pub chunk_hash: String,
    pub compression: String,
}

impl ChunkRefModel {
    /// Parses a ChunkRefModel from a database row.
    pub fn from_row(row: &Row) -> Result<Self> {
        Self::from_row_at(row, 0)
    }

    /// Parses a ChunkRefModel from a row starting at the given index.
    pub fn from_row_at(row: &Row, start: i32) -> Result<Self> {
        Ok(Self {
            id: row.get::<i64>(start)?,
            nar_id: row.get::<i64>(start + 1)?,
            seq: row.get::<i64>(start + 2)? as i32,
            chunk_id: row.get::<Option<i64>>(start + 3)?,
            chunk_hash: row.get::<String>(start + 4)?,
            compression: row.get::<String>(start + 5)?,
        })
    }

    /// Parses a ChunkRefModel from a row with a column prefix (for joins).
    pub fn from_row_prefixed(row: &Row, _prefix: &str, start_idx: usize) -> Result<Self> {
        Self::from_row_at(row, start_idx as i32)
    }

    /// Returns the number of columns in this model.
    pub const fn column_count() -> usize {
        6
    }
}

/// Parses a datetime string from the database.
fn parse_datetime(s: &str) -> Result<DateTime<Utc>> {
    // SQLite stores timestamps in various formats
    // Try RFC3339 first, then other common formats
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            // Try SQLite's default format: "YYYY-MM-DD HH:MM:SS"
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .map(|dt| dt.and_utc())
        })
        .or_else(|_| {
            // Try with fractional seconds
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                .map(|dt| dt.and_utc())
        })
        .map_err(|e| anyhow!("Failed to parse datetime '{}': {}", s, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nar_state_conversion() {
        assert_eq!(NarState::from_db_value("V").unwrap(), NarState::Valid);
        assert_eq!(NarState::Valid.to_db_value(), "V");
        assert_eq!(NarState::from_db_value("P").unwrap(), NarState::PendingUpload);
        assert_eq!(NarState::PendingUpload.to_db_value(), "P");
    }

    #[test]
    fn test_chunk_state_conversion() {
        assert_eq!(ChunkState::from_db_value("V").unwrap(), ChunkState::Valid);
        assert_eq!(ChunkState::Valid.to_db_value(), "V");
    }

    #[test]
    fn test_parse_datetime() {
        // RFC3339
        assert!(parse_datetime("2024-01-15T10:30:00Z").is_ok());
        // SQLite format
        assert!(parse_datetime("2024-01-15 10:30:00").is_ok());
        // With fractional seconds
        assert!(parse_datetime("2024-01-15 10:30:00.123").is_ok());
    }
}
