//! Test fixtures for integration tests.

use sha2::{Digest, Sha256};

use attic::hash::Hash;
use attic::nix_store::StorePathHash;

/// A minimal valid NAR archive.
///
/// This is a NAR containing a single regular file with "hello world" content.
/// Generated structure:
/// - nix-archive-1
/// - (type regular contents "hello world")
pub fn minimal_nar() -> Vec<u8> {
    // NAR format: series of (tag, value) pairs
    // Strings are 8-byte length-prefixed and padded to 8-byte boundary
    let mut nar = Vec::new();

    // "nix-archive-1" header
    write_string(&mut nar, "nix-archive-1");

    // Start of entry: "("
    write_string(&mut nar, "(");

    // type = regular
    write_string(&mut nar, "type");
    write_string(&mut nar, "regular");

    // contents = "hello world"
    write_string(&mut nar, "contents");
    write_string(&mut nar, "hello world");

    // End of entry: ")"
    write_string(&mut nar, ")");

    nar
}

/// Computes the hash of the minimal NAR.
pub fn minimal_nar_hash() -> Hash {
    let nar = minimal_nar();
    let mut hasher = Sha256::new();
    hasher.update(&nar);
    let hash = hasher.finalize();
    Hash::Sha256(hash.as_slice().try_into().unwrap())
}

/// A test store path hash (32 characters).
pub fn test_store_path_hash() -> StorePathHash {
    StorePathHash::new("00000000000000000000000000000000".to_string()).unwrap()
}

/// A test store path.
pub fn test_store_path() -> String {
    "/nix/store/00000000000000000000000000000000-test".to_string()
}

/// Another test store path hash for multi-path tests.
pub fn test_store_path_hash_2() -> StorePathHash {
    StorePathHash::new("11111111111111111111111111111111".to_string()).unwrap()
}

/// Another test store path for multi-path tests.
pub fn test_store_path_2() -> String {
    "/nix/store/11111111111111111111111111111111-test2".to_string()
}

/// Writes a NAR-format string to the buffer.
fn write_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = bytes.len() as u64;

    // Write length as little-endian u64
    buf.extend_from_slice(&len.to_le_bytes());

    // Write the string content
    buf.extend_from_slice(bytes);

    // Pad to 8-byte boundary
    let padding = (8 - (bytes.len() % 8)) % 8;
    buf.extend(std::iter::repeat(0u8).take(padding));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minimal_nar_is_valid() {
        let nar = minimal_nar();
        // Should start with "nix-archive-1" length prefix
        assert!(nar.len() > 8);
        let len = u64::from_le_bytes(nar[0..8].try_into().unwrap());
        assert_eq!(len, 13); // "nix-archive-1".len()
    }

    #[test]
    fn test_minimal_nar_hash_is_consistent() {
        let hash1 = minimal_nar_hash();
        let hash2 = minimal_nar_hash();
        assert_eq!(hash1, hash2);
    }
}
