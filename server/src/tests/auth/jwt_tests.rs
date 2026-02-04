//! Tests for JWT token validation.

use chrono::{Duration, Utc};

use attic_token::{CachePermission, HS256Key, SignatureType, Token};

use crate::tests::helpers::TestTokenBuilder;

// ==================== Token Creation Tests ====================

#[test]
fn test_token_builder_pull_permission() {
    let secret = HS256Key::generate();
    let token_str = TestTokenBuilder::new("test-user")
        .with_pull("test-cache")
        .build(&secret);

    // Verify the token can be decoded
    let signature_type = SignatureType::HS256(secret);
    let token = Token::from_jwt(&token_str, &signature_type, &None, &None).unwrap();

    let perm = token.get_permission_for_cache(&"test-cache".parse().unwrap());
    assert!(perm.pull);
    assert!(!perm.push);
}

#[test]
fn test_token_builder_push_permission() {
    let secret = HS256Key::generate();
    let token_str = TestTokenBuilder::new("test-user")
        .with_push("test-cache")
        .build(&secret);

    let signature_type = SignatureType::HS256(secret);
    let token = Token::from_jwt(&token_str, &signature_type, &None, &None).unwrap();

    let perm = token.get_permission_for_cache(&"test-cache".parse().unwrap());
    assert!(perm.push);
}

#[test]
fn test_token_builder_full_access() {
    let secret = HS256Key::generate();
    let token_str = TestTokenBuilder::new("test-user")
        .with_full_access("test-cache")
        .build(&secret);

    let signature_type = SignatureType::HS256(secret);
    let token = Token::from_jwt(&token_str, &signature_type, &None, &None).unwrap();

    let perm = token.get_permission_for_cache(&"test-cache".parse().unwrap());
    assert!(perm.pull);
    assert!(perm.push);
    assert!(perm.delete);
    assert!(perm.create_cache);
    assert!(perm.configure_cache);
    assert!(perm.configure_cache_retention);
    assert!(perm.destroy_cache);
}

#[test]
fn test_token_builder_wildcard_permission() {
    let secret = HS256Key::generate();
    let token_str = TestTokenBuilder::new("test-user")
        .with_pull("team-*")
        .build(&secret);

    let signature_type = SignatureType::HS256(secret);
    let token = Token::from_jwt(&token_str, &signature_type, &None, &None).unwrap();

    // Should match team-project but not other-cache
    let perm_match = token.get_permission_for_cache(&"team-project".parse().unwrap());
    assert!(perm_match.pull);

    let perm_no_match = token.get_permission_for_cache(&"other-cache".parse().unwrap());
    assert!(!perm_no_match.pull);
}

// ==================== Token Validation Tests ====================

#[test]
fn test_invalid_token_signature() {
    let secret1 = HS256Key::generate();
    let secret2 = HS256Key::generate();

    // Create token with secret1
    let token_str = TestTokenBuilder::new("test-user")
        .with_pull("test-cache")
        .build(&secret1);

    // Try to verify with secret2
    let signature_type = SignatureType::HS256(secret2);
    let result = Token::from_jwt(&token_str, &signature_type, &None, &None);
    assert!(result.is_err());
}

#[test]
fn test_expired_token() {
    let secret = HS256Key::generate();

    // Create a token that expired 1 hour ago
    let token_str = TestTokenBuilder::new("test-user")
        .with_pull("test-cache")
        .expires_in(Duration::hours(-1))
        .build(&secret);

    let signature_type = SignatureType::HS256(secret);
    let result = Token::from_jwt(&token_str, &signature_type, &None, &None);
    assert!(result.is_err());
}

#[test]
fn test_malformed_token() {
    let secret = HS256Key::generate();
    let signature_type = SignatureType::HS256(secret);

    let result = Token::from_jwt("not-a-valid-jwt", &signature_type, &None, &None);
    assert!(result.is_err());
}

// ==================== Permission Hierarchy Tests ====================

#[test]
fn test_permission_can_discover_with_pull() {
    let perm = CachePermission {
        pull: true,
        ..Default::default()
    };
    assert!(perm.can_discover());
}

#[test]
fn test_permission_can_discover_with_push() {
    let perm = CachePermission {
        push: true,
        ..Default::default()
    };
    assert!(perm.can_discover());
}

#[test]
fn test_permission_cannot_discover_default() {
    let perm = CachePermission::default();
    assert!(!perm.can_discover());
}
