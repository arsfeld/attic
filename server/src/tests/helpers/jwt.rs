//! Test JWT token builder.

use chrono::{Duration, Utc};

use attic::cache::CacheNamePattern;
use attic_token::{CachePermission, HS256Key, SignatureType, Token};

/// Builder for creating test JWT tokens.
pub struct TestTokenBuilder {
    subject: String,
    permissions: Vec<(CacheNamePattern, CachePermission)>,
    expires_in: Duration,
}

impl TestTokenBuilder {
    /// Creates a new token builder with the given subject.
    pub fn new(subject: &str) -> Self {
        Self {
            subject: subject.to_string(),
            permissions: Vec::new(),
            expires_in: Duration::hours(1),
        }
    }

    /// Adds pull permission for a cache pattern.
    pub fn with_pull(mut self, cache_pattern: &str) -> Self {
        let pattern: CacheNamePattern = cache_pattern.parse().expect("Invalid cache pattern");
        let mut perm = self.get_or_default_permission(&pattern);
        perm.pull = true;
        self.set_permission(pattern, perm);
        self
    }

    /// Adds push permission for a cache pattern.
    pub fn with_push(mut self, cache_pattern: &str) -> Self {
        let pattern: CacheNamePattern = cache_pattern.parse().expect("Invalid cache pattern");
        let mut perm = self.get_or_default_permission(&pattern);
        perm.push = true;
        self.set_permission(pattern, perm);
        self
    }

    /// Adds delete permission for a cache pattern.
    pub fn with_delete(mut self, cache_pattern: &str) -> Self {
        let pattern: CacheNamePattern = cache_pattern.parse().expect("Invalid cache pattern");
        let mut perm = self.get_or_default_permission(&pattern);
        perm.delete = true;
        self.set_permission(pattern, perm);
        self
    }

    /// Adds create cache permission for a cache pattern.
    pub fn with_create_cache(mut self, cache_pattern: &str) -> Self {
        let pattern: CacheNamePattern = cache_pattern.parse().expect("Invalid cache pattern");
        let mut perm = self.get_or_default_permission(&pattern);
        perm.create_cache = true;
        self.set_permission(pattern, perm);
        self
    }

    /// Adds configure cache permission for a cache pattern.
    pub fn with_configure_cache(mut self, cache_pattern: &str) -> Self {
        let pattern: CacheNamePattern = cache_pattern.parse().expect("Invalid cache pattern");
        let mut perm = self.get_or_default_permission(&pattern);
        perm.configure_cache = true;
        self.set_permission(pattern, perm);
        self
    }

    /// Adds configure cache retention permission for a cache pattern.
    pub fn with_configure_cache_retention(mut self, cache_pattern: &str) -> Self {
        let pattern: CacheNamePattern = cache_pattern.parse().expect("Invalid cache pattern");
        let mut perm = self.get_or_default_permission(&pattern);
        perm.configure_cache_retention = true;
        self.set_permission(pattern, perm);
        self
    }

    /// Adds destroy cache permission for a cache pattern.
    pub fn with_destroy_cache(mut self, cache_pattern: &str) -> Self {
        let pattern: CacheNamePattern = cache_pattern.parse().expect("Invalid cache pattern");
        let mut perm = self.get_or_default_permission(&pattern);
        perm.destroy_cache = true;
        self.set_permission(pattern, perm);
        self
    }

    /// Adds all permissions for a cache pattern (admin access).
    pub fn with_full_access(mut self, cache_pattern: &str) -> Self {
        let pattern: CacheNamePattern = cache_pattern.parse().expect("Invalid cache pattern");
        let perm = CachePermission {
            pull: true,
            push: true,
            delete: true,
            create_cache: true,
            configure_cache: true,
            configure_cache_retention: true,
            destroy_cache: true,
        };
        self.set_permission(pattern, perm);
        self
    }

    /// Sets the token expiration time.
    pub fn expires_in(mut self, duration: Duration) -> Self {
        self.expires_in = duration;
        self
    }

    /// Builds and encodes the token using the provided secret.
    pub fn build(self, secret: &HS256Key) -> String {
        let expiration = Utc::now() + self.expires_in;
        let mut token = Token::new(self.subject, &expiration);

        for (pattern, permission) in self.permissions {
            let perm = token.get_or_insert_permission_mut(pattern);
            *perm = permission;
        }

        let signature_type = SignatureType::HS256(secret.clone());
        token
            .encode(&signature_type, &None, &None)
            .expect("Failed to encode token")
    }

    fn get_or_default_permission(&self, pattern: &CacheNamePattern) -> CachePermission {
        self.permissions
            .iter()
            .find(|(p, _)| p == pattern)
            .map(|(_, perm)| perm.clone())
            .unwrap_or_default()
    }

    fn set_permission(&mut self, pattern: CacheNamePattern, permission: CachePermission) {
        if let Some(pos) = self.permissions.iter().position(|(p, _)| p == &pattern) {
            self.permissions[pos] = (pattern, permission);
        } else {
            self.permissions.push((pattern, permission));
        }
    }
}
