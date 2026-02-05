//! Permission helpers for the web UI.
//!
//! This module provides centralized permission checking logic for cache access
//! and token generation.

use crate::database::models::UserCachePermissionModel;

/// Effective permissions for a user on a specific cache.
#[derive(Debug, Clone, Default)]
pub struct EffectivePermissions {
    pub can_pull: bool,
    pub can_push: bool,
    pub can_delete: bool,
    pub can_create_cache: bool,
    pub can_configure_cache: bool,
    pub can_destroy_cache: bool,
}

impl EffectivePermissions {
    /// Returns true if the user has any permission on the cache.
    pub fn has_any(&self) -> bool {
        self.can_pull
            || self.can_push
            || self.can_delete
            || self.can_create_cache
            || self.can_configure_cache
            || self.can_destroy_cache
    }

    /// Creates full permissions (all true).
    pub fn full() -> Self {
        Self {
            can_pull: true,
            can_push: true,
            can_delete: true,
            can_create_cache: true,
            can_configure_cache: true,
            can_destroy_cache: true,
        }
    }
}

/// Checks if a cache name matches a pattern.
///
/// Patterns can be:
/// - Exact match: "my-cache" matches only "my-cache"
/// - Wildcard suffix: "team-*" matches "team-frontend", "team-backend", etc.
/// - Full wildcard: "*" matches everything
pub fn cache_matches_pattern(cache_name: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if pattern.ends_with('*') {
        let prefix = &pattern[..pattern.len() - 1];
        cache_name.starts_with(prefix)
    } else {
        cache_name == pattern
    }
}

/// Gets effective permissions for a user on a specific cache.
///
/// This aggregates permissions from all matching permission entries,
/// including wildcard patterns. Any permission that is true in any
/// matching entry results in true for that permission.
pub fn get_effective_permissions(
    permissions: &[UserCachePermissionModel],
    cache_name: &str,
) -> EffectivePermissions {
    let mut effective = EffectivePermissions::default();

    for perm in permissions {
        if cache_matches_pattern(cache_name, &perm.cache_name) {
            // Aggregate permissions - any true value wins
            effective.can_pull = effective.can_pull || perm.can_pull;
            effective.can_push = effective.can_push || perm.can_push;
            effective.can_delete = effective.can_delete || perm.can_delete;
            effective.can_create_cache = effective.can_create_cache || perm.can_create_cache;
            effective.can_configure_cache =
                effective.can_configure_cache || perm.can_configure_cache;
            effective.can_destroy_cache = effective.can_destroy_cache || perm.can_destroy_cache;
        }
    }

    effective
}

/// Computes the intersection of requested permissions and the user's actual permissions.
///
/// This is used for token generation - users can only grant permissions they actually have.
pub fn intersect_permissions(
    requested: &EffectivePermissions,
    user_has: &EffectivePermissions,
) -> EffectivePermissions {
    EffectivePermissions {
        can_pull: requested.can_pull && user_has.can_pull,
        can_push: requested.can_push && user_has.can_push,
        can_delete: requested.can_delete && user_has.can_delete,
        can_create_cache: requested.can_create_cache && user_has.can_create_cache,
        can_configure_cache: requested.can_configure_cache && user_has.can_configure_cache,
        can_destroy_cache: requested.can_destroy_cache && user_has.can_destroy_cache,
    }
}

/// Checks if a user has permission to create caches matching a pattern.
///
/// The user must have can_create_cache permission on a pattern that covers
/// the requested pattern.
pub fn can_create_cache_for_pattern(
    permissions: &[UserCachePermissionModel],
    pattern: &str,
) -> bool {
    // For any pattern, check if user has can_create_cache permission
    // that would cover that pattern
    for perm in permissions {
        if !perm.can_create_cache {
            continue;
        }

        // Check if the user's pattern covers the requested pattern
        if perm.cache_name == "*" {
            return true;
        }

        if perm.cache_name.ends_with('*') {
            let user_prefix = &perm.cache_name[..perm.cache_name.len() - 1];
            // User's wildcard must be a prefix of the requested pattern
            // e.g., user has "team-*", can create "team-foo"
            if pattern.starts_with(user_prefix) {
                return true;
            }
        } else {
            // Exact match required
            if perm.cache_name == pattern {
                return true;
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_permission(
        cache_name: &str,
        pull: bool,
        push: bool,
        delete: bool,
        create: bool,
        configure: bool,
        destroy: bool,
    ) -> UserCachePermissionModel {
        UserCachePermissionModel {
            id: 1,
            user_id: 1,
            cache_name: cache_name.to_string(),
            can_pull: pull,
            can_push: push,
            can_delete: delete,
            can_create_cache: create,
            can_configure_cache: configure,
            can_destroy_cache: destroy,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn test_exact_match() {
        assert!(cache_matches_pattern("my-cache", "my-cache"));
        assert!(!cache_matches_pattern("my-cache", "other-cache"));
        assert!(!cache_matches_pattern("my-cache-extra", "my-cache"));
    }

    #[test]
    fn test_wildcard_match() {
        assert!(cache_matches_pattern("team-frontend", "team-*"));
        assert!(cache_matches_pattern("team-backend", "team-*"));
        assert!(cache_matches_pattern("team-", "team-*"));
        assert!(!cache_matches_pattern("other-cache", "team-*"));
    }

    #[test]
    fn test_full_wildcard() {
        assert!(cache_matches_pattern("anything", "*"));
        assert!(cache_matches_pattern("", "*"));
    }

    #[test]
    fn test_get_effective_permissions_exact() {
        let perms = vec![make_permission(
            "my-cache", true, true, false, false, false, false,
        )];

        let effective = get_effective_permissions(&perms, "my-cache");
        assert!(effective.can_pull);
        assert!(effective.can_push);
        assert!(!effective.can_delete);
    }

    #[test]
    fn test_get_effective_permissions_wildcard() {
        let perms = vec![make_permission(
            "team-*", true, false, false, false, false, false,
        )];

        let effective = get_effective_permissions(&perms, "team-frontend");
        assert!(effective.can_pull);
        assert!(!effective.can_push);
    }

    #[test]
    fn test_get_effective_permissions_aggregation() {
        // Two permissions that together give full access
        let perms = vec![
            make_permission("my-cache", true, false, false, false, false, false),
            make_permission("*", false, true, false, false, false, false),
        ];

        let effective = get_effective_permissions(&perms, "my-cache");
        assert!(effective.can_pull);
        assert!(effective.can_push);
    }

    #[test]
    fn test_intersect_permissions() {
        let requested = EffectivePermissions {
            can_pull: true,
            can_push: true,
            can_delete: true,
            can_create_cache: false,
            can_configure_cache: false,
            can_destroy_cache: false,
        };

        let user_has = EffectivePermissions {
            can_pull: true,
            can_push: false,
            can_delete: false,
            can_create_cache: true,
            can_configure_cache: false,
            can_destroy_cache: false,
        };

        let result = intersect_permissions(&requested, &user_has);

        // User can only grant what they have
        assert!(result.can_pull);
        assert!(!result.can_push);
        assert!(!result.can_delete);
        assert!(!result.can_create_cache); // Not requested
    }

    #[test]
    fn test_can_create_cache_for_pattern() {
        let perms = vec![
            make_permission("team-*", false, false, false, true, false, false),
            make_permission("other", true, true, false, false, false, false),
        ];

        // User can create team-foo because they have team-* with create permission
        assert!(can_create_cache_for_pattern(&perms, "team-foo"));

        // User cannot create random-cache
        assert!(!can_create_cache_for_pattern(&perms, "random-cache"));

        // User cannot create "other" because that permission doesn't have can_create_cache
        assert!(!can_create_cache_for_pattern(&perms, "other"));
    }
}
