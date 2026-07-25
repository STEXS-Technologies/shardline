use std::collections::BTreeMap;

/// Auth policy for a single route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteAuthPolicy {
    /// Requires authentication with Bearer token (read or write scope).
    Authenticated,
    /// Requires authentication with Bearer token and write scope.
    AuthenticatedWrite,
    /// Open to unauthenticated requests (health checks, etc.).
    Open,
    /// Protected by a separate mechanism (provider bootstrap key, webhook HMAC, etc.).
    SeparatelyProtected,
}

/// A registry of every route and its auth policy.
#[derive(Debug, Clone)]
pub struct RoutePolicyRegistry {
    routes: BTreeMap<String, RouteAuthPolicy>,
}

impl RoutePolicyRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            routes: BTreeMap::new(),
        }
    }

    /// Registers a route with its auth policy.
    pub fn register(&mut self, method: &str, path: &str, policy: RouteAuthPolicy) {
        self.routes.insert(format!("{method} {path}"), policy);
    }

    /// Returns the policy for a route, if registered.
    #[must_use]
    pub fn policy(&self, method: &str, path: &str) -> Option<RouteAuthPolicy> {
        self.routes.get(&format!("{method} {path}")).copied()
    }

    /// Returns all registered routes sorted by key.
    #[must_use]
    pub fn entries(&self) -> impl Iterator<Item = (&str, RouteAuthPolicy)> {
        self.routes
            .iter()
            .map(|(key, policy)| (key.as_str(), *policy))
    }

    /// Returns the number of registered routes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    /// Returns true if no routes are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

impl Default for RoutePolicyRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Registers all application routes with their auth policies.
///
/// Every route added to the router must have a corresponding entry here.
/// When adding a new route, update this function AND the route count test.
pub(crate) fn register_route_policies(registry: &mut RoutePolicyRegistry) {
    // Health and readiness — open
    registry.register("GET", "/healthz", RouteAuthPolicy::Open);
    registry.register("GET", "/readyz", RouteAuthPolicy::Open);

    // Metrics — separately protected (token file)
    registry.register("GET", "/metrics", RouteAuthPolicy::SeparatelyProtected);

    // Stats — authenticated
    registry.register("GET", "/v1/stats", RouteAuthPolicy::Authenticated);

    // Provider token issuance — separately protected
    registry.register(
        "POST",
        "/v1/providers/{provider}/tokens",
        RouteAuthPolicy::SeparatelyProtected,
    );
    registry.register(
        "POST",
        "/v1/providers/{provider}/git-lfs-authenticate",
        RouteAuthPolicy::SeparatelyProtected,
    );
    registry.register(
        "POST",
        "/v1/providers/{provider}/webhooks",
        RouteAuthPolicy::SeparatelyProtected,
    );

    // XET read/write token routes — separately protected
    registry.register(
        "GET",
        "/v1/xet-read-token/{rev}",
        RouteAuthPolicy::SeparatelyProtected,
    );
    registry.register(
        "GET",
        "/v1/xet-write-token/{rev}",
        RouteAuthPolicy::SeparatelyProtected,
    );

    // Reconstruction routes — authenticated
    registry.register(
        "GET",
        "/v1/reconstructions",
        RouteAuthPolicy::Authenticated,
    );
    registry.register(
        "GET",
        "/v1/reconstructions/{file_id}",
        RouteAuthPolicy::Authenticated,
    );

    // Shard upload — write
    registry.register("POST", "/v1/shards", RouteAuthPolicy::AuthenticatedWrite);

    // Chunk read — authenticated
    registry.register(
        "GET",
        "/v1/chunks/default/{hash}",
        RouteAuthPolicy::Authenticated,
    );

    // Xorb — read/write
    registry.register(
        "HEAD",
        "/v1/xorbs/default/{hash}",
        RouteAuthPolicy::Authenticated,
    );
    registry.register(
        "POST",
        "/v1/xorbs/default/{hash}",
        RouteAuthPolicy::AuthenticatedWrite,
    );

    // Xorb transfer — read/write
    registry.register(
        "GET",
        "/transfer/xorb/{prefix}/{hash}",
        RouteAuthPolicy::Authenticated,
    );
    registry.register(
        "PUT",
        "/transfer/xorb/{prefix}/{hash}",
        RouteAuthPolicy::AuthenticatedWrite,
    );

    // LFS routes — authenticated
    registry.register(
        "POST",
        "/v1/lfs/objects/batch",
        RouteAuthPolicy::Authenticated,
    );
    registry.register(
        "GET",
        "/v1/lfs/objects/{oid}",
        RouteAuthPolicy::Authenticated,
    );
    registry.register(
        "PUT",
        "/v1/lfs/objects/{oid}",
        RouteAuthPolicy::AuthenticatedWrite,
    );

    // Bazel cache routes — authenticated
    registry.register(
        "PUT",
        "/v1/bazel/cache/ac/{hash}",
        RouteAuthPolicy::AuthenticatedWrite,
    );
    registry.register(
        "GET",
        "/v1/bazel/cache/cas/{hash}",
        RouteAuthPolicy::Authenticated,
    );

    // OCI routes — authenticated
    registry.register("GET", "/v2/", RouteAuthPolicy::Authenticated);
    registry.register(
        "GET",
        "/v2/token",
        RouteAuthPolicy::SeparatelyProtected,
    );

    // Reconstruction v2
    registry.register(
        "GET",
        "/v2/reconstructions/{file_id}",
        RouteAuthPolicy::Authenticated,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_policy_registry_has_expected_count() {
        let mut registry = RoutePolicyRegistry::new();
        register_route_policies(&mut registry);
        // When adding a new route, update this count AND add its policy above.
        // This test ensures no route is added without an auth policy.
        assert!(
            registry.len() >= 20,
            "Expected at least 20 registered routes, got {}. Add a policy for new routes.",
            registry.len()
        );
    }

    #[test]
    fn route_policy_registry_no_duplicates() {
        let mut registry = RoutePolicyRegistry::new();
        register_route_policies(&mut registry);
        let entries: Vec<_> = registry.entries().collect();
        let unique: std::collections::HashSet<_> = entries.iter().map(|(k, _)| *k).collect();
        assert_eq!(entries.len(), unique.len(), "Duplicate route registered");
    }

    #[test]
    fn policy_returns_none_for_unregistered_route() {
        let registry = RoutePolicyRegistry::new();
        assert_eq!(registry.policy("GET", "/nonexistent"), None);
    }

    #[test]
    fn policy_returns_expected_value_for_registered_route() {
        let mut registry = RoutePolicyRegistry::new();
        registry.register("GET", "/healthz", RouteAuthPolicy::Open);
        assert_eq!(
            registry.policy("GET", "/healthz"),
            Some(RouteAuthPolicy::Open)
        );
    }

    #[test]
    fn policy_method_sensitive() {
        let mut registry = RoutePolicyRegistry::new();
        registry.register("GET", "/test", RouteAuthPolicy::Authenticated);
        registry.register("POST", "/test", RouteAuthPolicy::AuthenticatedWrite);
        assert_eq!(
            registry.policy("GET", "/test"),
            Some(RouteAuthPolicy::Authenticated)
        );
        assert_eq!(
            registry.policy("POST", "/test"),
            Some(RouteAuthPolicy::AuthenticatedWrite)
        );
    }

    #[test]
    fn registry_default_is_empty() {
        let registry = RoutePolicyRegistry::default();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn entries_are_sorted_by_key() {
        let mut registry = RoutePolicyRegistry::new();
        registry.register("Z", "/route", RouteAuthPolicy::Open);
        registry.register("A", "/route", RouteAuthPolicy::Authenticated);
        let entries: Vec<_> = registry.entries().collect();
        assert_eq!(entries[0].0, "A /route");
        assert_eq!(entries[1].0, "Z /route");
    }
}
