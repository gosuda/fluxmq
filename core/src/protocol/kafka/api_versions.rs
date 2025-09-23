//! Kafka API Version Support
//!
//! This module handles API version negotiation and compatibility.
//! Different API versions support different features and message formats.

use std::collections::HashMap;

/// Supported API versions for each Kafka API
#[derive(Debug, Clone)]
pub struct ApiVersionInfo {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

impl ApiVersionInfo {
    pub fn new(api_key: i16, min_version: i16, max_version: i16) -> Self {
        Self {
            api_key,
            min_version,
            max_version,
        }
    }

    /// Check if a version is supported for this API
    pub fn supports_version(&self, version: i16) -> bool {
        version >= self.min_version && version <= self.max_version
    }

    /// Get the highest supported version (used for version negotiation)
    pub fn max_supported_version(&self, client_version: i16) -> i16 {
        std::cmp::min(self.max_version, client_version)
    }
}

/// API version registry for FluxMQ's Kafka compatibility
pub struct ApiVersionRegistry {
    versions: HashMap<i16, ApiVersionInfo>,
}

impl ApiVersionRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            versions: HashMap::new(),
        };

        // Register supported API versions
        // These versions are chosen for broad compatibility with Kafka clients

        // Core messaging APIs
        registry.register(0, 0, 7); // Produce: v0-v7 (v7 is latest as of Kafka 2.8)
        registry.register(1, 0, 11); // Fetch: v0-v11 (v11 is latest as of Kafka 2.8)
        registry.register(3, 0, 9); // Metadata: v0-v9 (v9 is latest as of Kafka 2.8)

        // Consumer group APIs
        registry.register(10, 0, 3); // FindCoordinator: v0-v3
        registry.register(11, 0, 5); // JoinGroup: v0-v5
        registry.register(12, 0, 3); // Heartbeat: v0-v3
        registry.register(13, 0, 2); // LeaveGroup: v0-v2
        registry.register(14, 0, 3); // SyncGroup: v0-v3
        registry.register(15, 0, 4); // DescribeGroups: v0-v4
        registry.register(16, 0, 3); // ListGroups: v0-v3

        // Offset APIs
        registry.register(2, 0, 5); // ListOffsets: v0-v5
        registry.register(8, 0, 6); // OffsetCommit: v0-v6
        registry.register(9, 0, 5); // OffsetFetch: v0-v5

        // Protocol negotiation
        registry.register(18, 0, 3); // ApiVersions: v0-v3

        // Admin APIs (basic support)
        registry.register(19, 0, 4); // CreateTopics: v0-v4
        registry.register(20, 0, 3); // DeleteTopics: v0-v3
        registry.register(32, 0, 3); // DescribeConfigs: v0-v3
        registry.register(33, 0, 2); // AlterConfigs: v0-v2

        // SASL Authentication APIs
        registry.register(17, 0, 1); // SaslHandshake: v0-v1
        registry.register(36, 0, 2); // SaslAuthenticate: v0-v2

        // Transaction APIs (exactly-once semantics)
        registry.register(22, 0, 4); // InitProducerId: v0-v4
        registry.register(24, 0, 3); // AddPartitionsToTxn: v0-v3
        registry.register(25, 0, 3); // AddOffsetsToTxn: v0-v3
        registry.register(26, 0, 3); // EndTxn: v0-v3
        registry.register(27, 0, 1); // WriteTxnMarkers: v0-v1
        registry.register(28, 0, 3); // TxnOffsetCommit: v0-v3

        registry
    }

    fn register(&mut self, api_key: i16, min_version: i16, max_version: i16) {
        self.versions.insert(
            api_key,
            ApiVersionInfo::new(api_key, min_version, max_version),
        );
    }

    /// Get version info for an API key
    pub fn get_version_info(&self, api_key: i16) -> Option<&ApiVersionInfo> {
        self.versions.get(&api_key)
    }

    /// Check if an API key and version combination is supported
    pub fn is_supported(&self, api_key: i16, version: i16) -> bool {
        self.versions
            .get(&api_key)
            .map_or(false, |info| info.supports_version(version))
    }

    /// Get all supported API versions (for ApiVersionsResponse)
    pub fn get_all_versions(&self) -> Vec<&ApiVersionInfo> {
        let mut versions: Vec<_> = self.versions.values().collect();
        versions.sort_by_key(|v| v.api_key);
        versions
    }

    /// Negotiate the best version for a given API key and client max version
    pub fn negotiate_version(&self, api_key: i16, client_max_version: i16) -> Option<i16> {
        self.versions
            .get(&api_key)
            .map(|info| info.max_supported_version(client_max_version))
    }

    /// Check if we support this API key at all
    pub fn supports_api(&self, api_key: i16) -> bool {
        self.versions.contains_key(&api_key)
    }
}

impl Default for ApiVersionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// API version compatibility helpers
pub mod compat {
    use super::*;

    /// Check if a Produce API version supports idempotent producers
    pub fn produce_supports_idempotent(version: i16) -> bool {
        version >= 3
    }

    /// Check if a Produce API version supports transactional producers
    pub fn produce_supports_transactions(version: i16) -> bool {
        version >= 3
    }

    /// Check if a Fetch API version supports isolation level
    pub fn fetch_supports_isolation_level(version: i16) -> bool {
        version >= 4
    }

    /// Check if a Fetch API version supports fetch sessions
    pub fn fetch_supports_sessions(version: i16) -> bool {
        version >= 7
    }

    /// Check if a Metadata API version supports auto topic creation flag
    pub fn metadata_supports_auto_creation(version: i16) -> bool {
        version >= 4
    }

    /// Check if a JoinGroup API version supports static membership
    pub fn join_group_supports_static_membership(version: i16) -> bool {
        version >= 5
    }

    /// Get default/recommended API versions for broad compatibility
    pub fn get_recommended_versions() -> HashMap<i16, i16> {
        let mut versions = HashMap::new();

        // Choose versions that balance features with compatibility
        versions.insert(0, 7); // Produce v7 - full feature support
        versions.insert(1, 7); // Fetch v7 - with fetch sessions but not too new
        versions.insert(2, 2); // ListOffsets v2 - with timestamp support
        versions.insert(3, 5); // Metadata v5 - good balance of features
        versions.insert(8, 3); // OffsetCommit v3 - with metadata
        versions.insert(9, 3); // OffsetFetch v3 - with metadata
        versions.insert(10, 1); // FindCoordinator v1 - stable coordinator discovery
        versions.insert(11, 3); // JoinGroup v3 - stable consumer group support
        versions.insert(12, 1); // Heartbeat v1 - standard heartbeat
        versions.insert(13, 1); // LeaveGroup v1 - basic leave support
        versions.insert(14, 1); // SyncGroup v1 - basic assignment support
        versions.insert(15, 1); // DescribeGroups v1 - basic group description
        versions.insert(16, 1); // ListGroups v1 - basic group listing
        versions.insert(18, 2); // ApiVersions v2 - standard version negotiation

        versions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_version_info() {
        let info = ApiVersionInfo::new(0, 1, 5);

        assert_eq!(info.api_key, 0);
        assert_eq!(info.min_version, 1);
        assert_eq!(info.max_version, 5);

        assert!(!info.supports_version(0));
        assert!(info.supports_version(1));
        assert!(info.supports_version(3));
        assert!(info.supports_version(5));
        assert!(!info.supports_version(6));

        assert_eq!(info.max_supported_version(3), 3);
        assert_eq!(info.max_supported_version(7), 5);
    }

    #[test]
    fn test_api_version_registry() {
        let registry = ApiVersionRegistry::new();

        // Test Produce API
        assert!(registry.is_supported(0, 0));
        assert!(registry.is_supported(0, 7));
        assert!(!registry.is_supported(0, 8));

        // Test unsupported API
        assert!(!registry.supports_api(999));
        assert!(!registry.is_supported(999, 0));

        // Test version negotiation
        assert_eq!(registry.negotiate_version(0, 3), Some(3));
        assert_eq!(registry.negotiate_version(0, 10), Some(7));
        assert_eq!(registry.negotiate_version(999, 1), None);

        // Test getting all versions
        let all_versions = registry.get_all_versions();
        assert!(!all_versions.is_empty());

        // Verify sorted by API key
        for i in 1..all_versions.len() {
            assert!(all_versions[i - 1].api_key <= all_versions[i].api_key);
        }
    }

    #[test]
    fn test_compatibility_helpers() {
        use compat::*;

        assert!(!produce_supports_idempotent(2));
        assert!(produce_supports_idempotent(3));

        assert!(!fetch_supports_isolation_level(3));
        assert!(fetch_supports_isolation_level(4));

        assert!(!metadata_supports_auto_creation(3));
        assert!(metadata_supports_auto_creation(4));

        let recommended = get_recommended_versions();
        assert!(recommended.contains_key(&0)); // Produce
        assert!(recommended.contains_key(&1)); // Fetch
        assert!(recommended.contains_key(&3)); // Metadata
    }
}
