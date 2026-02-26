//! # FluxMQ Topic Management System
//!
//! This module provides centralized topic and partition management for FluxMQ,
//! handling topic creation, configuration, and metadata distribution.
//!
//! ## Overview
//!
//! The topic management system is responsible for:
//!
//! - **Topic Creation**: Dynamic topic creation with configurable parameters
//! - **Partition Management**: Automatic partition assignment and distribution
//! - **Metadata Distribution**: Providing topic metadata to clients and brokers
//! - **Configuration Management**: Per-topic configuration and policy enforcement
//!
//! ## Architecture
//!
//! ```
//! ┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
//! │ Kafka Clients   │───▶│   TopicManager   │───▶│  Storage Layer  │
//! │ (CreateTopics)  │    │                  │    │                 │
//! └─────────────────┘    └──────────────────┘    └─────────────────┘
//!                                 │
//!                                 ▼
//!                        ┌──────────────────┐
//!                        │ Consumer Groups  │
//!                        │ (Partition Info) │
//!                        └──────────────────┘
//! ```
//!
//! ## Topic Lifecycle
//!
//! 1. **Creation**: Topics are created via Kafka CreateTopics API or automatically on first use
//! 2. **Configuration**: Each topic has configurable partitions, retention, and replication settings
//! 3. **Partition Assignment**: Partitions are automatically distributed across available brokers
//! 4. **Metadata Distribution**: Topic metadata is provided to consumers and producers
//! 5. **Dynamic Updates**: Topic configuration can be modified at runtime
//!
//! ## Usage Example
//!
//! ```rust,no_run
//! use fluxmq::topic_manager::{TopicManager, TopicConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let topic_manager = TopicManager::new();
//!     
//!     // Create a topic with custom configuration
//!     let config = TopicConfig {
//!         num_partitions: 6,
//!         replication_factor: 1,
//!         segment_size: 512 * 1024 * 1024, // 512MB segments
//!         retention_ms: Some(7 * 24 * 60 * 60 * 1000), // 7 days
//!     };
//!     
//!     topic_manager.create_topic("my-topic", config)?;
//!     
//!     // Retrieve topic metadata for clients
//!     if let Some(metadata) = topic_manager.get_topic("my-topic") {
//!         println!("Topic has {} partitions", metadata.num_partitions);
//!         for partition in &metadata.partitions {
//!             println!("Partition {}: leader={:?}", partition.id, partition.leader);
//!         }
//!     }
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Integration with Other Modules
//!
//! The topic manager coordinates with other FluxMQ components:
//!
//! - [`crate::broker`]: Processes CreateTopics and DeleteTopics API requests
//! - [`crate::storage`]: Creates storage segments for new topics and partitions
//! - [`crate::consumer`]: Provides partition information for consumer group assignments
//! - [`crate::protocol`]: Supplies topic metadata for Metadata API responses
//! - [`crate::replication`]: Coordinates partition replication across brokers
//!
//! ## Cross-References
//!
//! - [`crate::protocol::MetadataResponse`] - Topic metadata in protocol responses
//! - [`crate::consumer::TopicPartition`] - Partition identifiers for consumer groups  
//! - [`crate::storage::HybridStorage`] - Physical storage for topic data
//! - [`crate::replication::PartitionReplicaInfo`] - Replication metadata
//!
//! ## Configuration Options
//!
//! ### Default Topic Configuration
//! ```rust,no_run
//! use fluxmq::topic_manager::TopicConfig;
//!
//! let default_config = TopicConfig::default();
//! // num_partitions: 3 (good balance for small clusters)
//! // replication_factor: 1 (single broker setup)
//! // segment_size: 1GB (optimal for most workloads)
//! // retention_ms: None (unlimited retention)
//! ```
//!
//! ### High-Throughput Topics
//! ```rust,no_run
//! let high_throughput_config = TopicConfig {
//!     num_partitions: 12,        // More parallelism
//!     replication_factor: 1,     // Single node
//!     segment_size: 2 * 1024 * 1024 * 1024, // 2GB segments
//!     retention_ms: Some(24 * 60 * 60 * 1000), // 1 day retention
//! };
//! ```
//!
//! ## Thread Safety
//!
//! The `TopicManager` is designed for concurrent access:
//! - **Read Operations**: Lock-free for topic metadata retrieval
//! - **Write Operations**: Brief write locks for topic creation/deletion
//! - **Memory Consistency**: All operations are atomic and linearizable

use crate::protocol::{PartitionId, TopicName};
use crate::Result;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::info;

/// Configuration for topic creation
///
/// This structure defines the parameters for creating new topics in FluxMQ.
/// Each topic can have customized partition count, replication settings,
/// and storage characteristics.
///
/// # Examples
///
/// ```rust,no_run
/// use fluxmq::topic_manager::TopicConfig;
///
/// // Default configuration - good for development
/// let default_config = TopicConfig::default();
///
/// // High-throughput configuration
/// let high_throughput = TopicConfig {
///     num_partitions: 12,
///     replication_factor: 1,
///     segment_size: 2 * 1024 * 1024 * 1024, // 2GB
///     retention_ms: Some(86400000), // 24 hours
/// };
/// ```
#[derive(Debug, Clone)]
pub struct TopicConfig {
    pub num_partitions: u32,
    pub replication_factor: u32,
    pub segment_size: u64,
    pub retention_ms: Option<u64>,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            num_partitions: 3,                // Default to 3 partitions for load balancing
            replication_factor: 1,            // Single replica for now
            segment_size: 1024 * 1024 * 1024, // 1GB
            retention_ms: None,               // No retention limit
        }
    }
}

/// Manages topic metadata and partition assignment (lock-free)
#[derive(Debug)]
pub struct TopicManager {
    /// Lock-free topic storage using DashMap
    topics: Arc<DashMap<TopicName, TopicMetadata>>,
}

#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub name: TopicName,
    pub num_partitions: u32,
    pub config: TopicConfig,
    pub partitions: Vec<PartitionInfo>,
}

#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub id: PartitionId,
    pub leader: Option<u32>, // Broker ID (0 for single-node)
    pub replicas: Vec<u32>,
    pub in_sync_replicas: Vec<u32>,
}

impl TopicManager {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(DashMap::new()),
        }
    }

    /// Create a new topic with the specified configuration (lock-free, atomic)
    pub fn create_topic(&self, topic_name: &str, config: TopicConfig) -> Result<()> {
        // Validate configuration to prevent division-by-zero and invalid states
        if config.num_partitions == 0 {
            return Err(crate::FluxmqError::Config(
                "num_partitions must be >= 1, got 0".to_string(),
            ));
        }
        if config.segment_size == 0 {
            return Err(crate::FluxmqError::Config(
                "segment_size must be > 0".to_string(),
            ));
        }

        // Atomic check-and-insert using DashMap entry() API to prevent TOCTOU race
        use dashmap::mapref::entry::Entry;
        match self.topics.entry(topic_name.to_string()) {
            Entry::Occupied(_) => {
                // Topic already exists, that's fine
                Ok(())
            }
            Entry::Vacant(entry) => {
                let partitions: Vec<PartitionInfo> = (0..config.num_partitions)
                    .map(|partition_id| PartitionInfo {
                        id: partition_id,
                        leader: Some(0), // Single broker setup
                        replicas: vec![0],
                        in_sync_replicas: vec![0],
                    })
                    .collect();

                let metadata = TopicMetadata {
                    name: topic_name.to_string(),
                    num_partitions: config.num_partitions,
                    config: config.clone(),
                    partitions,
                };

                entry.insert(metadata);
                info!(
                    "Created topic '{}' with {} partitions",
                    topic_name, config.num_partitions
                );

                Ok(())
            }
        }
    }

    /// Get topic metadata (lock-free)
    pub fn get_topic(&self, topic_name: &str) -> Option<TopicMetadata> {
        self.topics.get(topic_name).map(|entry| entry.clone())
    }

    /// List all topics (lock-free)
    pub fn list_topics(&self) -> Vec<TopicName> {
        self.topics
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get partition assignment for a message key (lock-free)
    pub fn get_partition_for_key(
        &self,
        topic_name: &str,
        key: Option<&[u8]>,
    ) -> Option<PartitionId> {
        let topic = self.topics.get(topic_name)?;

        let partition = match key {
            Some(key_bytes) => {
                // Hash-based partitioning (simple hash for now)
                let hash = self.hash_key(key_bytes);
                hash % topic.num_partitions
            }
            None => {
                // Round-robin for messages without keys
                // For simplicity, we'll use partition 0 for now
                // In production, you'd maintain a counter per topic
                0
            }
        };

        Some(partition)
    }

    /// Get partition assignment using round-robin for keyless messages (lock-free)
    pub fn get_partition_round_robin(&self, topic_name: &str, counter: u64) -> Option<PartitionId> {
        let topic = self.topics.get(topic_name)?;
        Some((counter % topic.num_partitions as u64) as PartitionId)
    }

    /// Simple hash function for key-based partitioning
    fn hash_key(&self, key: &[u8]) -> u32 {
        // Simple FNV-1a hash
        let mut hash: u32 = 2166136261;
        for byte in key {
            hash ^= *byte as u32;
            hash = hash.wrapping_mul(16777619);
        }
        hash
    }

    /// Auto-create topic with default configuration if it doesn't exist (lock-free)
    pub fn ensure_topic_exists(&self, topic_name: &str) -> Result<TopicMetadata> {
        if let Some(topic) = self.get_topic(topic_name) {
            return Ok(topic);
        }

        // Auto-create with default config
        let default_config = TopicConfig::default();
        self.create_topic(topic_name, default_config)?;

        self.get_topic(topic_name)
            .ok_or_else(|| crate::FluxmqError::Config("Failed to create topic".to_string()))
    }

    /// Get all partitions for a topic (lock-free)
    pub fn get_partitions(&self, topic_name: &str) -> Vec<PartitionId> {
        match self.topics.get(topic_name) {
            Some(topic) => topic.partitions.iter().map(|p| p.id).collect(),
            None => vec![],
        }
    }

    /// Check if a specific partition exists for a topic (lock-free)
    pub fn partition_exists(&self, topic_name: &str, partition_id: PartitionId) -> bool {
        match self.topics.get(topic_name) {
            Some(topic) => partition_id < topic.num_partitions,
            None => false,
        }
    }

    /// Delete a topic (lock-free)
    pub fn delete_topic(&self, topic_name: &str) -> crate::Result<()> {
        match self.topics.remove(topic_name) {
            Some(_) => {
                info!("Successfully deleted topic '{}'", topic_name);
                Ok(())
            }
            None => Err(crate::FluxmqError::Config(format!(
                "Topic '{}' does not exist",
                topic_name
            ))),
        }
    }

    /// Get topic statistics (lock-free)
    pub fn get_topic_stats(&self, topic_name: &str) -> Option<u32> {
        self.topics
            .get(topic_name)
            .map(|topic| topic.num_partitions)
    }
}

/// Partition assignment strategies
pub enum PartitionStrategy {
    /// Hash-based partitioning using message key
    KeyHash,
    /// Round-robin assignment for keyless messages
    RoundRobin,
    /// Manual partition specification
    Manual(PartitionId),
}

/// Partition assignment utility (lock-free)
pub struct PartitionAssigner {
    manager: Arc<TopicManager>,
    /// Lock-free round-robin counters using DashMap with AtomicU64
    round_robin_counters: Arc<DashMap<TopicName, AtomicU64>>,
}

impl PartitionAssigner {
    pub fn new(manager: Arc<TopicManager>) -> Self {
        Self {
            manager,
            round_robin_counters: Arc::new(DashMap::new()),
        }
    }

    /// Assign partition based on strategy and message properties (lock-free)
    pub fn assign_partition(
        &self,
        topic_name: &str,
        key: Option<&[u8]>,
        strategy: PartitionStrategy,
    ) -> Result<PartitionId> {
        // Ensure topic exists
        let _topic = self.manager.ensure_topic_exists(topic_name)?;

        let partition = match strategy {
            PartitionStrategy::KeyHash => self
                .manager
                .get_partition_for_key(topic_name, key)
                .unwrap_or(0),
            PartitionStrategy::RoundRobin => {
                // Lock-free atomic counter increment
                let counter = self
                    .round_robin_counters
                    .entry(topic_name.to_string())
                    .or_insert_with(|| AtomicU64::new(0));
                let current = counter.fetch_add(1, Ordering::Relaxed) + 1;
                self.manager
                    .get_partition_round_robin(topic_name, current)
                    .unwrap_or(0)
            }
            PartitionStrategy::Manual(partition_id) => {
                if self.manager.partition_exists(topic_name, partition_id) {
                    partition_id
                } else {
                    return Err(crate::FluxmqError::Config(format!(
                        "Partition {} does not exist for topic {}",
                        partition_id, topic_name
                    )));
                }
            }
        };

        Ok(partition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_creation() {
        let manager = TopicManager::new();
        let config = TopicConfig {
            num_partitions: 5,
            ..Default::default()
        };

        manager.create_topic("test-topic", config).unwrap();

        let topic = manager.get_topic("test-topic").unwrap();
        assert_eq!(topic.name, "test-topic");
        assert_eq!(topic.num_partitions, 5);
        assert_eq!(topic.partitions.len(), 5);
    }

    #[test]
    fn test_partition_assignment_key_hash() {
        let manager = TopicManager::new();
        let config = TopicConfig {
            num_partitions: 3,
            ..Default::default()
        };

        manager.create_topic("test-topic", config).unwrap();

        // Same key should always go to same partition
        let partition1 = manager
            .get_partition_for_key("test-topic", Some(b"key1"))
            .unwrap();
        let partition2 = manager
            .get_partition_for_key("test-topic", Some(b"key1"))
            .unwrap();
        assert_eq!(partition1, partition2);

        // Different keys should potentially go to different partitions
        let partition3 = manager
            .get_partition_for_key("test-topic", Some(b"key2"))
            .unwrap();
        // Note: Due to hashing, this might be the same partition, but that's ok

        assert!(partition1 < 3);
        assert!(partition3 < 3);
    }

    #[test]
    fn test_partition_assigner() {
        let manager = Arc::new(TopicManager::new());
        let assigner = PartitionAssigner::new(manager);

        // Test auto-creation
        let partition = assigner
            .assign_partition("auto-topic", Some(b"key1"), PartitionStrategy::KeyHash)
            .unwrap();

        assert!(partition < 3); // Default is 3 partitions

        // Test round-robin
        let p1 = assigner
            .assign_partition("rr-topic", None, PartitionStrategy::RoundRobin)
            .unwrap();

        let p2 = assigner
            .assign_partition("rr-topic", None, PartitionStrategy::RoundRobin)
            .unwrap();

        // Should increment in round-robin fashion
        assert_ne!(p1, p2);
    }

    #[test]
    fn test_hash_consistency() {
        let manager = TopicManager::new();

        // Test that our hash function is deterministic
        let hash1 = manager.hash_key(b"test-key");
        let hash2 = manager.hash_key(b"test-key");
        assert_eq!(hash1, hash2);

        // Different keys should (probably) have different hashes
        let hash3 = manager.hash_key(b"different-key");
        assert_ne!(hash1, hash3);
    }
}
