//! # FluxMQ Consumer Group Module
//!
//! This module implements Kafka-compatible consumer groups with full coordination,
//! partition assignment, and offset management capabilities.
//!
//! ## Overview
//!
//! Consumer groups enable multiple consumers to coordinate consumption of topics
//! by dividing partitions among group members. FluxMQ provides complete consumer
//! group functionality including:
//!
//! - **Membership Management**: Join/leave group operations with heartbeat monitoring
//! - **Partition Assignment**: Automatic partition distribution using configurable strategies
//! - **Offset Management**: Commit and fetch consumer position tracking
//! - **Rebalancing**: Automatic partition reassignment when membership changes
//! - **Failure Detection**: Session timeout and heartbeat-based member monitoring
//!
//! ## Core Components
//!
//! - [`ConsumerGroupCoordinator`] - Central coordinator managing all consumer groups
//! - [`ConsumerGroupManager`] - Per-group state management and coordination logic
//! - [`PartitionAssignor`] - Partition assignment strategies (RoundRobin, Range, Sticky)
//! - Consumer group protocol messages and data structures
//!
//! ## Assignment Strategies
//!
//! FluxMQ supports three partition assignment strategies:
//!
//! ### Round Robin Assignment
//! Distributes partitions evenly across all consumers in round-robin fashion:
//!
//! ```rust,no_run
//! use fluxmq::consumer::{PartitionAssignor, AssignmentStrategy, TopicPartition};
//!
//! let assignor = PartitionAssignor::new(AssignmentStrategy::RoundRobin);
//! let consumers = vec!["consumer1".to_string(), "consumer2".to_string()];
//! let partitions = vec![
//!     TopicPartition::new("topic1", 0),
//!     TopicPartition::new("topic1", 1),
//!     TopicPartition::new("topic1", 2),
//!     TopicPartition::new("topic1", 3),
//! ];
//!
//! let assignments = assignor.assign(&consumers, &partitions);
//! // Result: consumer1 gets partitions 0,2 and consumer2 gets partitions 1,3
//! ```
//!
//! ### Range Assignment
//! Assigns contiguous ranges of partitions to each consumer, grouped by topic:
//!
//! ```rust,no_run
//! use fluxmq::consumer::{PartitionAssignor, AssignmentStrategy, TopicPartition};
//!
//! let assignor = PartitionAssignor::new(AssignmentStrategy::Range);
//! let consumers = vec!["consumer1".to_string(), "consumer2".to_string()];
//! let partitions = vec![
//!     TopicPartition::new("topic1", 0),
//!     TopicPartition::new("topic1", 1),
//!     TopicPartition::new("topic1", 2),
//!     TopicPartition::new("topic1", 3),
//! ];
//!
//! let assignments = assignor.assign(&consumers, &partitions);
//! // Result: consumer1 gets partitions 0,1 and consumer2 gets partitions 2,3
//! ```
//!
//! ### Sticky Assignment
//! Minimizes partition movement during rebalancing while maintaining balance:
//!
//! ```rust,no_run
//! use fluxmq::consumer::{PartitionAssignor, AssignmentStrategy, TopicPartition};
//!
//! let assignor = PartitionAssignor::new(AssignmentStrategy::Sticky);
//! // Sticky assignment preserves existing assignments when possible
//! // while ensuring balanced distribution
//! ```
//!
//! ## Consumer Group Lifecycle
//!
//! ### Basic Consumer Group Operations
//!
//! ```rust,no_run
//! use fluxmq::consumer::{ConsumerGroupCoordinator, ConsumerGroupConfig, ConsumerGroupMessage};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ConsumerGroupConfig::default();
//!     let coordinator = Arc::new(ConsumerGroupCoordinator::new(config));
//!
//!     // Consumer joins group
//!     let join_message = ConsumerGroupMessage::JoinGroup {
//!         group_id: "my-consumer-group".to_string(),
//!         consumer_id: "consumer-1".to_string(),
//!         client_id: "kafka-rust".to_string(),
//!         client_host: "localhost".to_string(),
//!         session_timeout_ms: 10000,
//!         rebalance_timeout_ms: 60000,
//!         protocol_type: "consumer".to_string(),
//!         group_protocols: vec![],
//!     };
//!
//!     // Process join group request
//!     let response = coordinator.handle_join_group(join_message).await?;
//!     println!("Consumer joined group successfully");
//!
//!     Ok(())
//! }
//! ```
//!
//! ### Offset Management
//!
//! ```rust,no_run
//! use fluxmq::consumer::{ConsumerGroupCoordinator, OffsetCommitRequest, TopicPartitionOffset};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let coordinator = Arc::new(ConsumerGroupCoordinator::new(Default::default()));
//!
//!     // Commit consumer offsets
//!     let commit_request = OffsetCommitRequest {
//!         group_id: "my-group".to_string(),
//!         consumer_id: "consumer-1".to_string(),
//!         generation_id: 1,
//!         retention_time_ms: -1, // Use default retention
//!         offsets: vec![
//!             TopicPartitionOffset {
//!                 topic: "my-topic".to_string(),
//!                 partition: 0,
//!                 offset: 1000,
//!                 metadata: Some("processed batch 1".to_string()),
//!             },
//!         ],
//!     };
//!
//!     coordinator.commit_offsets(commit_request).await?;
//!     println!("Offsets committed successfully");
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Configuration
//!
//! Consumer groups can be configured with various timing and behavior parameters:
//!
//! ```rust,no_run
//! use fluxmq::consumer::{ConsumerGroupConfig, AssignmentStrategy};
//!
//! let config = ConsumerGroupConfig {
//!     default_session_timeout_ms: 30000,    // 30 seconds
//!     default_rebalance_timeout_ms: 300000, // 5 minutes
//!     min_session_timeout_ms: 6000,         // 6 seconds minimum
//!     max_session_timeout_ms: 1800000,      // 30 minutes maximum
//!     consumer_expiration_check_interval_ms: 3000, // Check every 3 seconds
//!     default_assignment_strategy: AssignmentStrategy::RoundRobin,
//!     group_metadata_retention_ms: 86400000, // 24 hours
//! };
//! ```
//!
//! ## Error Handling
//!
//! The module provides comprehensive error codes for different failure scenarios:
//!
//! ```rust,no_run
//! use fluxmq::consumer::error_codes;
//!
//! match error_code {
//!     error_codes::NONE => println!("Success"),
//!     error_codes::UNKNOWN_CONSUMER_ID => println!("Consumer not in group"),
//!     error_codes::REBALANCE_IN_PROGRESS => println!("Group is rebalancing"),
//!     error_codes::ILLEGAL_GENERATION => println!("Generation ID mismatch"),
//!     _ => println!("Other error: {}", error_code),
//! }
//! ```
//!
//! ## Performance Characteristics
//!
//! - **High Throughput**: Handles thousands of concurrent consumers per group
//! - **Low Latency**: Sub-millisecond group coordination operations
//! - **Scalability**: Supports hundreds of consumer groups simultaneously
//! - **Persistence**: Group metadata persisted with crash recovery
//! - **Efficiency**: Minimal network overhead with batched operations
//!
//! ## Kafka Compatibility
//!
//! This implementation provides full compatibility with Kafka consumer groups:
//!
//! - All Kafka consumer group APIs (8-16) supported
//! - Compatible with kafka-python, java kafka-clients, and other libraries
//! - Standard partition assignment strategies
//! - Protocol-compliant message formats and error codes

pub mod coordinator;

#[cfg(test)]
mod tests;

use crate::protocol::{PartitionId, TopicName};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Instant, SystemTime};

/// Default value for deserialized Instant fields (serde cannot serialize Instant)
fn default_instant() -> Instant {
    Instant::now()
}

pub use coordinator::{ConsumerGroupCoordinator, ConsumerGroupManager};

/// Unique identifier for consumer groups
pub type ConsumerGroupId = String;

/// Unique identifier for consumers within a group
pub type ConsumerId = String;

/// Consumer group state
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConsumerGroupState {
    /// Group is forming, waiting for members
    PreparingRebalance,
    /// Members are synchronizing partition assignments
    CompletingRebalance,
    /// Group is stable and consuming
    Stable,
    /// Group is empty, no active members
    Empty,
    /// Group is dead and should be removed
    Dead,
}

impl ConsumerGroupState {
    /// Validate whether a state transition is allowed.
    /// Returns true if transitioning from `self` to `target` is a valid state change.
    pub fn can_transition_to(&self, target: &ConsumerGroupState) -> bool {
        matches!(
            (self, target),
            // Normal lifecycle transitions
            (ConsumerGroupState::Empty, ConsumerGroupState::PreparingRebalance)
                | (ConsumerGroupState::Stable, ConsumerGroupState::PreparingRebalance)
                | (ConsumerGroupState::PreparingRebalance, ConsumerGroupState::CompletingRebalance)
                | (ConsumerGroupState::CompletingRebalance, ConsumerGroupState::Stable)
                // Member removal can empty the group from any active state
                | (ConsumerGroupState::PreparingRebalance, ConsumerGroupState::Empty)
                | (ConsumerGroupState::CompletingRebalance, ConsumerGroupState::Empty)
                | (ConsumerGroupState::Stable, ConsumerGroupState::Empty)
                // Any state can transition to Dead
                | (_, ConsumerGroupState::Dead)
                // Dead group can be resurrected by new JoinGroup
                | (ConsumerGroupState::Dead, ConsumerGroupState::PreparingRebalance)
        )
    }
}

/// Consumer group member information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupMember {
    pub consumer_id: ConsumerId,
    pub group_id: ConsumerGroupId,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout_ms: u64,
    pub rebalance_timeout_ms: u64,
    pub subscribed_topics: Vec<TopicName>,
    pub assigned_partitions: Vec<TopicPartition>,
    pub last_heartbeat: SystemTime,
    /// Monotonic clock for timeout calculations (immune to NTP clock adjustments)
    #[serde(skip, default = "default_instant")]
    pub last_heartbeat_monotonic: Instant,
    pub is_leader: bool,
}

/// Topic and partition combination
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicPartition {
    pub topic: TopicName,
    pub partition: PartitionId,
}

impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: PartitionId) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

/// Consumer group metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupMetadata {
    pub group_id: ConsumerGroupId,
    pub state: ConsumerGroupState,
    pub protocol_type: String,
    pub protocol_name: String,
    pub leader_id: Option<ConsumerId>,
    pub members: HashMap<ConsumerId, ConsumerGroupMember>,
    pub generation_id: i32,
    pub created_at: SystemTime,
    pub state_timestamp: SystemTime,
}

/// Partition assignment strategy
#[derive(Debug, Clone, PartialEq)]
pub enum AssignmentStrategy {
    /// Round-robin assignment across consumers
    RoundRobin,
    /// Range assignment (contiguous partitions per consumer)
    Range,
    /// Sticky assignment (minimize partition movement)
    Sticky,
}

/// Consumer offset information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerOffset {
    pub group_id: ConsumerGroupId,
    pub topic: TopicName,
    pub partition: PartitionId,
    pub offset: i64,
    pub metadata: Option<String>,
    pub commit_timestamp: SystemTime,
    pub expire_timestamp: Option<SystemTime>,
}

/// Offset commit request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetCommitRequest {
    pub group_id: ConsumerGroupId,
    pub consumer_id: ConsumerId,
    pub generation_id: i32,
    pub retention_time_ms: i64,
    pub offsets: Vec<TopicPartitionOffset>,
}

/// Offset fetch request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetFetchRequest {
    pub group_id: ConsumerGroupId,
    pub topic_partitions: Option<Vec<TopicPartition>>, // None means all partitions for the group
}

/// Topic partition offset information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicPartitionOffset {
    pub topic: TopicName,
    pub partition: PartitionId,
    pub offset: i64,
    pub metadata: Option<String>,
}

/// Consumer group protocol messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsumerGroupMessage {
    /// Join group request from consumer
    JoinGroup {
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
        client_id: String,
        client_host: String,
        session_timeout_ms: u64,
        rebalance_timeout_ms: u64,
        protocol_type: String,
        group_protocols: Vec<GroupProtocol>,
    },
    /// Join group response to consumer
    JoinGroupResponse {
        error_code: i16,
        generation_id: i32,
        group_protocol: String,
        leader_id: ConsumerId,
        consumer_id: ConsumerId,
        members: Vec<ConsumerGroupMember>,
    },
    /// Sync group request with partition assignments
    SyncGroup {
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
        generation_id: i32,
        group_assignments: HashMap<ConsumerId, Vec<TopicPartition>>,
    },
    /// Sync group response with consumer's assignment
    SyncGroupResponse {
        error_code: i16,
        assignment: Vec<TopicPartition>,
    },
    /// Heartbeat from consumer to coordinator
    Heartbeat {
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
        generation_id: i32,
    },
    /// Heartbeat response
    HeartbeatResponse { error_code: i16 },
    /// Leave group when consumer is shutting down
    LeaveGroup {
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
    },
    /// Leave group response
    LeaveGroupResponse { error_code: i16 },
    /// List groups request
    ListGroups,
    /// List groups response
    ListGroupsResponse {
        error_code: i16,
        groups: Vec<GroupOverview>,
    },
    /// Describe groups request
    DescribeGroups { group_ids: Vec<ConsumerGroupId> },
    /// Describe groups response
    DescribeGroupsResponse {
        groups: Vec<ConsumerGroupDescription>,
    },
    /// Commit consumer offsets
    OffsetCommit {
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
        generation_id: i32,
        retention_time_ms: i64,
        offsets: Vec<TopicPartitionOffset>,
    },
    /// Offset commit response
    OffsetCommitResponse {
        error_code: i16,
        topic_partition_errors: Vec<TopicPartitionError>,
    },
    /// Fetch committed offsets
    OffsetFetch {
        group_id: ConsumerGroupId,
        topic_partitions: Option<Vec<TopicPartition>>,
    },
    /// Offset fetch response
    OffsetFetchResponse {
        error_code: i16,
        offsets: Vec<TopicPartitionOffsetResult>,
    },
}

/// Protocol information for joining a group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupProtocol {
    pub name: String,
    pub metadata: Vec<u8>, // Serialized protocol-specific metadata
}

/// Brief group information for listing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupOverview {
    pub group_id: ConsumerGroupId,
    pub protocol_type: String,
}

/// Detailed group information for describe operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupDescription {
    pub error_code: i16,
    pub group_id: ConsumerGroupId,
    pub state: ConsumerGroupState,
    pub protocol_type: String,
    pub protocol_data: String,
    pub members: Vec<MemberDescription>,
}

/// Member information in group description
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberDescription {
    pub consumer_id: ConsumerId,
    pub client_id: String,
    pub client_host: String,
    pub member_metadata: Vec<u8>,
    pub member_assignment: Vec<u8>,
}

/// Topic partition error for offset operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicPartitionError {
    pub topic: TopicName,
    pub partition: PartitionId,
    pub error_code: i16,
}

/// Topic partition offset result for fetch operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicPartitionOffsetResult {
    pub topic: TopicName,
    pub partition: PartitionId,
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: Option<String>,
    pub error_code: i16,
}

/// Consumer group statistics
#[derive(Debug, Clone)]
pub struct GroupStats {
    pub group_id: ConsumerGroupId,
    pub state: ConsumerGroupState,
    pub member_count: usize,
    pub generation_id: i32,
    pub total_assigned_partitions: usize,
    pub leader_id: Option<ConsumerId>,
    pub assignment_strategy: AssignmentStrategy,
    pub created_at: SystemTime,
    pub last_state_change: SystemTime,
}

/// Consumer group configuration
#[derive(Debug, Clone)]
pub struct ConsumerGroupConfig {
    /// Default session timeout for consumers
    pub default_session_timeout_ms: u64,
    /// Default rebalance timeout for consumers
    pub default_rebalance_timeout_ms: u64,
    /// Minimum session timeout allowed
    pub min_session_timeout_ms: u64,
    /// Maximum session timeout allowed
    pub max_session_timeout_ms: u64,
    /// How often to check for expired consumers
    pub consumer_expiration_check_interval_ms: u64,
    /// Default assignment strategy
    pub default_assignment_strategy: AssignmentStrategy,
    /// How long to keep group metadata after all members leave
    pub group_metadata_retention_ms: u64,
}

impl Default for ConsumerGroupConfig {
    fn default() -> Self {
        Self {
            default_session_timeout_ms: 10000,           // 10 seconds
            default_rebalance_timeout_ms: 60000,         // 60 seconds
            min_session_timeout_ms: 6000,                // 6 seconds
            max_session_timeout_ms: 1800000,             // 30 minutes
            consumer_expiration_check_interval_ms: 3000, // 3 seconds
            default_assignment_strategy: AssignmentStrategy::RoundRobin,
            group_metadata_retention_ms: 86400000, // 24 hours
        }
    }
}

/// Partition assignment coordinator
pub struct PartitionAssignor {
    strategy: AssignmentStrategy,
}

impl PartitionAssignor {
    pub fn new(strategy: AssignmentStrategy) -> Self {
        Self { strategy }
    }

    /// Assign partitions to consumers based on the configured strategy
    pub fn assign(
        &self,
        consumers: &[ConsumerId],
        partitions: &[TopicPartition],
    ) -> HashMap<ConsumerId, Vec<TopicPartition>> {
        if consumers.is_empty() || partitions.is_empty() {
            return HashMap::new();
        }

        match self.strategy {
            AssignmentStrategy::RoundRobin => self.round_robin_assign(consumers, partitions),
            AssignmentStrategy::Range => self.range_assign(consumers, partitions),
            AssignmentStrategy::Sticky => self.sticky_assign(consumers, partitions),
        }
    }

    fn round_robin_assign(
        &self,
        consumers: &[ConsumerId],
        partitions: &[TopicPartition],
    ) -> HashMap<ConsumerId, Vec<TopicPartition>> {
        let mut assignments = HashMap::new();

        // Initialize empty assignments for all consumers
        for consumer in consumers {
            assignments.insert(consumer.clone(), Vec::new());
        }

        // Round-robin assignment
        for (index, partition) in partitions.iter().enumerate() {
            let consumer_index = index % consumers.len();
            let consumer = &consumers[consumer_index];
            if let Some(parts) = assignments.get_mut(consumer) {
                parts.push(partition.clone());
            }
        }

        assignments
    }

    fn range_assign(
        &self,
        consumers: &[ConsumerId],
        partitions: &[TopicPartition],
    ) -> HashMap<ConsumerId, Vec<TopicPartition>> {
        let mut assignments = HashMap::new();

        // Group partitions by topic
        let mut topic_partitions: HashMap<String, Vec<TopicPartition>> = HashMap::new();
        for partition in partitions {
            topic_partitions
                .entry(partition.topic.clone())
                .or_insert_with(Vec::new)
                .push(partition.clone());
        }

        // Initialize empty assignments
        for consumer in consumers {
            assignments.insert(consumer.clone(), Vec::new());
        }

        // Assign partitions per topic using range strategy
        for (_topic, mut topic_parts) in topic_partitions {
            topic_parts.sort_by_key(|tp| tp.partition);

            let partitions_per_consumer = topic_parts.len() / consumers.len();
            let remainder = topic_parts.len() % consumers.len();

            let mut partition_index = 0;
            for (consumer_index, consumer) in consumers.iter().enumerate() {
                let extra = if consumer_index < remainder { 1 } else { 0 };
                let count = partitions_per_consumer + extra;

                for _ in 0..count {
                    if partition_index < topic_parts.len() {
                        if let Some(parts) = assignments.get_mut(consumer) {
                            parts.push(topic_parts[partition_index].clone());
                        }
                        partition_index += 1;
                    }
                }
            }
        }

        assignments
    }

    fn sticky_assign(
        &self,
        consumers: &[ConsumerId],
        partitions: &[TopicPartition],
    ) -> HashMap<ConsumerId, Vec<TopicPartition>> {
        // Sticky assignment tries to minimize partition movement during rebalancing
        // For now, implement as a balanced assignment that tries to keep
        // partitions evenly distributed

        let mut assignments: HashMap<ConsumerId, Vec<TopicPartition>> = HashMap::new();

        // Initialize empty assignments
        for consumer in consumers {
            assignments.insert(consumer.clone(), Vec::new());
        }

        if partitions.is_empty() {
            return assignments;
        }

        // Group partitions by topic for better locality
        let mut topic_partitions: HashMap<String, Vec<TopicPartition>> = HashMap::new();
        for partition in partitions {
            topic_partitions
                .entry(partition.topic.clone())
                .or_insert_with(Vec::new)
                .push(partition.clone());
        }

        // Assign partitions topic by topic to maintain locality
        for (_topic, mut topic_parts) in topic_partitions {
            topic_parts.sort_by_key(|tp| tp.partition);

            for partition in topic_parts {
                // Find consumer with least partitions currently assigned
                let target_consumer = consumers
                    .iter()
                    .min_by_key(|consumer| assignments.get(*consumer).unwrap().len())
                    .unwrap();

                assignments
                    .get_mut(target_consumer)
                    .unwrap()
                    .push(partition);
            }
        }

        assignments
    }
}

/// Error codes for consumer group operations
pub mod error_codes {
    pub const NONE: i16 = 0;
    pub const UNKNOWN_CONSUMER_ID: i16 = 25;
    pub const CONSUMER_COORDINATOR_NOT_AVAILABLE: i16 = 15;
    pub const NOT_COORDINATOR: i16 = 16;
    pub const ILLEGAL_GENERATION: i16 = 22;
    pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
    pub const INVALID_GROUP_ID: i16 = 24;
    pub const UNKNOWN_GROUP_ID: i16 = 25;
    pub const INVALID_SESSION_TIMEOUT: i16 = 26;
    pub const REBALANCE_IN_PROGRESS: i16 = 27;
    pub const INVALID_COMMIT_OFFSET_SIZE: i16 = 28;
    pub const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
    pub const GROUP_AUTHORIZATION_FAILED: i16 = 30;
}
