pub mod follower;
pub mod leader;

#[cfg(test)]
mod tests;

use crate::protocol::{Message, Offset, PartitionId, TopicName};
use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

pub use follower::{FollowerState, FollowerSync};
pub use leader::{LeaderState, ReplicationManager};

/// Unique identifier for broker nodes
pub type BrokerId = u32;

/// Replication state for a partition
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationRole {
    Leader,
    Follower { leader_id: BrokerId },
    Offline,
}

/// Metadata for partition replication
#[derive(Debug, Clone)]
pub struct PartitionReplicaInfo {
    pub topic: TopicName,
    pub partition: PartitionId,
    pub role: ReplicationRole,
    pub high_watermark: Offset,
    pub log_end_offset: Offset,
    pub replicas: Vec<BrokerId>,
    pub in_sync_replicas: Vec<BrokerId>,
}

/// Replication request/response messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationMessage {
    /// Request to replicate log entries from leader to follower
    ReplicateRequest {
        topic: TopicName,
        partition: PartitionId,
        leader_epoch: u64,
        prev_log_offset: Offset,
        entries: Vec<LogEntry>,
        leader_commit: Offset,
    },
    /// Response from follower after processing replication request
    ReplicateResponse {
        topic: TopicName,
        partition: PartitionId,
        success: bool,
        last_log_offset: Offset,
        follower_id: BrokerId,
    },
    /// Heartbeat from leader to maintain leadership
    Heartbeat {
        leader_id: BrokerId,
        term: u64,
        commit_index: Offset,
    },
    /// Response to heartbeat
    HeartbeatResponse {
        follower_id: BrokerId,
        term: u64,
        success: bool,
    },
}

/// Log entry for replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub offset: Offset,
    pub term: u64,
    pub message: Message,
    pub timestamp: u64,
}

/// Configuration for replication
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Minimum number of in-sync replicas required
    pub min_isr: u32,
    /// Replication factor (total number of replicas)
    pub replication_factor: u32,
    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u64,
    /// Timeout for replication requests
    pub replication_timeout_ms: u64,
    /// Maximum batch size for replication
    pub max_batch_size: usize,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            min_isr: 1,
            replication_factor: 3,
            heartbeat_interval_ms: 1000,
            replication_timeout_ms: 5000,
            max_batch_size: 1000,
        }
    }
}

/// Central coordinator for replication across all partitions
#[derive(Debug)]
pub struct ReplicationCoordinator {
    broker_id: BrokerId,
    config: ReplicationConfig,
    partition_states: Arc<RwLock<HashMap<(TopicName, PartitionId), PartitionReplicaInfo>>>,
    leaders: Arc<RwLock<HashMap<(TopicName, PartitionId), Arc<LeaderState>>>>,
    followers: Arc<RwLock<HashMap<(TopicName, PartitionId), Arc<FollowerState>>>>,
}

impl ReplicationCoordinator {
    pub fn new(broker_id: BrokerId, config: ReplicationConfig) -> Self {
        Self {
            broker_id,
            config,
            partition_states: Arc::new(RwLock::new(HashMap::new())),
            leaders: Arc::new(RwLock::new(HashMap::new())),
            followers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Initialize replication for a partition as leader
    pub async fn become_leader(
        &self,
        topic: &str,
        partition: PartitionId,
        replicas: Vec<BrokerId>,
    ) -> Result<()> {
        let key = (topic.to_string(), partition);

        let replica_info = PartitionReplicaInfo {
            topic: topic.to_string(),
            partition,
            role: ReplicationRole::Leader,
            high_watermark: 0,
            log_end_offset: 0,
            replicas: replicas.clone(),
            in_sync_replicas: vec![self.broker_id],
        };

        {
            let mut states = self.partition_states.write().await;
            states.insert(key.clone(), replica_info);
        }

        let leader_state = Arc::new(LeaderState::new(
            self.broker_id,
            topic.to_string(),
            partition,
            replicas,
            self.config.clone(),
        ));

        {
            let mut leaders = self.leaders.write().await;
            leaders.insert(key, leader_state);
        }

        info!("Became leader for {}:{}", topic, partition);
        Ok(())
    }

    /// Initialize replication for a partition as follower
    pub async fn become_follower(
        &self,
        topic: &str,
        partition: PartitionId,
        leader_id: BrokerId,
    ) -> Result<()> {
        let key = (topic.to_string(), partition);

        let replica_info = PartitionReplicaInfo {
            topic: topic.to_string(),
            partition,
            role: ReplicationRole::Follower { leader_id },
            high_watermark: 0,
            log_end_offset: 0,
            replicas: vec![leader_id, self.broker_id],
            in_sync_replicas: vec![leader_id],
        };

        {
            let mut states = self.partition_states.write().await;
            states.insert(key.clone(), replica_info);
        }

        let follower_state = Arc::new(FollowerState::new(
            self.broker_id,
            topic.to_string(),
            partition,
            leader_id,
            self.config.clone(),
        ));

        {
            let mut followers = self.followers.write().await;
            followers.insert(key, follower_state);
        }

        info!(
            "Became follower for {}:{} with leader {}",
            topic, partition, leader_id
        );
        Ok(())
    }

    /// Get replication state for a partition
    pub async fn get_partition_state(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Option<PartitionReplicaInfo> {
        let states = self.partition_states.read().await;
        states.get(&(topic.to_string(), partition)).cloned()
    }

    /// Check if this broker is the leader for a partition
    pub async fn is_leader(&self, topic: &str, partition: PartitionId) -> bool {
        if let Some(state) = self.get_partition_state(topic, partition).await {
            matches!(state.role, ReplicationRole::Leader)
        } else {
            false
        }
    }

    /// Get leader broker ID for a partition
    pub async fn get_leader(&self, topic: &str, partition: PartitionId) -> Option<BrokerId> {
        if let Some(state) = self.get_partition_state(topic, partition).await {
            match state.role {
                ReplicationRole::Leader => Some(self.broker_id),
                ReplicationRole::Follower { leader_id } => Some(leader_id),
                ReplicationRole::Offline => None,
            }
        } else {
            None
        }
    }
}
