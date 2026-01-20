use super::*;
use crate::storage::HybridStorage;
use crate::topic_manager::TopicManager;
use crate::Result;
use crossbeam::channel;
use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Consumer group coordinator managing group membership and partition assignments
#[derive(Debug)]
pub struct ConsumerGroupCoordinator {
    /// Configuration for consumer groups
    config: ConsumerGroupConfig,
    /// Active consumer groups (🚀 LOCK-FREE: DashMap for concurrent access)
    groups: Arc<DashMap<ConsumerGroupId, ConsumerGroupMetadata>>,
    /// Topic manager for partition information
    topic_manager: Arc<TopicManager>,
    /// Storage for offset commits (future use)
    #[allow(dead_code)]
    storage: Option<Arc<HybridStorage>>,
    /// In-memory offset storage for fast access (🚀 LOCK-FREE: DashMap for concurrent access)
    offset_storage: Arc<DashMap<(ConsumerGroupId, TopicName, PartitionId), ConsumerOffset>>,
    /// Channel for group state change notifications
    state_change_tx: channel::Sender<GroupStateChange>,
    state_change_rx: Arc<tokio::sync::Mutex<Option<channel::Receiver<GroupStateChange>>>>,
    /// Directory for persisting group metadata
    metadata_dir: Option<PathBuf>,
}

/// Internal group state change notifications
#[derive(Debug, Clone)]
enum GroupStateChange {
    MemberJoined {
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
    },
    MemberLeft {
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
    },
    RebalanceTriggered {
        group_id: ConsumerGroupId,
    },
    GroupEmpty {
        group_id: ConsumerGroupId,
    },
}

/// Serializable version of ConsumerGroupMetadata that handles SystemTime
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializableGroupMetadata {
    pub group_id: ConsumerGroupId,
    pub state: ConsumerGroupState,
    pub protocol_type: String,
    pub protocol_name: String,
    pub leader_id: Option<ConsumerId>,
    pub members: HashMap<ConsumerId, SerializableGroupMember>,
    pub generation_id: i32,
    pub created_at: u64,      // Unix timestamp in seconds
    pub state_timestamp: u64, // Unix timestamp in seconds
}

/// Serializable version of ConsumerGroupMember that handles SystemTime
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializableGroupMember {
    pub consumer_id: ConsumerId,
    pub group_id: ConsumerGroupId,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout_ms: u64,
    pub rebalance_timeout_ms: u64,
    pub subscribed_topics: Vec<TopicName>,
    pub assigned_partitions: Vec<TopicPartition>,
    pub last_heartbeat: u64, // Unix timestamp in seconds
    pub is_leader: bool,
}

/// Serializable version of ConsumerOffset that handles SystemTime
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializableConsumerOffset {
    pub group_id: ConsumerGroupId,
    pub topic: TopicName,
    pub partition: PartitionId,
    pub offset: i64,
    pub metadata: Option<String>,
    pub commit_timestamp: u64,         // Unix timestamp in seconds
    pub expire_timestamp: Option<u64>, // Unix timestamp in seconds
}

impl From<&ConsumerOffset> for SerializableConsumerOffset {
    fn from(offset: &ConsumerOffset) -> Self {
        Self {
            group_id: offset.group_id.clone(),
            topic: offset.topic.clone(),
            partition: offset.partition,
            offset: offset.offset,
            metadata: offset.metadata.clone(),
            commit_timestamp: offset
                .commit_timestamp
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            expire_timestamp: offset.expire_timestamp.map(|ts| {
                ts.duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            }),
        }
    }
}

impl From<SerializableConsumerOffset> for ConsumerOffset {
    fn from(offset: SerializableConsumerOffset) -> Self {
        Self {
            group_id: offset.group_id,
            topic: offset.topic,
            partition: offset.partition,
            offset: offset.offset,
            metadata: offset.metadata,
            commit_timestamp: SystemTime::UNIX_EPOCH + Duration::from_secs(offset.commit_timestamp),
            expire_timestamp: offset
                .expire_timestamp
                .map(|ts| SystemTime::UNIX_EPOCH + Duration::from_secs(ts)),
        }
    }
}

impl ConsumerGroupCoordinator {
    /// Create a new consumer group coordinator
    pub fn new(
        config: ConsumerGroupConfig,
        topic_manager: Arc<TopicManager>,
        storage: Option<Arc<HybridStorage>>,
        metadata_dir: Option<PathBuf>,
    ) -> Self {
        // Bounded channel to prevent memory leak during rapid state changes
        // 1000 entries is sufficient for normal rebalancing bursts
        let (state_change_tx, state_change_rx) = channel::bounded(1_000);

        Self {
            config,
            groups: Arc::new(DashMap::new()), // 🚀 LOCK-FREE: DashMap initialization
            topic_manager,
            storage,
            offset_storage: Arc::new(DashMap::new()), // 🚀 LOCK-FREE: DashMap initialization
            state_change_tx,
            state_change_rx: Arc::new(tokio::sync::Mutex::new(Some(state_change_rx))),
            metadata_dir,
        }
    }

    /// Get metadata persistence file path for a group
    fn get_metadata_file_path(&self, group_id: &ConsumerGroupId) -> Option<PathBuf> {
        self.metadata_dir
            .as_ref()
            .map(|dir| dir.join(format!("group_{}.json", group_id)))
    }

    /// Get metadata directory path
    fn get_metadata_dir(&self) -> Option<&Path> {
        self.metadata_dir.as_deref()
    }

    /// Start the coordinator background tasks
    pub async fn start(self: Arc<Self>) -> Result<()> {
        // Load persisted group metadata if available
        if let Err(e) = self.load_persisted_metadata().await {
            warn!("Failed to load persisted group metadata: {}", e);
        }

        // Start expiration checker
        let expiry_coordinator = Arc::clone(&self);
        tokio::spawn(async move {
            expiry_coordinator.expiration_checker_loop().await;
        });

        // Start state change processor
        let state_coordinator = Arc::clone(&self);
        tokio::spawn(async move {
            state_coordinator.process_state_changes().await;
        });

        // Start offset cleanup task
        let cleanup_coordinator = Arc::clone(&self);
        tokio::spawn(async move {
            cleanup_coordinator.offset_cleanup_loop().await;
        });

        // Start metadata persistence task
        let persistence_coordinator = Arc::clone(&self);
        tokio::spawn(async move {
            persistence_coordinator.metadata_persistence_loop().await;
        });

        info!("Consumer group coordinator started with persistence");
        Ok(())
    }

    /// Handle consumer group messages
    pub async fn handle_message(
        &self,
        request: ConsumerGroupMessage,
    ) -> Result<ConsumerGroupMessage> {
        match request {
            ConsumerGroupMessage::JoinGroup { .. } => Ok(self.handle_join_group(request).await),
            ConsumerGroupMessage::SyncGroup { .. } => Ok(self.handle_sync_group(request).await),
            ConsumerGroupMessage::Heartbeat { .. } => Ok(self.handle_heartbeat(request).await),
            ConsumerGroupMessage::LeaveGroup { .. } => Ok(self.handle_leave_group(request).await),
            ConsumerGroupMessage::ListGroups => Ok(self.handle_list_groups().await),
            ConsumerGroupMessage::DescribeGroups { .. } => {
                Ok(self.handle_describe_groups(request).await)
            }
            ConsumerGroupMessage::OffsetCommit { .. } => {
                Ok(self.handle_offset_commit(request).await)
            }
            ConsumerGroupMessage::OffsetFetch { .. } => Ok(self.handle_offset_fetch(request).await),
            _ => {
                // Return error for unsupported messages
                Ok(ConsumerGroupMessage::JoinGroupResponse {
                    error_code: error_codes::UNKNOWN_CONSUMER_ID,
                    generation_id: -1,
                    group_protocol: "".to_string(),
                    leader_id: "".to_string(),
                    consumer_id: "".to_string(),
                    members: vec![],
                })
            }
        }
    }

    /// Handle join group request  
    pub async fn handle_join_group(&self, request: ConsumerGroupMessage) -> ConsumerGroupMessage {
        if let ConsumerGroupMessage::JoinGroup {
            group_id,
            consumer_id,
            client_id,
            client_host,
            session_timeout_ms,
            rebalance_timeout_ms,
            protocol_type,
            group_protocols,
        } = request
        {
            // Validate session timeout
            if session_timeout_ms < self.config.min_session_timeout_ms
                || session_timeout_ms > self.config.max_session_timeout_ms
            {
                return ConsumerGroupMessage::JoinGroupResponse {
                    error_code: error_codes::INVALID_SESSION_TIMEOUT,
                    generation_id: -1,
                    group_protocol: String::new(),
                    leader_id: String::new(),
                    consumer_id: consumer_id.clone(),
                    members: Vec::new(),
                };
            }

            let result = self
                .join_group_internal(
                    group_id.clone(),
                    consumer_id.clone(),
                    client_id,
                    client_host,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    protocol_type,
                    group_protocols,
                )
                .await;

            match result {
                Ok((generation_id, group_protocol, leader_id, assigned_member_id, members)) => {
                    ConsumerGroupMessage::JoinGroupResponse {
                        error_code: error_codes::NONE,
                        generation_id,
                        group_protocol,
                        leader_id,
                        consumer_id: assigned_member_id, // Use the server-assigned member_id
                        members,
                    }
                }
                Err(error_code) => ConsumerGroupMessage::JoinGroupResponse {
                    error_code,
                    generation_id: -1,
                    group_protocol: String::new(),
                    leader_id: String::new(),
                    consumer_id, // Keep original consumer_id on error
                    members: Vec::new(),
                },
            }
        } else {
            ConsumerGroupMessage::JoinGroupResponse {
                error_code: error_codes::INVALID_GROUP_ID,
                generation_id: -1,
                group_protocol: String::new(),
                leader_id: String::new(),
                consumer_id: String::new(),
                members: Vec::new(),
            }
        }
    }

    /// Handle sync group request
    pub async fn handle_sync_group(&self, request: ConsumerGroupMessage) -> ConsumerGroupMessage {
        if let ConsumerGroupMessage::SyncGroup {
            group_id,
            consumer_id,
            generation_id,
            group_assignments,
        } = request
        {
            let result = self
                .sync_group_internal(
                    group_id,
                    consumer_id.clone(),
                    generation_id,
                    group_assignments,
                )
                .await;

            match result {
                Ok(assignment) => ConsumerGroupMessage::SyncGroupResponse {
                    error_code: error_codes::NONE,
                    assignment,
                },
                Err(error_code) => ConsumerGroupMessage::SyncGroupResponse {
                    error_code,
                    assignment: Vec::new(),
                },
            }
        } else {
            ConsumerGroupMessage::SyncGroupResponse {
                error_code: error_codes::INVALID_GROUP_ID,
                assignment: Vec::new(),
            }
        }
    }

    /// Handle heartbeat request
    pub async fn handle_heartbeat(&self, request: ConsumerGroupMessage) -> ConsumerGroupMessage {
        if let ConsumerGroupMessage::Heartbeat {
            group_id,
            consumer_id,
            generation_id,
        } = request
        {
            let error_code = self
                .heartbeat_internal(group_id, consumer_id, generation_id)
                .await;

            ConsumerGroupMessage::HeartbeatResponse { error_code }
        } else {
            ConsumerGroupMessage::HeartbeatResponse {
                error_code: error_codes::INVALID_GROUP_ID,
            }
        }
    }

    /// Handle leave group request
    pub async fn handle_leave_group(&self, request: ConsumerGroupMessage) -> ConsumerGroupMessage {
        if let ConsumerGroupMessage::LeaveGroup {
            group_id,
            consumer_id,
        } = request
        {
            let error_code = self.leave_group_internal(group_id, consumer_id).await;
            ConsumerGroupMessage::LeaveGroupResponse { error_code }
        } else {
            ConsumerGroupMessage::LeaveGroupResponse {
                error_code: error_codes::INVALID_GROUP_ID,
            }
        }
    }

    /// Handle list groups request
    pub async fn handle_list_groups(&self) -> ConsumerGroupMessage {
        let group_overviews: Vec<GroupOverview> = self
            .groups
            .iter()
            .filter(|entry| {
                // Only include non-dead groups
                !matches!(entry.value().state, ConsumerGroupState::Dead)
            })
            .map(|entry| GroupOverview {
                group_id: entry.value().group_id.clone(),
                protocol_type: entry.value().protocol_type.clone(),
            })
            .collect();

        debug!("Listed {} active consumer groups", group_overviews.len());

        ConsumerGroupMessage::ListGroupsResponse {
            error_code: error_codes::NONE,
            groups: group_overviews,
        }
    }

    /// Handle list groups request with state filtering
    pub async fn handle_list_groups_filtered(
        &self,
        states: Option<Vec<ConsumerGroupState>>,
    ) -> ConsumerGroupMessage {
        let group_overviews: Vec<GroupOverview> = self
            .groups
            .iter()
            .filter(|entry| match &states {
                Some(filter_states) => filter_states.contains(&entry.value().state),
                None => !matches!(entry.value().state, ConsumerGroupState::Dead),
            })
            .map(|entry| GroupOverview {
                group_id: entry.value().group_id.clone(),
                protocol_type: entry.value().protocol_type.clone(),
            })
            .collect();

        debug!(
            "Listed {} filtered consumer groups (filter: {:?})",
            group_overviews.len(),
            states
        );

        ConsumerGroupMessage::ListGroupsResponse {
            error_code: error_codes::NONE,
            groups: group_overviews,
        }
    }

    /// Handle describe groups request
    pub async fn handle_describe_groups(
        &self,
        request: ConsumerGroupMessage,
    ) -> ConsumerGroupMessage {
        if let ConsumerGroupMessage::DescribeGroups { group_ids } = request {
            let mut descriptions = Vec::new();

            for group_id in group_ids {
                if let Some(metadata) = self.groups.get(&group_id) {
                    let member_descriptions: Vec<MemberDescription> = metadata
                        .value()
                        .members
                        .values()
                        .map(|member| {
                            // Serialize member metadata (topics subscribed)
                            let member_metadata =
                                self.serialize_member_metadata(&member.subscribed_topics);

                            // Serialize member assignment (partitions assigned)
                            let member_assignment =
                                self.serialize_member_assignment(&member.assigned_partitions);

                            MemberDescription {
                                consumer_id: member.consumer_id.clone(),
                                client_id: member.client_id.clone(),
                                client_host: member.client_host.clone(),
                                member_metadata,
                                member_assignment,
                            }
                        })
                        .collect();

                    // Calculate additional group statistics
                    let total_partitions: usize = metadata
                        .value()
                        .members
                        .values()
                        .map(|m| m.assigned_partitions.len())
                        .sum();

                    debug!(
                        "Describing group {}: state={:?}, members={}, partitions={}",
                        group_id,
                        metadata.value().state,
                        metadata.value().members.len(),
                        total_partitions
                    );

                    descriptions.push(ConsumerGroupDescription {
                        error_code: error_codes::NONE,
                        group_id: group_id.clone(),
                        state: metadata.value().state.clone(),
                        protocol_type: metadata.value().protocol_type.clone(),
                        protocol_data: metadata.value().protocol_name.clone(),
                        members: member_descriptions,
                    });
                } else {
                    warn!("Requested description for unknown group: {}", group_id);
                    descriptions.push(ConsumerGroupDescription {
                        error_code: error_codes::UNKNOWN_GROUP_ID,
                        group_id: group_id.clone(),
                        state: ConsumerGroupState::Dead,
                        protocol_type: String::new(),
                        protocol_data: String::new(),
                        members: Vec::new(),
                    });
                }
            }

            ConsumerGroupMessage::DescribeGroupsResponse {
                groups: descriptions,
            }
        } else {
            ConsumerGroupMessage::DescribeGroupsResponse { groups: Vec::new() }
        }
    }

    /// Handle offset commit request
    pub async fn handle_offset_commit(
        &self,
        request: ConsumerGroupMessage,
    ) -> ConsumerGroupMessage {
        if let ConsumerGroupMessage::OffsetCommit {
            group_id,
            consumer_id,
            generation_id,
            retention_time_ms,
            offsets,
        } = request
        {
            let result = self
                .commit_offsets_internal(
                    group_id,
                    consumer_id,
                    generation_id,
                    retention_time_ms,
                    offsets,
                )
                .await;

            match result {
                Ok(errors) => ConsumerGroupMessage::OffsetCommitResponse {
                    error_code: error_codes::NONE,
                    topic_partition_errors: errors,
                },
                Err(global_error) => ConsumerGroupMessage::OffsetCommitResponse {
                    error_code: global_error,
                    topic_partition_errors: Vec::new(),
                },
            }
        } else {
            ConsumerGroupMessage::OffsetCommitResponse {
                error_code: error_codes::INVALID_GROUP_ID,
                topic_partition_errors: Vec::new(),
            }
        }
    }

    /// Handle offset fetch request
    pub async fn handle_offset_fetch(&self, request: ConsumerGroupMessage) -> ConsumerGroupMessage {
        if let ConsumerGroupMessage::OffsetFetch {
            group_id,
            topic_partitions,
        } = request
        {
            let result = self
                .fetch_offsets_internal(group_id, topic_partitions)
                .await;

            match result {
                Ok(offsets) => ConsumerGroupMessage::OffsetFetchResponse {
                    error_code: error_codes::NONE,
                    offsets,
                },
                Err(error_code) => ConsumerGroupMessage::OffsetFetchResponse {
                    error_code,
                    offsets: Vec::new(),
                },
            }
        } else {
            ConsumerGroupMessage::OffsetFetchResponse {
                error_code: error_codes::INVALID_GROUP_ID,
                offsets: Vec::new(),
            }
        }
    }

    // Internal implementation methods

    async fn join_group_internal(
        &self,
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
        client_id: String,
        client_host: String,
        session_timeout_ms: u64,
        rebalance_timeout_ms: u64,
        protocol_type: String,
        group_protocols: Vec<GroupProtocol>,
    ) -> std::result::Result<
        (
            i32,
            String,
            ConsumerId,
            ConsumerId,
            Vec<ConsumerGroupMember>,
        ),
        i16,
    > {
        // Return: (generation_id, protocol_name, leader_id, assigned_member_id, members)
        let now = SystemTime::now();

        // Generate a new member_id if the client sent an empty one (first join)
        // Kafka protocol: client sends empty memberId on first join, server assigns a unique one
        let actual_consumer_id = if consumer_id.is_empty() {
            // Generate unique member_id: <client_id>-<timestamp_nanos>-<random_suffix>
            let timestamp = now
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let random_suffix: u32 = rand::random();
            format!("{}-{}-{:08x}", client_id, timestamp, random_suffix)
        } else {
            consumer_id
        };

        // Get or create group
        let mut group_ref =
            self.groups
                .entry(group_id.clone())
                .or_insert_with(|| ConsumerGroupMetadata {
                    group_id: group_id.clone(),
                    state: ConsumerGroupState::Empty,
                    protocol_type: protocol_type.clone(),
                    protocol_name: self.select_compatible_protocol(&group_protocols),
                    leader_id: None,
                    members: HashMap::new(),
                    generation_id: 0,
                    created_at: now,
                    state_timestamp: now,
                });

        // Check protocol compatibility
        if group_ref.protocol_type != protocol_type && !group_ref.members.is_empty() {
            return Err(error_codes::INCONSISTENT_GROUP_PROTOCOL);
        }

        // Extract subscribed topics from protocol metadata
        let subscribed_topics = self.extract_subscribed_topics(&group_protocols);

        debug!(
            "Extracted {} subscribed topics for consumer {}: {:?}",
            subscribed_topics.len(),
            actual_consumer_id,
            subscribed_topics
        );

        // Add or update member
        let is_new_member = !group_ref.members.contains_key(&actual_consumer_id);
        let member = ConsumerGroupMember {
            consumer_id: actual_consumer_id.clone(),
            group_id: group_id.clone(),
            client_id,
            client_host,
            session_timeout_ms,
            rebalance_timeout_ms,
            subscribed_topics,
            assigned_partitions: Vec::new(),
            last_heartbeat: now,
            is_leader: false,
        };

        group_ref.members.insert(actual_consumer_id.clone(), member);

        // If this is a new member or group was empty, trigger rebalance
        // IMPORTANT: Only increment generation_id when transitioning TO PreparingRebalance,
        // not when already in PreparingRebalance (to ensure all consumers get the same generation)
        if is_new_member || group_ref.state == ConsumerGroupState::Empty {
            // Only increment generation if we're not already in PreparingRebalance state
            // This ensures all consumers joining during a rebalance get the same generation_id
            if group_ref.state != ConsumerGroupState::PreparingRebalance {
                group_ref.generation_id += 1;
            }
            group_ref.state = ConsumerGroupState::PreparingRebalance;
            group_ref.state_timestamp = now;

            // Select leader (first member or existing leader if still present)
            if group_ref.leader_id.is_none()
                || !group_ref
                    .members
                    .contains_key(group_ref.leader_id.as_ref().unwrap())
            {
                group_ref.leader_id = Some(actual_consumer_id.clone());
                if let Some(leader_member) = group_ref.members.get_mut(&actual_consumer_id) {
                    leader_member.is_leader = true;
                }
            }

            // Notify state change
            let _ = self.state_change_tx.send(GroupStateChange::MemberJoined {
                group_id: group_id.clone(),
                consumer_id: actual_consumer_id.clone(),
            });
        }

        // Drop the lock before waiting to allow other members to join
        let generation_id = group_ref.generation_id;
        let protocol_name = group_ref.protocol_name.clone();
        drop(group_ref);

        // Wait briefly for other members to join before responding
        // This allows the coordinator to collect all members before sending JoinGroup responses
        // In production, this should be based on rebalance_timeout_ms, but for simplicity we use a short delay
        let wait_time_ms = std::cmp::min(100, rebalance_timeout_ms / 10);
        tokio::time::sleep(std::time::Duration::from_millis(wait_time_ms as u64)).await;

        // Re-acquire the group to get the latest member list
        let group_ref = self
            .groups
            .get(&group_id)
            .ok_or(error_codes::UNKNOWN_GROUP_ID)?;

        let leader_id = group_ref.leader_id.clone().unwrap_or_default();
        let members: Vec<ConsumerGroupMember> = group_ref.members.values().cloned().collect();

        debug!(
            "JoinGroup returning: group={}, protocol_name={}, generation={}, member_id={}, members_count={}",
            group_id, protocol_name, generation_id, actual_consumer_id, members.len()
        );

        // Return tuple includes the actual_consumer_id that was assigned to this member
        Ok((
            generation_id,
            protocol_name,
            leader_id,
            actual_consumer_id,
            members,
        ))
    }

    async fn sync_group_internal(
        &self,
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
        generation_id: i32,
        group_assignments: HashMap<ConsumerId, Vec<TopicPartition>>,
    ) -> std::result::Result<Vec<TopicPartition>, i16> {
        let mut group_ref = self
            .groups
            .get_mut(&group_id)
            .ok_or(error_codes::UNKNOWN_GROUP_ID)?;

        // Verify generation
        if group_ref.generation_id != generation_id {
            return Err(error_codes::ILLEGAL_GENERATION);
        }

        // Verify member exists
        let member = group_ref
            .members
            .get_mut(&consumer_id)
            .ok_or(error_codes::UNKNOWN_CONSUMER_ID)?;

        // Update member's last heartbeat
        member.last_heartbeat = SystemTime::now();

        // If this request contains assignments (from the leader), apply them to all members
        // Note: In Kafka protocol, only the leader sends non-empty assignments in SyncGroup
        if !group_assignments.is_empty() {
            info!(
                "📝 SyncGroup from consumer '{}': applying {} member assignments to group '{}'",
                consumer_id,
                group_assignments.len(),
                group_id
            );

            // Apply assignments to all members
            for (member_id, assignment) in group_assignments.iter() {
                if let Some(target_member) = group_ref.members.get_mut(member_id) {
                    target_member.assigned_partitions = assignment.clone();
                    info!(
                        "📝 Assigned {} partitions to consumer '{}' in group '{}': {:?}",
                        assignment.len(),
                        member_id,
                        group_id,
                        assignment
                    );
                } else {
                    info!(
                        "📝 Warning: member '{}' not found in group '{}', skipping assignment",
                        member_id, group_id
                    );
                }
            }

            // Transition group to stable state
            group_ref.state = ConsumerGroupState::Stable;
            group_ref.state_timestamp = SystemTime::now();

            info!(
                "📝 Group '{}' transitioned to stable state with {} members",
                group_id,
                group_ref.members.len()
            );
        }

        // Return this consumer's assignment (from the just-applied assignments or previously stored)
        // Re-borrow member after assignments are applied
        let member = group_ref
            .members
            .get(&consumer_id)
            .ok_or(error_codes::UNKNOWN_CONSUMER_ID)?;

        let mut assignment = member.assigned_partitions.clone();
        let mut is_stable = group_ref.state == ConsumerGroupState::Stable;

        // Drop the lock before potentially waiting
        drop(group_ref);

        // If the group is not in Stable state and the consumer has no assignment,
        // wait a bit for the leader's SyncGroup to be processed
        if !is_stable && assignment.is_empty() {
            info!(
                "📝 SyncGroup for consumer '{}': group '{}' not stable yet, waiting for leader's assignment...",
                consumer_id, group_id
            );

            // Wait up to 100ms for the leader to apply assignments
            for _ in 0..10 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;

                // Re-check group state
                if let Some(group_ref) = self.groups.get(&group_id) {
                    if group_ref.state == ConsumerGroupState::Stable {
                        // Group became stable, get the assignment
                        if let Some(member) = group_ref.members.get(&consumer_id) {
                            assignment = member.assigned_partitions.clone();
                            is_stable = true;
                            info!(
                                "📝 SyncGroup for consumer '{}': group became stable, got {} partitions",
                                consumer_id, assignment.len()
                            );
                            break;
                        }
                    }
                }
            }
        }

        // After waiting, if still no assignment, return REBALANCE_IN_PROGRESS
        if !is_stable && assignment.is_empty() {
            info!(
                "📝 SyncGroup response for consumer '{}': group '{}' still not stable after waiting, returning REBALANCE_IN_PROGRESS",
                consumer_id, group_id
            );
            return Err(error_codes::REBALANCE_IN_PROGRESS);
        }

        info!(
            "📝 SyncGroup response for consumer '{}': returning {} partitions",
            consumer_id,
            assignment.len()
        );

        Ok(assignment)
    }

    async fn heartbeat_internal(
        &self,
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
        generation_id: i32,
    ) -> i16 {
        let mut group_ref = match self.groups.get_mut(&group_id) {
            Some(g) => g,
            None => return error_codes::UNKNOWN_GROUP_ID,
        };

        // Verify generation
        if group_ref.generation_id != generation_id {
            return error_codes::ILLEGAL_GENERATION;
        }

        // Update member's last heartbeat
        if let Some(member) = group_ref.members.get_mut(&consumer_id) {
            member.last_heartbeat = SystemTime::now();
            error_codes::NONE
        } else {
            error_codes::UNKNOWN_CONSUMER_ID
        }
    }

    async fn leave_group_internal(
        &self,
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
    ) -> i16 {
        let mut group_ref = match self.groups.get_mut(&group_id) {
            Some(g) => g,
            None => return error_codes::UNKNOWN_GROUP_ID,
        };

        // Remove member
        if group_ref.members.remove(&consumer_id).is_some() {
            // If leader left, select new leader
            if group_ref.leader_id.as_ref() == Some(&consumer_id) {
                group_ref.leader_id = group_ref.members.keys().next().cloned();
                // 🚀 Clone leader_id before mutable borrow to avoid borrow checker conflict
                if let Some(new_leader_id) = group_ref.leader_id.clone() {
                    if let Some(new_leader) = group_ref.members.get_mut(&new_leader_id) {
                        new_leader.is_leader = true;
                    }
                }
            }

            // Update group state
            if group_ref.members.is_empty() {
                group_ref.state = ConsumerGroupState::Empty;
                group_ref.leader_id = None;

                // Notify state change
                let _ = self.state_change_tx.send(GroupStateChange::GroupEmpty {
                    group_id: group_id.clone(),
                });
            } else {
                // Trigger rebalance
                group_ref.state = ConsumerGroupState::PreparingRebalance;
                group_ref.generation_id += 1;

                // Notify state change
                let _ = self
                    .state_change_tx
                    .send(GroupStateChange::RebalanceTriggered {
                        group_id: group_id.clone(),
                    });
            }

            group_ref.state_timestamp = SystemTime::now();

            // Notify member left
            let _ = self.state_change_tx.send(GroupStateChange::MemberLeft {
                group_id,
                consumer_id,
            });

            error_codes::NONE
        } else {
            error_codes::UNKNOWN_CONSUMER_ID
        }
    }

    /// Background task to check for expired consumers
    async fn expiration_checker_loop(&self) {
        let mut interval = interval(Duration::from_millis(
            self.config.consumer_expiration_check_interval_ms,
        ));

        loop {
            interval.tick().await;

            if let Err(e) = self.check_expired_consumers().await {
                error!("Error checking expired consumers: {}", e);
            }
        }
    }

    async fn check_expired_consumers(&self) -> Result<()> {
        let now = SystemTime::now();
        let mut expired_members = Vec::new();

        // First pass: collect expired members
        for entry in self.groups.iter() {
            let group_id = entry.key();
            let group = entry.value();

            for (consumer_id, member) in &group.members {
                let elapsed = now
                    .duration_since(member.last_heartbeat)
                    .unwrap_or(Duration::from_secs(0));

                if elapsed.as_millis() > member.session_timeout_ms as u128 {
                    expired_members.push((group_id.clone(), consumer_id.clone()));
                }
            }
        }

        // Second pass: remove expired members and trigger rebalances
        for (group_id, consumer_id) in expired_members {
            if let Some(mut group_ref) = self.groups.get_mut(&group_id) {
                group_ref.members.remove(&consumer_id);

                // Update group state
                if group_ref.members.is_empty() {
                    group_ref.state = ConsumerGroupState::Empty;
                    group_ref.leader_id = None;
                } else {
                    // Select new leader if needed
                    if group_ref.leader_id.as_ref() == Some(&consumer_id) {
                        group_ref.leader_id = group_ref.members.keys().next().cloned();
                        // 🚀 Clone leader_id before mutable borrow to avoid borrow checker conflict
                        if let Some(new_leader_id) = group_ref.leader_id.clone() {
                            if let Some(new_leader) = group_ref.members.get_mut(&new_leader_id) {
                                new_leader.is_leader = true;
                            }
                        }
                    }

                    // Trigger rebalance
                    group_ref.state = ConsumerGroupState::PreparingRebalance;
                    group_ref.generation_id += 1;
                }

                group_ref.state_timestamp = now;

                warn!(
                    "Removed expired consumer {} from group {}",
                    consumer_id, group_id
                );

                // Notify state changes
                let _ = self.state_change_tx.send(GroupStateChange::MemberLeft {
                    group_id: group_id.clone(),
                    consumer_id,
                });

                if group_ref.members.is_empty() {
                    let _ = self
                        .state_change_tx
                        .send(GroupStateChange::GroupEmpty { group_id });
                } else {
                    let _ = self
                        .state_change_tx
                        .send(GroupStateChange::RebalanceTriggered { group_id });
                }
            }
        }

        Ok(())
    }

    /// Background task to process state changes
    async fn process_state_changes(&self) {
        let state_change_rx = {
            let mut rx_guard = self.state_change_rx.lock().await;
            if let Some(rx) = rx_guard.take() {
                rx
            } else {
                return;
            }
        };

        loop {
            // Use spawn_blocking to handle crossbeam channel
            let result = tokio::task::spawn_blocking({
                let rx_clone = state_change_rx.clone();
                move || rx_clone.recv()
            })
            .await;

            match result {
                Ok(Ok(change)) => match change {
                    GroupStateChange::MemberJoined {
                        group_id,
                        consumer_id,
                    } => {
                        debug!("Consumer {} joined group {}", consumer_id, group_id);
                    }
                    GroupStateChange::MemberLeft {
                        group_id,
                        consumer_id,
                    } => {
                        debug!("Consumer {} left group {}", consumer_id, group_id);
                    }
                    GroupStateChange::RebalanceTriggered { group_id } => {
                        debug!("Rebalance triggered for group {}", group_id);
                    }
                    GroupStateChange::GroupEmpty { group_id } => {
                        debug!("Group {} is now empty", group_id);
                    }
                },
                Ok(Err(_)) => break, // Channel closed
                Err(_) => break,     // spawn_blocking failed
            }
        }
    }

    /// Get partition assignment for a group using the configured strategy
    pub async fn assign_partitions(
        &self,
        group_id: &ConsumerGroupId,
        topics: &[TopicName],
    ) -> Result<HashMap<ConsumerId, Vec<TopicPartition>>> {
        let group = self
            .groups
            .get(group_id)
            .ok_or_else(|| crate::FluxmqError::Config(format!("Unknown group: {}", group_id)))?;

        // Collect all partitions for subscribed topics
        let mut all_partitions = Vec::new();
        for topic in topics {
            let partitions = self.topic_manager.get_partitions(topic);
            for partition_id in partitions {
                all_partitions.push(TopicPartition::new(topic, partition_id));
            }
        }

        // Get consumer IDs (sorted for consistent assignment)
        let mut consumers: Vec<ConsumerId> = group.value().members.keys().cloned().collect();
        consumers.sort();

        // Use partition assignor
        let assignor = PartitionAssignor::new(self.config.default_assignment_strategy.clone());
        let assignments = assignor.assign(&consumers, &all_partitions);

        debug!(
            "Generated partition assignments for group {}: {} consumers, {} partitions",
            group_id,
            consumers.len(),
            all_partitions.len()
        );

        Ok(assignments)
    }

    /// Assign partitions without acquiring group locks (to avoid deadlock)
    pub fn assign_partitions_without_lock(
        &self,
        group: &ConsumerGroupMetadata,
        topics: &[TopicName],
    ) -> Result<HashMap<ConsumerId, Vec<TopicPartition>>> {
        // Collect all partitions for subscribed topics
        let mut all_partitions = Vec::new();
        for topic in topics {
            let partitions = self.topic_manager.get_partitions(topic);
            debug!(
                "Topic '{}' has {} partitions: {:?}",
                topic,
                partitions.len(),
                partitions
            );
            for partition_id in partitions {
                all_partitions.push(TopicPartition::new(topic, partition_id));
            }
        }

        // Get consumer IDs (sorted for consistent assignment)
        let mut consumers: Vec<ConsumerId> = group.members.keys().cloned().collect();
        consumers.sort();

        // Use partition assignor
        let assignor = PartitionAssignor::new(self.config.default_assignment_strategy.clone());
        let assignments = assignor.assign(&consumers, &all_partitions);

        debug!(
            "Generated partition assignments: {} consumers, {} partitions",
            consumers.len(),
            all_partitions.len()
        );

        Ok(assignments)
    }

    /// Generate partition assignments automatically based on group's subscribed topics
    #[allow(dead_code)]
    async fn generate_partition_assignments(
        &self,
        group_id: &ConsumerGroupId,
        group: &ConsumerGroupMetadata,
    ) -> std::result::Result<HashMap<ConsumerId, Vec<TopicPartition>>, i16> {
        // Collect all subscribed topics from group members
        let mut subscribed_topics = std::collections::HashSet::new();
        for member in group.members.values() {
            for topic in &member.subscribed_topics {
                subscribed_topics.insert(topic.clone());
            }
        }

        // If no topics are subscribed, try to infer from existing topics
        if subscribed_topics.is_empty() {
            // For now, assign all available topics
            // In a real implementation, this would be based on consumer metadata
            let all_topics = self.topic_manager.list_topics();
            if all_topics.is_empty() {
                debug!("No topics available for assignment in group {}", group_id);
                let mut empty_assignments = HashMap::new();
                for consumer_id in group.members.keys() {
                    empty_assignments.insert(consumer_id.clone(), Vec::new());
                }
                return Ok(empty_assignments);
            }
            for topic in all_topics {
                subscribed_topics.insert(topic);
            }
        }

        let topics_vec: Vec<TopicName> = subscribed_topics.into_iter().collect();

        debug!(
            "Generating assignments for group {} with topics: {:?}, strategy: {:?}",
            group_id, topics_vec, self.config.default_assignment_strategy
        );

        // Generate assignments without holding the group lock to avoid deadlock
        match self.assign_partitions_without_lock(group, &topics_vec) {
            Ok(assignments) => {
                let total_partitions: usize = assignments.values().map(|v| v.len()).sum();
                info!(
                    "Generated {} partition assignments across {} consumers for group {}",
                    total_partitions,
                    assignments.len(),
                    group_id
                );
                Ok(assignments)
            }
            Err(e) => {
                warn!(
                    "Failed to generate partition assignments for group {}: {}",
                    group_id, e
                );
                // If assignment fails, return empty assignments
                let mut empty_assignments = HashMap::new();
                for consumer_id in group.members.keys() {
                    empty_assignments.insert(consumer_id.clone(), Vec::new());
                }
                Ok(empty_assignments)
            }
        }
    }

    /// Check if a rebalance is needed for the group
    pub async fn needs_rebalance(&self, group_id: &ConsumerGroupId) -> bool {
        if let Some(group) = self.groups.get(group_id) {
            match group.value().state {
                ConsumerGroupState::PreparingRebalance
                | ConsumerGroupState::CompletingRebalance => true,
                _ => false,
            }
        } else {
            false
        }
    }

    /// Force a rebalance for a group (useful for administrative operations)
    pub async fn trigger_rebalance(&self, group_id: &ConsumerGroupId) -> Result<()> {
        if let Some(mut group_ref) = self.groups.get_mut(group_id) {
            if !group_ref.members.is_empty() {
                group_ref.state = ConsumerGroupState::PreparingRebalance;
                group_ref.generation_id += 1;
                group_ref.state_timestamp = SystemTime::now();

                info!(
                    "Manually triggered rebalance for group {} (generation {})",
                    group_id, group_ref.generation_id
                );

                // Notify state change
                let _ = self
                    .state_change_tx
                    .send(GroupStateChange::RebalanceTriggered {
                        group_id: group_id.clone(),
                    });
            }
        }
        Ok(())
    }

    /// Get detailed group statistics
    pub async fn get_group_stats(&self, group_id: &ConsumerGroupId) -> Option<GroupStats> {
        self.groups.get(group_id).map(|group| {
            let total_partitions: usize = group
                .value()
                .members
                .values()
                .map(|member| member.assigned_partitions.len())
                .sum();

            GroupStats {
                group_id: group_id.clone(),
                state: group.value().state.clone(),
                member_count: group.value().members.len(),
                generation_id: group.value().generation_id,
                total_assigned_partitions: total_partitions,
                leader_id: group.value().leader_id.clone(),
                assignment_strategy: self.config.default_assignment_strategy.clone(),
                created_at: group.value().created_at,
                last_state_change: group.value().state_timestamp,
            }
        })
    }

    /// Internal offset commit implementation
    async fn commit_offsets_internal(
        &self,
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
        generation_id: i32,
        retention_time_ms: i64,
        offsets: Vec<TopicPartitionOffset>,
    ) -> std::result::Result<Vec<TopicPartitionError>, i16> {
        // Verify the consumer group exists and generation is valid
        {
            let group = self
                .groups
                .get(&group_id)
                .ok_or(error_codes::UNKNOWN_GROUP_ID)?;

            // Verify generation
            if group.value().generation_id != generation_id {
                return Err(error_codes::ILLEGAL_GENERATION);
            }

            // Verify member exists
            if !group.value().members.contains_key(&consumer_id) {
                return Err(error_codes::UNKNOWN_CONSUMER_ID);
            }
        }

        let now = SystemTime::now();
        let expire_timestamp = if retention_time_ms > 0 {
            Some(now + Duration::from_millis(retention_time_ms as u64))
        } else {
            None
        };

        let mut topic_partition_errors = Vec::new();

        for offset in offsets {
            // Validate that the topic/partition exists
            if self.topic_manager.get_topic(&offset.topic).is_none() {
                topic_partition_errors.push(TopicPartitionError {
                    topic: offset.topic,
                    partition: offset.partition,
                    error_code: super::error_codes::UNKNOWN_CONSUMER_ID, // Using closest match
                });
                continue;
            }

            if !self
                .topic_manager
                .partition_exists(&offset.topic, offset.partition)
            {
                topic_partition_errors.push(TopicPartitionError {
                    topic: offset.topic,
                    partition: offset.partition,
                    error_code: super::error_codes::UNKNOWN_CONSUMER_ID, // Using closest match
                });
                continue;
            }

            // Store offset in memory
            let key = (group_id.clone(), offset.topic.clone(), offset.partition);
            let consumer_offset = ConsumerOffset {
                group_id: group_id.clone(),
                topic: offset.topic.clone(),
                partition: offset.partition,
                offset: offset.offset,
                metadata: offset.metadata,
                commit_timestamp: now,
                expire_timestamp,
            };

            // Clone before moving for disk persistence
            let consumer_offset_for_disk = consumer_offset.clone();
            self.offset_storage.insert(key, consumer_offset);

            // Persist to disk storage if available
            if self.metadata_dir.is_some() {
                if let Err(e) = self
                    .persist_offset_to_disk(&group_id, &consumer_offset_for_disk)
                    .await
                {
                    error!(
                        "Failed to persist offset to disk for group {}, topic {}, partition {}: {}",
                        group_id, offset.topic, offset.partition, e
                    );
                    // Continue execution - disk persistence failure shouldn't block the operation
                }
            }
        }

        Ok(topic_partition_errors)
    }

    /// Internal offset fetch implementation
    async fn fetch_offsets_internal(
        &self,
        group_id: ConsumerGroupId,
        topic_partitions: Option<Vec<TopicPartition>>,
    ) -> std::result::Result<Vec<TopicPartitionOffsetResult>, i16> {
        // Verify the consumer group exists
        {
            if !self.groups.contains_key(&group_id) {
                return Err(error_codes::UNKNOWN_GROUP_ID);
            }
        }

        let mut results = Vec::new();

        match topic_partitions {
            Some(partitions) => {
                // Fetch specific partitions
                for tp in partitions {
                    let key = (group_id.clone(), tp.topic.clone(), tp.partition);

                    if let Some(stored_offset) = self.offset_storage.get(&key) {
                        // Check if offset has expired
                        let is_expired = stored_offset
                            .value()
                            .expire_timestamp
                            .map(|expire_time| SystemTime::now() > expire_time)
                            .unwrap_or(false);

                        if !is_expired {
                            results.push(TopicPartitionOffsetResult {
                                topic: tp.topic,
                                partition: tp.partition,
                                offset: stored_offset.value().offset,
                                leader_epoch: -1, // Not tracked yet
                                metadata: stored_offset.value().metadata.clone(),
                                error_code: error_codes::NONE,
                            });
                        } else {
                            // Expired offset, return -1 (no committed offset)
                            results.push(TopicPartitionOffsetResult {
                                topic: tp.topic,
                                partition: tp.partition,
                                offset: -1,
                                leader_epoch: -1,
                                metadata: None,
                                error_code: error_codes::NONE,
                            });
                        }
                    } else {
                        // No committed offset for this partition
                        results.push(TopicPartitionOffsetResult {
                            topic: tp.topic,
                            partition: tp.partition,
                            offset: -1, // Kafka convention for "no offset committed"
                            leader_epoch: -1,
                            metadata: None,
                            error_code: error_codes::NONE,
                        });
                    }
                }
            }
            None => {
                // Fetch all offsets for this group
                for entry in self.offset_storage.iter() {
                    let (stored_group_id, topic, partition) = entry.key();
                    let stored_offset = entry.value();

                    if stored_group_id == &group_id {
                        // Check if offset has expired
                        let is_expired = stored_offset
                            .expire_timestamp
                            .map(|expire_time| SystemTime::now() > expire_time)
                            .unwrap_or(false);

                        if !is_expired {
                            results.push(TopicPartitionOffsetResult {
                                topic: topic.clone(),
                                partition: *partition,
                                offset: stored_offset.offset,
                                leader_epoch: -1, // Not tracked yet
                                metadata: stored_offset.metadata.clone(),
                                error_code: error_codes::NONE,
                            });
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Background task to cleanup expired offsets
    async fn offset_cleanup_loop(&self) {
        let mut interval = interval(Duration::from_millis(
            self.config.consumer_expiration_check_interval_ms * 2, // Run less frequently than consumer expiration
        ));

        loop {
            interval.tick().await;

            if let Err(e) = self.cleanup_expired_offsets().await {
                error!("Error cleaning up expired offsets: {}", e);
            }
        }
    }

    /// Clean up expired offsets (called periodically)
    pub async fn cleanup_expired_offsets(&self) -> Result<()> {
        let now = SystemTime::now();

        self.offset_storage.retain(|_key, offset| {
            offset
                .expire_timestamp
                .map(|expire_time| now <= expire_time)
                .unwrap_or(true) // Keep offsets without expiration
        });

        Ok(())
    }

    /// Serialize member metadata (subscribed topics) for DescribeGroups response
    fn serialize_member_metadata(&self, subscribed_topics: &[TopicName]) -> Vec<u8> {
        // Simple serialization format: length-prefixed topic names
        // In a real implementation, this would follow Kafka's serialization format
        let mut serialized = Vec::new();

        // Write number of topics (4 bytes)
        serialized.extend_from_slice(&(subscribed_topics.len() as u32).to_be_bytes());

        for topic in subscribed_topics {
            // Write topic name length (4 bytes) + topic name
            serialized.extend_from_slice(&(topic.len() as u32).to_be_bytes());
            serialized.extend_from_slice(topic.as_bytes());
        }

        serialized
    }

    /// Serialize member assignment (assigned partitions) for DescribeGroups response  
    fn serialize_member_assignment(&self, assigned_partitions: &[TopicPartition]) -> Vec<u8> {
        // Simple serialization format: length-prefixed topic-partition pairs
        let mut serialized = Vec::new();

        // Write number of topic-partitions (4 bytes)
        serialized.extend_from_slice(&(assigned_partitions.len() as u32).to_be_bytes());

        for tp in assigned_partitions {
            // Write topic name length (4 bytes) + topic name
            serialized.extend_from_slice(&(tp.topic.len() as u32).to_be_bytes());
            serialized.extend_from_slice(tp.topic.as_bytes());

            // Write partition ID (4 bytes)
            serialized.extend_from_slice(&tp.partition.to_be_bytes());
        }

        serialized
    }

    /// Get all active groups (for administrative purposes)
    pub async fn get_all_groups(&self) -> Vec<ConsumerGroupId> {
        self.groups
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get group by ID with full metadata
    pub async fn get_group_metadata(
        &self,
        group_id: &ConsumerGroupId,
    ) -> Option<ConsumerGroupMetadata> {
        self.groups.get(group_id).map(|entry| entry.value().clone())
    }

    /// Check if group exists and is active
    pub async fn is_group_active(&self, group_id: &ConsumerGroupId) -> bool {
        self.groups
            .get(group_id)
            .map(|group| {
                !matches!(
                    group.value().state,
                    ConsumerGroupState::Dead | ConsumerGroupState::Empty
                )
            })
            .unwrap_or(false)
    }

    /// Load persisted group metadata from storage
    async fn load_persisted_metadata(&self) -> Result<()> {
        let Some(metadata_dir) = self.get_metadata_dir() else {
            debug!("No metadata directory configured, skipping persistence loading");
            return Ok(());
        };

        if !metadata_dir.exists() {
            debug!("Metadata directory does not exist: {:?}", metadata_dir);
            return Ok(());
        }

        let mut loaded_count = 0;

        // Read all JSON files in the metadata directory
        let mut entries = fs::read_dir(metadata_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if let Some(extension) = path.extension() {
                if extension == "json"
                    && path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .map(|name| name.starts_with("group_"))
                        .unwrap_or(false)
                {
                    match self.load_group_metadata_file(&path).await {
                        Ok(group_metadata) => {
                            info!(
                                "Loaded persisted group metadata for group: {}",
                                group_metadata.group_id
                            );
                            self.groups
                                .insert(group_metadata.group_id.clone(), group_metadata);
                            loaded_count += 1;
                        }
                        Err(e) => {
                            warn!("Failed to load group metadata from {:?}: {}", path, e);
                        }
                    }
                }
            }
        }

        info!(
            "Loaded {} consumer group metadata files from disk",
            loaded_count
        );
        Ok(())
    }

    /// Load group metadata from a specific JSON file
    async fn load_group_metadata_file(&self, path: &Path) -> Result<ConsumerGroupMetadata> {
        let content = fs::read_to_string(path).await?;
        let serializable: SerializableGroupMetadata = serde_json::from_str(&content)?;

        // Convert from serializable to runtime format
        let metadata = self.from_serializable_metadata(serializable);
        Ok(metadata)
    }

    /// Convert serializable metadata to runtime metadata
    fn from_serializable_metadata(
        &self,
        serializable: SerializableGroupMetadata,
    ) -> ConsumerGroupMetadata {
        let members = serializable
            .members
            .into_iter()
            .map(|(consumer_id, serializable_member)| {
                let member = ConsumerGroupMember {
                    consumer_id: serializable_member.consumer_id,
                    group_id: serializable_member.group_id,
                    client_id: serializable_member.client_id,
                    client_host: serializable_member.client_host,
                    session_timeout_ms: serializable_member.session_timeout_ms,
                    rebalance_timeout_ms: serializable_member.rebalance_timeout_ms,
                    subscribed_topics: serializable_member.subscribed_topics,
                    assigned_partitions: serializable_member.assigned_partitions,
                    last_heartbeat: SystemTime::UNIX_EPOCH
                        + Duration::from_secs(serializable_member.last_heartbeat),
                    is_leader: serializable_member.is_leader,
                };
                (consumer_id, member)
            })
            .collect();

        ConsumerGroupMetadata {
            group_id: serializable.group_id,
            state: serializable.state,
            protocol_type: serializable.protocol_type,
            protocol_name: serializable.protocol_name,
            leader_id: serializable.leader_id,
            members,
            generation_id: serializable.generation_id,
            created_at: SystemTime::UNIX_EPOCH + Duration::from_secs(serializable.created_at),
            state_timestamp: SystemTime::UNIX_EPOCH
                + Duration::from_secs(serializable.state_timestamp),
        }
    }

    /// Background task for persisting metadata changes
    async fn metadata_persistence_loop(&self) {
        let mut interval = interval(Duration::from_millis(
            self.config.consumer_expiration_check_interval_ms * 10, // Persist less frequently
        ));

        loop {
            interval.tick().await;

            if let Err(e) = self.persist_metadata_changes().await {
                error!("Error persisting group metadata: {}", e);
            }
        }
    }

    /// Persist current group metadata to storage
    async fn persist_metadata_changes(&self) -> Result<()> {
        let Some(metadata_dir) = self.get_metadata_dir() else {
            // No metadata directory configured, skip persistence
            return Ok(());
        };

        // Ensure metadata directory exists
        if !metadata_dir.exists() {
            fs::create_dir_all(metadata_dir).await?;
        }

        // 🚀 LOCK-FREE: DashMap iteration without read lock
        let mut persisted_count = 0;

        for entry in self.groups.iter() {
            let group = entry.value();
            // Only persist active groups (not Dead state)
            if !matches!(group.state, ConsumerGroupState::Dead) {
                match self.persist_group_metadata(group).await {
                    Ok(()) => persisted_count += 1,
                    Err(e) => {
                        error!(
                            "Failed to persist metadata for group {}: {}",
                            group.group_id, e
                        );
                    }
                }
            }
        }

        if persisted_count > 0 {
            debug!(
                "Persisted {} consumer group metadata files",
                persisted_count
            );
        }

        Ok(())
    }

    /// Persist a single group's metadata to JSON file
    async fn persist_group_metadata(&self, group: &ConsumerGroupMetadata) -> Result<()> {
        let Some(file_path) = self.get_metadata_file_path(&group.group_id) else {
            return Ok(());
        };

        let serializable = self.to_serializable_metadata(group);
        let json_content = serde_json::to_string_pretty(&serializable)?;

        fs::write(&file_path, json_content).await?;

        debug!(
            "Persisted metadata for group {} to {:?}",
            group.group_id, file_path
        );
        Ok(())
    }

    /// Convert runtime metadata to serializable format
    fn to_serializable_metadata(
        &self,
        metadata: &ConsumerGroupMetadata,
    ) -> SerializableGroupMetadata {
        let members = metadata
            .members
            .iter()
            .map(|(consumer_id, member)| {
                let serializable_member = SerializableGroupMember {
                    consumer_id: member.consumer_id.clone(),
                    group_id: member.group_id.clone(),
                    client_id: member.client_id.clone(),
                    client_host: member.client_host.clone(),
                    session_timeout_ms: member.session_timeout_ms,
                    rebalance_timeout_ms: member.rebalance_timeout_ms,
                    subscribed_topics: member.subscribed_topics.clone(),
                    assigned_partitions: member.assigned_partitions.clone(),
                    last_heartbeat: member
                        .last_heartbeat
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or(Duration::from_secs(0))
                        .as_secs(),
                    is_leader: member.is_leader,
                };
                (consumer_id.clone(), serializable_member)
            })
            .collect();

        SerializableGroupMetadata {
            group_id: metadata.group_id.clone(),
            state: metadata.state.clone(),
            protocol_type: metadata.protocol_type.clone(),
            protocol_name: metadata.protocol_name.clone(),
            leader_id: metadata.leader_id.clone(),
            members,
            generation_id: metadata.generation_id,
            created_at: metadata
                .created_at
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or(Duration::from_secs(0))
                .as_secs(),
            state_timestamp: metadata
                .state_timestamp
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or(Duration::from_secs(0))
                .as_secs(),
        }
    }

    /// Force persistence of all metadata immediately
    pub async fn force_persist_metadata(&self) -> Result<()> {
        self.persist_metadata_changes().await
    }

    /// Remove persisted metadata file for a group (when group becomes dead)
    pub async fn remove_persisted_group(&self, group_id: &ConsumerGroupId) -> Result<()> {
        if let Some(file_path) = self.get_metadata_file_path(group_id) {
            if file_path.exists() {
                fs::remove_file(&file_path).await?;
                debug!(
                    "Removed persisted metadata file for dead group {}: {:?}",
                    group_id, file_path
                );
            }
        }
        Ok(())
    }
}

/// Manager for multiple consumer group coordinators
pub struct ConsumerGroupManager {
    coordinators: HashMap<String, Arc<ConsumerGroupCoordinator>>,
}

impl ConsumerGroupManager {
    pub fn new() -> Self {
        Self {
            coordinators: HashMap::new(),
        }
    }

    /// Add a coordinator for a specific partition or topic range
    pub fn add_coordinator(&mut self, key: String, coordinator: Arc<ConsumerGroupCoordinator>) {
        self.coordinators.insert(key, coordinator);
    }

    /// Get coordinator for a group (simple implementation uses default coordinator)
    pub fn get_coordinator(&self, _group_id: &str) -> Option<Arc<ConsumerGroupCoordinator>> {
        // In a full implementation, this would hash the group_id to determine
        // which coordinator (partition) should handle this group
        self.coordinators.get("default").cloned()
    }
}

impl ConsumerGroupCoordinator {
    /// Select a compatible protocol from the client's supported protocols
    fn select_compatible_protocol(&self, group_protocols: &[GroupProtocol]) -> String {
        // FluxMQ supports these assignment strategies
        let supported_protocols = ["range", "roundrobin", "sticky"];

        // First, try to find a directly supported protocol
        for protocol in group_protocols {
            if supported_protocols.contains(&protocol.name.as_str()) {
                return protocol.name.clone();
            }
        }

        // If client sends "consumer" protocol, this means it's using kafka-python's generic protocol.
        // The client actually expects a specific assignment strategy name back, not "consumer".
        // From the logs, we see kafka-python sends both protocol type "consumer" and protocol name "range",
        // so we should return "range" (the assignment strategy) to satisfy the client expectation.
        for protocol in group_protocols {
            if protocol.name == "consumer" {
                // For kafka-python compatibility, return "range" strategy instead of "consumer"
                return "range".to_string();
            }
        }

        // If no known protocols, default to "range"
        "range".to_string()
    }

    /// Extract subscribed topics from group protocol metadata
    fn extract_subscribed_topics(&self, group_protocols: &[GroupProtocol]) -> Vec<TopicName> {
        let mut topics = Vec::new();

        // Look for the consumer protocol
        for protocol in group_protocols {
            if protocol.name == "consumer"
                || protocol.name == "range"
                || protocol.name == "roundrobin"
            {
                // Try to parse the metadata bytes as topics
                // The metadata format in Kafka consumer protocol contains serialized topic subscriptions
                if let Ok(parsed_topics) = self.parse_consumer_protocol_metadata(&protocol.metadata)
                {
                    topics.extend(parsed_topics);
                }
            }
        }

        // If no topics found in protocol metadata, this might be a legacy client
        // or the metadata parsing failed. In this case, we'll let the assignment logic
        // fall back to using available topics
        if topics.is_empty() {
            debug!("No topics found in protocol metadata, will use topic inference");
        }

        topics
    }

    /// Parse consumer protocol metadata to extract topic subscriptions
    fn parse_consumer_protocol_metadata(&self, metadata: &[u8]) -> Result<Vec<TopicName>> {
        use bytes::Buf;

        // Kafka consumer protocol metadata format:
        // - version: int16
        // - topics: array of strings
        // - userdata: bytes (can be empty)

        if metadata.len() < 6 {
            return Ok(Vec::new());
        }

        let mut cursor = std::io::Cursor::new(metadata);

        // Read version (2 bytes)
        let _version = cursor.get_i16();

        // Read topic array length (4 bytes)
        let topic_count = cursor.get_i32();
        if topic_count < 0 || topic_count > 1000 {
            // Sanity check
            return Ok(Vec::new());
        }

        let mut topics = Vec::new();
        for _ in 0..topic_count {
            if cursor.remaining() < 2 {
                break;
            }

            // Read topic name length (2 bytes)
            let topic_len = cursor.get_i16();
            if topic_len <= 0 || cursor.remaining() < topic_len as usize {
                break;
            }

            // Read topic name
            let mut topic_bytes = vec![0u8; topic_len as usize];
            cursor.copy_to_slice(&mut topic_bytes);

            if let Ok(topic_name) = String::from_utf8(topic_bytes) {
                topics.push(topic_name);
            }
        }

        Ok(topics)
    }

    /// Save consumer offset to disk storage
    async fn persist_offset_to_disk(
        &self,
        group_id: &ConsumerGroupId,
        consumer_offset: &ConsumerOffset,
    ) -> Result<()> {
        if let Some(metadata_dir) = &self.metadata_dir {
            let offsets_dir = metadata_dir.join("offsets");

            // Ensure offsets directory exists
            if let Err(e) = fs::create_dir_all(&offsets_dir).await {
                error!(
                    "Failed to create offsets directory {:?}: {}",
                    offsets_dir, e
                );
                return Err(crate::FluxmqError::Storage(e));
            }

            let group_file = offsets_dir.join(format!("{}.json", group_id));

            // Read existing offsets for this group
            let mut group_offsets: Vec<SerializableConsumerOffset> = if group_file.exists() {
                match fs::read_to_string(&group_file).await {
                    Ok(content) => match serde_json::from_str(&content) {
                        Ok(offsets) => offsets,
                        Err(e) => {
                            warn!(
                                "Failed to parse existing offsets for group {}: {}",
                                group_id, e
                            );
                            Vec::new()
                        }
                    },
                    Err(e) => {
                        warn!(
                            "Failed to read existing offsets for group {}: {}",
                            group_id, e
                        );
                        Vec::new()
                    }
                }
            } else {
                Vec::new()
            };

            // Update or add the offset
            let serializable_offset = SerializableConsumerOffset::from(consumer_offset);
            let key = (consumer_offset.topic.as_str(), consumer_offset.partition);

            // Remove existing offset for same topic-partition
            group_offsets.retain(|offset| (offset.topic.as_str(), offset.partition) != key);

            // Add the new offset
            group_offsets.push(serializable_offset);

            // Write back to disk
            match serde_json::to_string_pretty(&group_offsets) {
                Ok(json_content) => {
                    if let Err(e) = fs::write(&group_file, json_content).await {
                        error!("Failed to write offsets to {:?}: {}", group_file, e);
                        return Err(crate::FluxmqError::Storage(e));
                    }
                    debug!(
                        "Persisted offset to disk: group={}, topic={}, partition={}, offset={}",
                        group_id,
                        consumer_offset.topic,
                        consumer_offset.partition,
                        consumer_offset.offset
                    );
                }
                Err(e) => {
                    error!("Failed to serialize offsets for group {}: {}", group_id, e);
                    return Err(crate::FluxmqError::Json(e));
                }
            }
        }

        Ok(())
    }

    /// Load consumer offsets from disk storage on startup
    pub async fn load_offsets_from_disk(&self) -> Result<()> {
        if let Some(metadata_dir) = &self.metadata_dir {
            let offsets_dir = metadata_dir.join("offsets");

            if !offsets_dir.exists() {
                debug!("Offsets directory does not exist: {:?}", offsets_dir);
                return Ok(());
            }

            let mut entries = match fs::read_dir(&offsets_dir).await {
                Ok(entries) => entries,
                Err(e) => {
                    warn!("Failed to read offsets directory {:?}: {}", offsets_dir, e);
                    return Ok(());
                }
            };

            let mut total_loaded = 0;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();

                if path.extension().and_then(|s| s.to_str()) != Some("json") {
                    continue;
                }

                let _group_id = match path.file_stem().and_then(|s| s.to_str()) {
                    Some(id) => id.to_string(),
                    None => continue,
                };

                // Read and parse the group's offset file
                match fs::read_to_string(&path).await {
                    Ok(content) => {
                        match serde_json::from_str::<Vec<SerializableConsumerOffset>>(&content) {
                            Ok(serializable_offsets) => {
                                // 🚀 LOCK-FREE: DashMap direct insert without write lock
                                for serializable_offset in serializable_offsets {
                                    let consumer_offset = ConsumerOffset::from(serializable_offset);
                                    let key = (
                                        consumer_offset.group_id.clone(),
                                        consumer_offset.topic.clone(),
                                        consumer_offset.partition,
                                    );
                                    self.offset_storage.insert(key, consumer_offset);
                                    total_loaded += 1;
                                }
                            }
                            Err(e) => {
                                error!("Failed to parse offset file {:?}: {}", path, e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to read offset file {:?}: {}", path, e);
                    }
                }
            }

            if total_loaded > 0 {
                info!("Loaded {} consumer offsets from disk storage", total_loaded);
            } else {
                debug!("No consumer offsets found on disk");
            }
        }

        Ok(())
    }
}
