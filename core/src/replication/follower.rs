use super::{BrokerId, LogEntry, ReplicationConfig, ReplicationMessage};
use crate::protocol::{Message, Offset, PartitionId, TopicName};
use crate::storage::HybridStorage;
use crate::Result;
use crossbeam::channel;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

/// Follower state for a partition
#[derive(Debug)]
pub struct FollowerState {
    broker_id: BrokerId,
    topic: TopicName,
    partition: PartitionId,
    leader_id: BrokerId,
    _config: ReplicationConfig,

    /// Current term known by this follower
    current_term: Arc<RwLock<u64>>,

    /// Last log offset committed by the leader
    commit_index: Arc<RwLock<Offset>>,

    /// Last log offset applied by this follower
    last_applied: Arc<RwLock<Offset>>,

    /// Channel for receiving replication requests from leader
    request_rx: Arc<Mutex<Option<channel::Receiver<ReplicationMessage>>>>,
    _request_tx: channel::Sender<ReplicationMessage>,

    /// Channel for sending responses back to leader
    response_tx: channel::Sender<ReplicationMessage>,
}

impl FollowerState {
    pub fn new(
        broker_id: BrokerId,
        topic: TopicName,
        partition: PartitionId,
        leader_id: BrokerId,
        config: ReplicationConfig,
    ) -> Self {
        let (request_tx, request_rx) = channel::unbounded();
        let (response_tx, _) = channel::unbounded(); // Response channel would connect to leader

        Self {
            broker_id,
            topic,
            partition,
            leader_id,
            _config: config,
            current_term: Arc::new(RwLock::new(0)),
            commit_index: Arc::new(RwLock::new(0)),
            last_applied: Arc::new(RwLock::new(0)),
            request_rx: Arc::new(Mutex::new(Some(request_rx))),
            _request_tx: request_tx,
            response_tx,
        }
    }

    /// Start the follower's replication tasks
    pub async fn start_replication(self: Arc<Self>, storage: Arc<HybridStorage>) -> Result<()> {
        // Start request handler task
        let handler_state = Arc::clone(&self);
        let handler_storage = Arc::clone(&storage);
        tokio::spawn(async move {
            handler_state.handle_requests(handler_storage).await;
        });

        info!(
            "Started follower replication for {}:{} with leader {}",
            self.topic, self.partition, self.leader_id
        );

        Ok(())
    }

    /// Handle incoming replication requests from the leader
    async fn handle_requests(&self, storage: Arc<HybridStorage>) {
        let request_rx = {
            let mut rx_guard = self.request_rx.lock().await;
            if let Some(rx) = rx_guard.take() {
                rx
            } else {
                return;
            }
        };

        loop {
            // Use spawn_blocking to handle crossbeam channel
            let result = tokio::task::spawn_blocking({
                let rx_clone = request_rx.clone();
                move || rx_clone.recv()
            })
            .await;

            match result {
                Ok(Ok(request)) => {
                    if let Err(e) = self.process_request(request, storage.clone()).await {
                        error!("Failed to process replication request: {}", e);
                    }
                }
                Ok(Err(_)) => break, // Channel closed
                Err(_) => break,     // spawn_blocking failed
            }
        }
    }

    /// Process a replication request from the leader
    async fn process_request(
        &self,
        request: ReplicationMessage,
        storage: Arc<HybridStorage>,
    ) -> Result<()> {
        match request {
            ReplicationMessage::ReplicateRequest {
                topic,
                partition,
                leader_epoch,
                prev_log_offset,
                entries,
                leader_commit,
            } => {
                debug!(
                    "Processing replicate request for {}:{}, epoch: {}, prev_offset: {}, {} entries",
                    topic, partition, leader_epoch, prev_log_offset, entries.len()
                );

                // Update current term if leader's epoch is higher
                {
                    let mut current_term = self.current_term.write().await;
                    if leader_epoch > *current_term {
                        *current_term = leader_epoch;
                    }
                }

                // Check if we can append these entries
                let can_append = self.can_append_entries(&storage, prev_log_offset).await?;

                if can_append {
                    // Apply the log entries
                    let entries_len = entries.len();
                    let last_offset = self.apply_entries(&storage, entries).await?;

                    // Update commit index if leader's commit is higher
                    {
                        let mut commit_idx = self.commit_index.write().await;
                        if leader_commit > *commit_idx {
                            *commit_idx = leader_commit.min(last_offset);
                        }
                    }

                    // Send success response
                    let response = ReplicationMessage::ReplicateResponse {
                        topic,
                        partition,
                        success: true,
                        last_log_offset: last_offset,
                        follower_id: self.broker_id,
                    };

                    if let Err(_) = self.response_tx.send(response) {
                        warn!("Failed to send replication response to leader");
                    }

                    debug!(
                        "Successfully replicated {} entries, last_offset: {}",
                        entries_len, last_offset
                    );
                } else {
                    // Send failure response - leader will decrement next_index and retry
                    let current_offset = storage.get_latest_offset(&topic, partition).unwrap_or(0);

                    let response = ReplicationMessage::ReplicateResponse {
                        topic,
                        partition,
                        success: false,
                        last_log_offset: current_offset,
                        follower_id: self.broker_id,
                    };

                    if let Err(_) = self.response_tx.send(response) {
                        warn!("Failed to send replication response to leader");
                    }

                    debug!(
                        "Cannot append entries at prev_offset: {}, current_offset: {}",
                        prev_log_offset, current_offset
                    );
                }
            }
            ReplicationMessage::Heartbeat {
                leader_id,
                term,
                commit_index: leader_commit,
            } => {
                debug!(
                    "Received heartbeat from leader {} for {}:{}, term: {}, commit: {}",
                    leader_id, self.topic, self.partition, term, leader_commit
                );

                // Update current term
                let mut success = true;
                {
                    let mut current_term = self.current_term.write().await;
                    if term >= *current_term {
                        *current_term = term;
                    } else {
                        success = false; // Reject heartbeat from outdated leader
                    }
                }

                // Update commit index
                if success {
                    let mut commit_idx = self.commit_index.write().await;
                    if leader_commit > *commit_idx {
                        let current_offset = storage
                            .get_latest_offset(&self.topic, self.partition)
                            .unwrap_or(0);
                        *commit_idx = leader_commit.min(current_offset);
                    }
                }

                // Send heartbeat response
                let response = ReplicationMessage::HeartbeatResponse {
                    follower_id: self.broker_id,
                    term: *self.current_term.read().await,
                    success,
                };

                if let Err(_) = self.response_tx.send(response) {
                    warn!("Failed to send heartbeat response to leader");
                }
            }
            _ => {
                warn!("Received unexpected message type for follower");
            }
        }

        Ok(())
    }

    /// Check if we can append entries at the given previous log offset
    async fn can_append_entries(
        &self,
        storage: &Arc<HybridStorage>,
        prev_log_offset: Offset,
    ) -> Result<bool> {
        let current_offset = storage
            .get_latest_offset(&self.topic, self.partition)
            .unwrap_or(0);

        // If prev_log_offset is 0, we can always append (starting from beginning)
        if prev_log_offset == 0 {
            return Ok(current_offset == 0);
        }

        // Check if our log matches at prev_log_offset
        // In a full implementation, we'd also check the term at that offset
        Ok(current_offset >= prev_log_offset)
    }

    /// Apply log entries to local storage
    async fn apply_entries(
        &self,
        storage: &Arc<HybridStorage>,
        entries: Vec<LogEntry>,
    ) -> Result<Offset> {
        if entries.is_empty() {
            let current_offset = storage
                .get_latest_offset(&self.topic, self.partition)
                .unwrap_or(0);
            return Ok(current_offset);
        }

        let entries_len = entries.len();
        let messages: Vec<Message> = entries.into_iter().map(|entry| entry.message).collect();
        let base_offset = storage.append_messages(&self.topic, self.partition, messages)?;

        // Update last applied offset
        let last_offset = base_offset + (entries_len as u64) - 1;
        {
            let mut last_applied = self.last_applied.write().await;
            *last_applied = last_offset;
        }

        Ok(last_offset)
    }

    /// Get the current commit index
    pub async fn get_commit_index(&self) -> Offset {
        *self.commit_index.read().await
    }

    /// Get the last applied offset
    pub async fn get_last_applied(&self) -> Offset {
        *self.last_applied.read().await
    }

    /// Get the current term
    pub async fn get_current_term(&self) -> u64 {
        *self.current_term.read().await
    }
}

/// Manages follower synchronization with leaders
pub struct FollowerSync {
    broker_id: BrokerId,
    followers: Arc<RwLock<std::collections::HashMap<(TopicName, PartitionId), Arc<FollowerState>>>>,
    storage: Arc<HybridStorage>,
    config: ReplicationConfig,
}

impl FollowerSync {
    pub fn new(
        broker_id: BrokerId,
        storage: Arc<HybridStorage>,
        config: ReplicationConfig,
    ) -> Self {
        Self {
            broker_id,
            followers: Arc::new(RwLock::new(std::collections::HashMap::new())),
            storage,
            config,
        }
    }

    /// Add a new follower partition
    pub async fn add_follower(
        &self,
        topic: &str,
        partition: PartitionId,
        leader_id: BrokerId,
    ) -> Result<()> {
        let follower_state = FollowerState::new(
            self.broker_id,
            topic.to_string(),
            partition,
            leader_id,
            self.config.clone(),
        );

        let follower_state_arc = Arc::new(follower_state);
        follower_state_arc
            .clone()
            .start_replication(Arc::clone(&self.storage))
            .await?;

        {
            let mut followers = self.followers.write().await;
            followers.insert((topic.to_string(), partition), follower_state_arc);
        }

        info!(
            "Added follower for {}:{} with leader {}",
            topic, partition, leader_id
        );
        Ok(())
    }

    /// Remove a follower partition
    pub async fn remove_follower(&self, topic: &str, partition: PartitionId) -> Result<()> {
        {
            let mut followers = self.followers.write().await;
            followers.remove(&(topic.to_string(), partition));
        }

        info!("Removed follower for {}:{}", topic, partition);
        Ok(())
    }

    /// Get follower state for a partition
    pub async fn get_follower(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Option<Arc<FollowerState>> {
        let followers = self.followers.read().await;
        followers.get(&(topic.to_string(), partition)).cloned()
    }

    /// Synchronize with leader - in a full implementation, this would initiate catch-up
    pub async fn sync_with_leader(&self, topic: &str, partition: PartitionId) -> Result<()> {
        let follower_state = self.get_follower(topic, partition).await;

        if let Some(follower) = follower_state {
            let current_offset = self
                .storage
                .get_latest_offset(topic, partition)
                .unwrap_or(0);
            let last_applied = follower.get_last_applied().await;

            if current_offset != last_applied {
                info!(
                    "Synchronizing follower for {}:{}, current_offset: {}, last_applied: {}",
                    topic, partition, current_offset, last_applied
                );

                // In a full implementation, would request missing entries from leader
                // For now, just update the last_applied to match current storage
                {
                    let mut last_applied_guard = follower.last_applied.write().await;
                    *last_applied_guard = current_offset;
                }
            }
        }

        Ok(())
    }

    /// Check if we're caught up with all leaders
    pub async fn is_caught_up(&self) -> bool {
        let followers = self.followers.read().await;

        for ((_topic, _partition), follower) in followers.iter() {
            let commit_index = follower.get_commit_index().await;
            let last_applied = follower.get_last_applied().await;

            // If we're behind on any partition, we're not caught up
            if last_applied < commit_index {
                return false;
            }
        }

        true
    }

    /// Get status of all follower partitions
    pub async fn get_follower_status(
        &self,
    ) -> std::collections::HashMap<(TopicName, PartitionId), FollowerStatus> {
        let mut status = std::collections::HashMap::new();
        let followers = self.followers.read().await;

        for ((topic, partition), follower) in followers.iter() {
            let commit_index = follower.get_commit_index().await;
            let last_applied = follower.get_last_applied().await;
            let current_term = follower.get_current_term().await;

            status.insert(
                (topic.clone(), *partition),
                FollowerStatus {
                    leader_id: follower.leader_id,
                    current_term,
                    commit_index,
                    last_applied,
                    is_caught_up: last_applied >= commit_index,
                },
            );
        }

        status
    }
}

/// Status information for a follower partition
#[derive(Debug, Clone)]
pub struct FollowerStatus {
    pub leader_id: BrokerId,
    pub current_term: u64,
    pub commit_index: Offset,
    pub last_applied: Offset,
    pub is_caught_up: bool,
}
