use super::{BrokerId, LogEntry, ReplicationConfig, ReplicationMessage, ReplicationNetworkManager};
use crate::protocol::{Message, Offset, PartitionId, TopicName};
use crate::storage::HybridStorage;
use crate::Result;
use crossbeam::channel;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Leader state for a partition
#[derive(Debug)]
pub struct LeaderState {
    broker_id: BrokerId,
    topic: TopicName,
    partition: PartitionId,
    term: u64,
    config: ReplicationConfig,

    /// Followers and their replication progress
    follower_states: Arc<RwLock<HashMap<BrokerId, FollowerProgress>>>,

    /// Next offset to send to each follower
    next_index: Arc<RwLock<HashMap<BrokerId, Offset>>>,

    /// Highest offset replicated by each follower
    match_index: Arc<RwLock<HashMap<BrokerId, Offset>>>,

    /// Channel for receiving replication responses
    response_rx: Arc<tokio::sync::Mutex<Option<channel::Receiver<ReplicationMessage>>>>,
    _response_tx: channel::Sender<ReplicationMessage>,

    /// Network manager for sending replication messages
    network_manager: Arc<RwLock<Option<Weak<ReplicationNetworkManager>>>>,
}

/// Progress tracking for a follower
#[derive(Debug, Clone)]
pub struct FollowerProgress {
    _broker_id: BrokerId,
    next_index: Offset,
    match_index: Offset,
    last_heartbeat: SystemTime,
    is_alive: bool,
}

impl LeaderState {
    pub fn new(
        broker_id: BrokerId,
        topic: TopicName,
        partition: PartitionId,
        followers: Vec<BrokerId>,
        config: ReplicationConfig,
    ) -> Self {
        let (response_tx, response_rx) = channel::unbounded();

        let mut follower_states = HashMap::new();
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();

        for follower_id in followers {
            if follower_id != broker_id {
                follower_states.insert(
                    follower_id,
                    FollowerProgress {
                        _broker_id: follower_id,
                        next_index: 0,
                        match_index: 0,
                        last_heartbeat: SystemTime::now(),
                        is_alive: true,
                    },
                );
                next_index.insert(follower_id, 0);
                match_index.insert(follower_id, 0);
            }
        }

        Self {
            broker_id,
            topic,
            partition,
            term: 1,
            config,
            follower_states: Arc::new(RwLock::new(follower_states)),
            next_index: Arc::new(RwLock::new(next_index)),
            match_index: Arc::new(RwLock::new(match_index)),
            response_rx: Arc::new(tokio::sync::Mutex::new(Some(response_rx))),
            _response_tx: response_tx,
            network_manager: Arc::new(RwLock::new(None)),
        }
    }

    /// Start the leader's replication tasks
    pub async fn start_replication(self: Arc<Self>, storage: Arc<HybridStorage>) -> Result<()> {
        // Start heartbeat task
        let heartbeat_state = Arc::clone(&self);
        let heartbeat_storage = Arc::clone(&storage);
        tokio::spawn(async move {
            heartbeat_state.heartbeat_loop(heartbeat_storage).await;
        });

        // Start response handler task
        let response_state = Arc::clone(&self);
        tokio::spawn(async move {
            response_state.handle_responses().await;
        });

        info!(
            "Started leader replication for {}:{} with {} followers",
            self.topic,
            self.partition,
            self.follower_states.read().await.len()
        );

        Ok(())
    }

    /// Replicate new messages to followers
    pub async fn replicate_messages(
        &self,
        storage: Arc<HybridStorage>,
        messages: Vec<Message>,
        base_offset: Offset,
    ) -> Result<()> {
        let entries: Vec<LogEntry> = messages
            .into_iter()
            .enumerate()
            .map(|(i, message)| LogEntry {
                offset: base_offset + i as u64,
                term: self.term,
                message,
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            })
            .collect();

        self.send_entries_to_followers(storage, entries).await?;
        Ok(())
    }

    /// Send log entries to all followers
    async fn send_entries_to_followers(
        &self,
        storage: Arc<HybridStorage>,
        entries: Vec<LogEntry>,
    ) -> Result<()> {
        let followers: Vec<BrokerId> = {
            let follower_states = self.follower_states.read().await;
            follower_states.keys().copied().collect()
        };

        for follower_id in followers {
            if let Err(e) = self
                .send_entries_to_follower(follower_id, storage.clone(), &entries)
                .await
            {
                warn!("Failed to send entries to follower {}: {}", follower_id, e);
            }
        }

        Ok(())
    }

    /// Send log entries to a specific follower
    async fn send_entries_to_follower(
        &self,
        follower_id: BrokerId,
        storage: Arc<HybridStorage>,
        entries: &[LogEntry],
    ) -> Result<()> {
        let next_index = {
            let next_indices = self.next_index.read().await;
            next_indices.get(&follower_id).copied().unwrap_or(0)
        };

        let prev_log_offset = if next_index > 0 { next_index - 1 } else { 0 };
        let leader_commit = self.get_commit_index(storage.clone()).await?;

        let request = ReplicationMessage::ReplicateRequest {
            topic: self.topic.clone(),
            partition: self.partition,
            leader_epoch: self.term,
            prev_log_offset,
            entries: entries.to_vec(),
            leader_commit,
        };

        // Send the actual replication request over network
        if let Some(network_manager) = self.get_network_manager().await {
            match network_manager
                .send_replication_message(follower_id, request)
                .await
            {
                Ok(_) => {
                    debug!(
                        "Successfully sent {} entries to follower {} for {}:{}, prev_offset: {}, commit: {}",
                        entries.len(),
                        follower_id,
                        self.topic,
                        self.partition,
                        prev_log_offset,
                        leader_commit
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to send replication request to follower {}: {}",
                        follower_id, e
                    );
                    // Mark follower as potentially offline for health check
                    self.mark_follower_unhealthy(follower_id).await;
                }
            }
        } else {
            // Fallback for local testing without network
            debug!(
                "No network manager available, simulating send of {} entries to follower {} for {}:{}, prev_offset: {}, commit: {}",
                entries.len(),
                follower_id,
                self.topic,
                self.partition,
                prev_log_offset,
                leader_commit
            );
        }

        Ok(())
    }

    /// Get the current commit index (highest offset that's been replicated to majority)
    async fn get_commit_index(&self, storage: Arc<HybridStorage>) -> Result<Offset> {
        let latest_offset = storage
            .get_latest_offset(&self.topic, self.partition)
            .unwrap_or(0);

        // For now, return the latest offset
        // In a full implementation, this would check majority replication
        Ok(latest_offset)
    }

    /// Periodic heartbeat to maintain leadership
    async fn heartbeat_loop(&self, storage: Arc<HybridStorage>) {
        let mut interval = interval(Duration::from_millis(self.config.heartbeat_interval_ms));

        loop {
            interval.tick().await;

            if let Err(e) = self.send_heartbeats(storage.clone()).await {
                error!("Failed to send heartbeats: {}", e);
            }

            self.check_follower_health().await;
        }
    }

    /// Send heartbeats to all followers
    async fn send_heartbeats(&self, storage: Arc<HybridStorage>) -> Result<()> {
        let commit_index = self.get_commit_index(storage).await?;

        let followers: Vec<BrokerId> = {
            let follower_states = self.follower_states.read().await;
            follower_states.keys().copied().collect()
        };

        let heartbeat = ReplicationMessage::Heartbeat {
            leader_id: self.broker_id,
            term: self.term,
            commit_index,
        };

        if let Some(network_manager) = self.get_network_manager().await {
            for follower_id in followers {
                match network_manager
                    .send_replication_message(follower_id, heartbeat.clone())
                    .await
                {
                    Ok(_) => {
                        debug!(
                            "Successfully sent heartbeat to follower {} for {}:{}, term: {}, commit: {}",
                            follower_id, self.topic, self.partition, self.term, commit_index
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to send heartbeat to follower {}: {}",
                            follower_id, e
                        );
                        // Mark follower as potentially offline
                        self.mark_follower_unhealthy(follower_id).await;
                    }
                }
            }
        } else {
            // Fallback for local testing without network
            for follower_id in followers {
                debug!(
                    "No network manager available, simulating heartbeat to follower {} for {}:{}, term: {}, commit: {}",
                    follower_id, self.topic, self.partition, self.term, commit_index
                );
            }
        }

        Ok(())
    }

    /// Check health of followers and remove unhealthy ones from ISR
    async fn check_follower_health(&self) {
        let now = SystemTime::now();
        let timeout = Duration::from_millis(self.config.replication_timeout_ms);

        let mut followers = self.follower_states.write().await;
        for (follower_id, progress) in followers.iter_mut() {
            if now
                .duration_since(progress.last_heartbeat)
                .unwrap_or(timeout)
                > timeout
            {
                if progress.is_alive {
                    warn!(
                        "Follower {} is no longer responding, marking as offline",
                        follower_id
                    );
                    progress.is_alive = false;
                }
            }
        }
    }

    /// Handle responses from followers
    async fn handle_responses(&self) {
        let response_rx = {
            let mut rx_guard = self.response_rx.lock().await;
            if let Some(rx) = rx_guard.take() {
                rx
            } else {
                return;
            }
        };

        loop {
            // Use spawn_blocking to handle crossbeam channel
            let result = tokio::task::spawn_blocking({
                let rx_clone = response_rx.clone();
                move || rx_clone.recv()
            })
            .await;

            match result {
                Ok(Ok(response)) => {
                    if let Err(e) = self.process_response(response).await {
                        error!("Failed to process follower response: {}", e);
                    }
                }
                Ok(Err(_)) => break, // Channel closed
                Err(_) => break,     // spawn_blocking failed
            }
        }
    }

    /// Process a response from a follower
    async fn process_response(&self, response: ReplicationMessage) -> Result<()> {
        match response {
            ReplicationMessage::ReplicateResponse {
                follower_id,
                success,
                last_log_offset,
                ..
            } => {
                if success {
                    // Update follower progress
                    {
                        let mut match_indices = self.match_index.write().await;
                        match_indices.insert(follower_id, last_log_offset);
                    }

                    {
                        let mut next_indices = self.next_index.write().await;
                        next_indices.insert(follower_id, last_log_offset + 1);
                    }

                    {
                        let mut followers = self.follower_states.write().await;
                        if let Some(progress) = followers.get_mut(&follower_id) {
                            progress.match_index = last_log_offset;
                            progress.next_index = last_log_offset + 1;
                            progress.last_heartbeat = SystemTime::now();
                            progress.is_alive = true;
                        }
                    }

                    debug!(
                        "Updated follower {} progress: match_index={}, next_index={}",
                        follower_id,
                        last_log_offset,
                        last_log_offset + 1
                    );
                } else {
                    // Replication failed, decrement next_index and retry
                    {
                        let mut next_indices = self.next_index.write().await;
                        if let Some(next_idx) = next_indices.get_mut(&follower_id) {
                            *next_idx = (*next_idx).saturating_sub(1);
                        }
                    }

                    warn!(
                        "Replication failed for follower {}, will retry",
                        follower_id
                    );
                }
            }
            ReplicationMessage::HeartbeatResponse {
                follower_id,
                term,
                success,
            } => {
                if success && term == self.term {
                    let mut followers = self.follower_states.write().await;
                    if let Some(progress) = followers.get_mut(&follower_id) {
                        progress.last_heartbeat = SystemTime::now();
                        progress.is_alive = true;
                    }

                    debug!("Received heartbeat response from follower {}", follower_id);
                } else if term > self.term {
                    warn!(
                        "Received heartbeat response with higher term {} from follower {}, stepping down",
                        term, follower_id
                    );
                    // In a full implementation, would step down from leadership
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Get the current in-sync replica set
    pub async fn get_isr(&self) -> Vec<BrokerId> {
        let mut isr = vec![self.broker_id]; // Leader is always in ISR

        let followers = self.follower_states.read().await;
        for (follower_id, progress) in followers.iter() {
            if progress.is_alive {
                isr.push(*follower_id);
            }
        }

        isr
    }

    /// Set the network manager for this leader
    pub async fn set_network_manager(&self, network_manager: Arc<ReplicationNetworkManager>) {
        let mut nm = self.network_manager.write().await;
        *nm = Some(Arc::downgrade(&network_manager));
    }

    /// Get the network manager if available
    async fn get_network_manager(&self) -> Option<Arc<ReplicationNetworkManager>> {
        let nm = self.network_manager.read().await;
        if let Some(weak_nm) = nm.as_ref() {
            weak_nm.upgrade()
        } else {
            None
        }
    }

    /// Mark a follower as unhealthy for faster detection
    async fn mark_follower_unhealthy(&self, follower_id: BrokerId) {
        let mut followers = self.follower_states.write().await;
        if let Some(progress) = followers.get_mut(&follower_id) {
            progress.is_alive = false;
            warn!(
                "Marked follower {} as unhealthy for {}:{}",
                follower_id, self.topic, self.partition
            );
        }
    }
}

/// Manages replication for all partitions on this broker
pub struct ReplicationManager {
    broker_id: BrokerId,
    leaders: Arc<RwLock<HashMap<(TopicName, PartitionId), Arc<LeaderState>>>>,
    storage: Arc<HybridStorage>,
    config: ReplicationConfig,
}

impl ReplicationManager {
    pub fn new(
        broker_id: BrokerId,
        storage: Arc<HybridStorage>,
        config: ReplicationConfig,
    ) -> Self {
        Self {
            broker_id,
            leaders: Arc::new(RwLock::new(HashMap::new())),
            storage,
            config,
        }
    }

    /// Add a new leader partition
    pub async fn add_leader(
        &self,
        topic: &str,
        partition: PartitionId,
        followers: Vec<BrokerId>,
    ) -> Result<()> {
        let leader_state = LeaderState::new(
            self.broker_id,
            topic.to_string(),
            partition,
            followers,
            self.config.clone(),
        );

        let leader_state_arc = Arc::new(leader_state);
        leader_state_arc
            .clone()
            .start_replication(Arc::clone(&self.storage))
            .await?;

        {
            let mut leaders = self.leaders.write().await;
            leaders.insert((topic.to_string(), partition), leader_state_arc);
        }

        info!("Added leader for {}:{}", topic, partition);
        Ok(())
    }

    /// Handle new messages for a partition (if this broker is the leader)
    pub async fn handle_messages(
        &self,
        topic: &str,
        partition: PartitionId,
        messages: Vec<Message>,
        base_offset: Offset,
    ) -> Result<()> {
        let leader_state = {
            let leaders = self.leaders.read().await;
            leaders.get(&(topic.to_string(), partition)).cloned()
        };

        if let Some(leader) = leader_state {
            leader
                .replicate_messages(Arc::clone(&self.storage), messages, base_offset)
                .await?;
        }

        Ok(())
    }
}
