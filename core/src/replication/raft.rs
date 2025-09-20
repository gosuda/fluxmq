use super::{BrokerId, LogEntry};
use crate::storage::HybridStorage;
use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, timeout};
use tracing::{debug, error, info, warn};

/// Raft node state
#[derive(Debug, Clone, Copy, PartialEq, Hash, Eq)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

/// Raft configuration parameters
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Election timeout range in milliseconds (min, max)
    pub election_timeout_ms: (u64, u64),
    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u64,
    /// RPC timeout in milliseconds
    pub rpc_timeout_ms: u64,
    /// Maximum number of log entries per AppendEntries RPC
    pub max_entries_per_request: usize,
    /// Log compaction threshold (number of entries)
    pub log_compaction_threshold: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_ms: (150, 300),
            heartbeat_interval_ms: 50,
            rpc_timeout_ms: 100,
            max_entries_per_request: 100,
            log_compaction_threshold: 1000,
        }
    }
}

/// Raft RPC messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    /// RequestVote RPC
    RequestVote {
        term: u64,
        candidate_id: BrokerId,
        last_log_index: u64,
        last_log_term: u64,
    },
    /// RequestVote response
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
        voter_id: BrokerId,
    },
    /// AppendEntries RPC (includes heartbeat when entries is empty)
    AppendEntries {
        term: u64,
        leader_id: BrokerId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftLogEntry>,
        leader_commit: u64,
    },
    /// AppendEntries response
    AppendEntriesResponse {
        term: u64,
        success: bool,
        match_index: u64,
        follower_id: BrokerId,
    },
    /// InstallSnapshot RPC (for log compaction)
    InstallSnapshot {
        term: u64,
        leader_id: BrokerId,
        last_included_index: u64,
        last_included_term: u64,
        data: Vec<u8>,
        done: bool,
    },
    /// InstallSnapshot response
    InstallSnapshotResponse { term: u64, follower_id: BrokerId },
}

/// Raft log entry wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftLogEntry {
    pub index: u64,
    pub term: u64,
    pub data: LogEntry,
}

/// Persistent state on all servers (updated on stable storage before responding to RPCs)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    /// Latest term server has seen (initialized to 0, increases monotonically)
    pub current_term: u64,
    /// Candidate ID that received vote in current term (or None)
    pub voted_for: Option<BrokerId>,
    /// Log entries; each entry contains command for state machine, and term when entry was received by leader
    pub log: Vec<RaftLogEntry>,
}

impl PersistentState {
    pub fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
        }
    }

    /// Get the last log index
    pub fn last_log_index(&self) -> u64 {
        self.log.len().saturating_sub(1) as u64
    }

    /// Get the last log term
    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|entry| entry.term).unwrap_or(0)
    }

    /// Get log entry at index
    pub fn get_entry(&self, index: u64) -> Option<&RaftLogEntry> {
        self.log.get(index as usize)
    }

    /// Get term at index
    pub fn get_term(&self, index: u64) -> u64 {
        self.get_entry(index).map(|entry| entry.term).unwrap_or(0)
    }

    /// Append new entries starting at index
    pub fn append_entries(&mut self, start_index: u64, entries: Vec<RaftLogEntry>) {
        // Delete any conflicting entries
        self.log.truncate(start_index as usize);
        // Append new entries
        self.log.extend(entries);
    }
}

/// Volatile state on all servers
#[derive(Debug)]
pub struct VolatileState {
    /// Index of highest log entry known to be committed
    pub commit_index: AtomicU64,
    /// Index of highest log entry applied to state machine
    pub last_applied: AtomicU64,
}

impl VolatileState {
    pub fn new() -> Self {
        Self {
            commit_index: AtomicU64::new(0),
            last_applied: AtomicU64::new(0),
        }
    }
}

/// Volatile state on leaders (reinitialized after election)
#[derive(Debug)]
pub struct LeaderState {
    /// For each server, index of the next log entry to send to that server
    pub next_index: HashMap<BrokerId, u64>,
    /// For each server, index of highest log entry known to be replicated on server
    pub match_index: HashMap<BrokerId, u64>,
}

impl LeaderState {
    pub fn new(peers: &[BrokerId], last_log_index: u64) -> Self {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();

        for &peer in peers {
            next_index.insert(peer, last_log_index + 1);
            match_index.insert(peer, 0);
        }

        Self {
            next_index,
            match_index,
        }
    }

    /// Update follower progress
    pub fn update_progress(&mut self, follower: BrokerId, match_idx: u64) {
        self.match_index.insert(follower, match_idx);
        self.next_index.insert(follower, match_idx + 1);
    }

    /// Calculate commit index based on majority replication
    pub fn calculate_commit_index(&self, current_commit: u64, total_nodes: usize) -> u64 {
        let mut indices: Vec<u64> = self.match_index.values().copied().collect();
        indices.sort_unstable();
        indices.reverse();

        // Find the highest index that's replicated on a majority
        let majority = (total_nodes / 2) + 1;
        if indices.len() >= majority - 1 {
            // -1 because we don't include ourselves in match_index
            indices[majority - 2].max(current_commit)
        } else {
            current_commit
        }
    }
}

/// Core Raft consensus implementation
pub struct RaftNode {
    /// This node's ID
    node_id: BrokerId,
    /// IDs of all nodes in the cluster
    peers: Vec<BrokerId>,
    /// Current state
    state: Arc<RwLock<RaftState>>,
    /// Persistent state
    persistent: Arc<RwLock<PersistentState>>,
    /// Volatile state
    volatile: Arc<VolatileState>,
    /// Leader state (only valid when we are leader)
    leader_state: Arc<RwLock<Option<LeaderState>>>,
    /// Configuration
    config: RaftConfig,
    /// Last time we heard from a leader
    last_heartbeat: Arc<RwLock<Instant>>,
    /// Storage for state machine
    _storage: Arc<HybridStorage>,
    /// Network sender for outgoing RPCs
    rpc_sender: mpsc::UnboundedSender<(BrokerId, RaftMessage)>,
    /// Channel for applying committed entries
    apply_sender: mpsc::UnboundedSender<LogEntry>,
}

impl RaftNode {
    pub fn new(
        node_id: BrokerId,
        peers: Vec<BrokerId>,
        storage: Arc<HybridStorage>,
        config: RaftConfig,
        rpc_sender: mpsc::UnboundedSender<(BrokerId, RaftMessage)>,
        apply_sender: mpsc::UnboundedSender<LogEntry>,
    ) -> Self {
        Self {
            node_id,
            peers,
            state: Arc::new(RwLock::new(RaftState::Follower)),
            persistent: Arc::new(RwLock::new(PersistentState::new())),
            volatile: Arc::new(VolatileState::new()),
            leader_state: Arc::new(RwLock::new(None)),
            config,
            last_heartbeat: Arc::new(RwLock::new(Instant::now())),
            _storage: storage,
            rpc_sender,
            apply_sender,
        }
    }

    /// Start the Raft node
    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!("Starting Raft node {}", self.node_id);

        // For single-node clusters, immediately start an election
        if self.peers.is_empty() {
            info!("Single-node cluster detected, starting immediate election");
            let election_node = Arc::clone(&self);
            tokio::spawn(async move {
                // Small delay to ensure node is fully initialized
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                election_node.start_election().await;
            });
        }

        // Start main loop
        let main_loop_node = Arc::clone(&self);
        tokio::spawn(async move {
            main_loop_node.main_loop().await;
        });

        // Start apply loop
        let apply_node = Arc::clone(&self);
        tokio::spawn(async move {
            apply_node.apply_loop().await;
        });

        Ok(())
    }

    /// Main Raft loop
    async fn main_loop(&self) {
        let mut election_timer = interval(Duration::from_millis(self.config.election_timeout_ms.0));
        let mut heartbeat_timer =
            interval(Duration::from_millis(self.config.heartbeat_interval_ms));

        loop {
            tokio::select! {
                _ = election_timer.tick() => {
                    self.handle_election_timeout().await;
                }
                _ = heartbeat_timer.tick() => {
                    self.handle_heartbeat_timer().await;
                }
            }
        }
    }

    /// Handle election timeout (follower/candidate only)
    async fn handle_election_timeout(&self) {
        let state = *self.state.read().await;
        let last_heartbeat = *self.last_heartbeat.read().await;
        // Simple random timeout between min and max
        let timeout_range = self.config.election_timeout_ms.1 - self.config.election_timeout_ms.0;
        let random_offset = (std::ptr::addr_of!(self) as usize % timeout_range as usize) as u64;
        let election_timeout =
            Duration::from_millis(self.config.election_timeout_ms.0 + random_offset);

        if matches!(state, RaftState::Leader) {
            return; // Leaders don't have election timeouts
        }

        if last_heartbeat.elapsed() >= election_timeout {
            info!(
                "Election timeout for node {}, starting election",
                self.node_id
            );
            self.start_election().await;
        }
    }

    /// Handle heartbeat timer (leader only)
    async fn handle_heartbeat_timer(&self) {
        let state = *self.state.read().await;
        if matches!(state, RaftState::Leader) {
            self.send_heartbeats().await;
        }
    }

    /// Start a new election
    async fn start_election(&self) {
        // Transition to candidate
        {
            let mut state = self.state.write().await;
            *state = RaftState::Candidate;
        }

        // Increment term and vote for self
        let (current_term, last_log_index, last_log_term) = {
            let mut persistent = self.persistent.write().await;
            persistent.current_term += 1;
            persistent.voted_for = Some(self.node_id);
            (
                persistent.current_term,
                persistent.last_log_index(),
                persistent.last_log_term(),
            )
        };

        info!(
            "Node {} starting election for term {}",
            self.node_id, current_term
        );

        // Reset heartbeat timer
        {
            let mut last_heartbeat = self.last_heartbeat.write().await;
            *last_heartbeat = Instant::now();
        }

        // Send RequestVote RPCs to all peers
        let mut votes_received = 1; // Vote for self
        let mut responses_received = 0;
        let total_nodes = self.peers.len() + 1; // Including self
        let majority = (total_nodes / 2) + 1;

        // Create a channel to collect vote responses
        let (_vote_tx, mut vote_rx) = mpsc::unbounded_channel();

        // Send vote requests
        for &peer in &self.peers {
            let request = RaftMessage::RequestVote {
                term: current_term,
                candidate_id: self.node_id,
                last_log_index,
                last_log_term,
            };

            if let Err(e) = self.rpc_sender.send((peer, request)) {
                warn!("Failed to send vote request to peer {}: {}", peer, e);
            }
        }

        // Special case: single-node cluster
        if self.peers.is_empty() {
            // Only one node in cluster, automatically become leader
            info!("Single-node cluster, becoming leader immediately");
            self.become_leader(current_term).await;
            return;
        }

        // Collect responses with timeout
        let election_timeout = Duration::from_millis(self.config.election_timeout_ms.1);
        let deadline = tokio::time::Instant::now() + election_timeout;

        while responses_received < self.peers.len() {
            match timeout(deadline - tokio::time::Instant::now(), vote_rx.recv()).await {
                Ok(Some(granted)) => {
                    responses_received += 1;
                    if granted {
                        votes_received += 1;
                    }

                    // Check if we have majority
                    if votes_received >= majority {
                        self.become_leader(current_term).await;
                        return;
                    }

                    // Check if we can't possibly win
                    let remaining_votes = self.peers.len() - responses_received;
                    if votes_received + remaining_votes < majority {
                        break;
                    }
                }
                Ok(None) => break, // Channel closed
                Err(_) => break,   // Timeout
            }
        }

        // Election failed or timed out
        info!(
            "Election failed for node {} in term {}",
            self.node_id, current_term
        );

        // Revert to follower
        {
            let mut state = self.state.write().await;
            *state = RaftState::Follower;
        }
    }

    /// Become leader after winning election
    async fn become_leader(&self, term: u64) {
        info!("Node {} became leader for term {}", self.node_id, term);

        // Transition to leader
        {
            let mut state = self.state.write().await;
            *state = RaftState::Leader;
        }

        // Initialize leader state
        let last_log_index = {
            let persistent = self.persistent.read().await;
            persistent.last_log_index()
        };

        {
            let mut leader_state = self.leader_state.write().await;
            *leader_state = Some(LeaderState::new(&self.peers, last_log_index));
        }

        // Send immediate heartbeat to establish leadership
        self.send_heartbeats().await;
    }

    /// Send heartbeats to all followers (leader only)
    async fn send_heartbeats(&self) {
        let (current_term, commit_index) = {
            let persistent = self.persistent.read().await;
            (
                persistent.current_term,
                self.volatile.commit_index.load(Ordering::Acquire),
            )
        };

        for &peer in &self.peers {
            let (prev_log_index, prev_log_term, entries) = {
                let persistent = self.persistent.read().await;
                let leader_state = self.leader_state.read().await;

                if let Some(ref leader) = *leader_state {
                    let next_index = leader.next_index.get(&peer).copied().unwrap_or(0);
                    let prev_log_index = next_index.saturating_sub(1);
                    let prev_log_term = persistent.get_term(prev_log_index);

                    // For heartbeat, send empty entries
                    // In a full implementation, this would send actual entries when needed
                    (prev_log_index, prev_log_term, Vec::new())
                } else {
                    (0, 0, Vec::new())
                }
            };

            let heartbeat = RaftMessage::AppendEntries {
                term: current_term,
                leader_id: self.node_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: commit_index,
            };

            if let Err(e) = self.rpc_sender.send((peer, heartbeat)) {
                warn!("Failed to send heartbeat to peer {}: {}", peer, e);
            }
        }

        debug!("Sent heartbeats to {} peers", self.peers.len());
    }

    /// Process incoming RPC message
    pub async fn handle_rpc(&self, _from: BrokerId, message: RaftMessage) -> Option<RaftMessage> {
        match message {
            RaftMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                self.handle_request_vote(term, candidate_id, last_log_index, last_log_term)
                    .await
            }
            RaftMessage::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                self.handle_append_entries(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                )
                .await
            }
            RaftMessage::RequestVoteResponse {
                term,
                vote_granted,
                voter_id,
            } => {
                self.handle_vote_response(term, vote_granted, voter_id)
                    .await;
                None
            }
            RaftMessage::AppendEntriesResponse {
                term,
                success,
                match_index,
                follower_id,
            } => {
                self.handle_append_response(term, success, match_index, follower_id)
                    .await;
                None
            }
            _ => None, // InstallSnapshot not implemented yet
        }
    }

    /// Handle RequestVote RPC
    async fn handle_request_vote(
        &self,
        term: u64,
        candidate_id: BrokerId,
        last_log_index: u64,
        last_log_term: u64,
    ) -> Option<RaftMessage> {
        let mut vote_granted = false;

        {
            let mut persistent = self.persistent.write().await;

            // If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower
            if term > persistent.current_term {
                persistent.current_term = term;
                persistent.voted_for = None;

                let mut state = self.state.write().await;
                *state = RaftState::Follower;
            }

            // Grant vote if:
            // 1. Haven't voted for anyone else in this term
            // 2. Candidate's log is at least as up-to-date as ours
            if term >= persistent.current_term
                && (persistent.voted_for.is_none() || persistent.voted_for == Some(candidate_id))
            {
                let our_last_log_term = persistent.last_log_term();
                let our_last_log_index = persistent.last_log_index();

                let log_up_to_date = last_log_term > our_last_log_term
                    || (last_log_term == our_last_log_term && last_log_index >= our_last_log_index);

                if log_up_to_date {
                    persistent.voted_for = Some(candidate_id);
                    vote_granted = true;

                    // Reset heartbeat timer when granting vote
                    let mut last_heartbeat = self.last_heartbeat.write().await;
                    *last_heartbeat = Instant::now();

                    debug!(
                        "Granted vote to candidate {} for term {}",
                        candidate_id, term
                    );
                }
            }
        }

        let current_term = {
            let persistent = self.persistent.read().await;
            persistent.current_term
        };

        Some(RaftMessage::RequestVoteResponse {
            term: current_term,
            vote_granted,
            voter_id: self.node_id,
        })
    }

    /// Handle AppendEntries RPC
    async fn handle_append_entries(
        &self,
        term: u64,
        leader_id: BrokerId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftLogEntry>,
        leader_commit: u64,
    ) -> Option<RaftMessage> {
        let mut success = false;
        let mut match_index = 0;

        {
            let mut persistent = self.persistent.write().await;

            // If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower
            if term > persistent.current_term {
                persistent.current_term = term;
                persistent.voted_for = None;
            }

            if term >= persistent.current_term {
                // Valid leader, reset election timeout
                {
                    let mut last_heartbeat = self.last_heartbeat.write().await;
                    *last_heartbeat = Instant::now();
                }

                // Convert to follower if not already
                {
                    let mut state = self.state.write().await;
                    *state = RaftState::Follower;
                }

                // Check log consistency
                if prev_log_index == 0
                    || (prev_log_index <= persistent.last_log_index()
                        && persistent.get_term(prev_log_index) == prev_log_term)
                {
                    success = true;

                    // Append new entries (if any)
                    if !entries.is_empty() {
                        persistent.append_entries(prev_log_index + 1, entries.clone());
                        debug!(
                            "Appended {} entries from leader {}",
                            entries.len(),
                            leader_id
                        );
                    }

                    match_index = persistent.last_log_index();

                    // Update commit index
                    if leader_commit > self.volatile.commit_index.load(Ordering::Acquire) {
                        let new_commit = leader_commit.min(persistent.last_log_index());
                        self.volatile
                            .commit_index
                            .store(new_commit, Ordering::Release);
                        debug!("Updated commit index to {}", new_commit);
                    }
                } else {
                    debug!(
                        "Log inconsistency detected for entries from leader {}",
                        leader_id
                    );
                }
            }
        }

        let current_term = {
            let persistent = self.persistent.read().await;
            persistent.current_term
        };

        Some(RaftMessage::AppendEntriesResponse {
            term: current_term,
            success,
            match_index,
            follower_id: self.node_id,
        })
    }

    /// Handle vote response
    async fn handle_vote_response(&self, term: u64, vote_granted: bool, _voter_id: BrokerId) {
        let current_term = {
            let persistent = self.persistent.read().await;
            persistent.current_term
        };

        // Ignore stale responses
        if term != current_term {
            return;
        }

        debug!(
            "Received vote response from {}: granted={}",
            _voter_id, vote_granted
        );

        // In a full implementation, this would be coordinated with the election process
        // For now, we just log it
    }

    /// Handle append entries response
    async fn handle_append_response(
        &self,
        term: u64,
        success: bool,
        match_index: u64,
        follower_id: BrokerId,
    ) {
        // Step down if we see a higher term
        if term > {
            let persistent = self.persistent.read().await;
            persistent.current_term
        } {
            let mut persistent = self.persistent.write().await;
            persistent.current_term = term;
            persistent.voted_for = None;

            let mut state = self.state.write().await;
            *state = RaftState::Follower;
            return;
        }

        // Only process if we're still leader
        let state = *self.state.read().await;
        if !matches!(state, RaftState::Leader) {
            return;
        }

        // Update follower progress
        {
            let mut leader_state_guard = self.leader_state.write().await;
            if let Some(ref mut leader_state) = *leader_state_guard {
                if success {
                    leader_state.update_progress(follower_id, match_index);
                    debug!(
                        "Updated progress for follower {}: match_index={}",
                        follower_id, match_index
                    );

                    // Try to advance commit index
                    let current_commit = self.volatile.commit_index.load(Ordering::Acquire);
                    let new_commit =
                        leader_state.calculate_commit_index(current_commit, self.peers.len() + 1);

                    if new_commit > current_commit {
                        self.volatile
                            .commit_index
                            .store(new_commit, Ordering::Release);
                        info!("Advanced commit index to {}", new_commit);
                    }
                } else {
                    // Decrement next_index for this follower and retry
                    if let Some(next_idx) = leader_state.next_index.get_mut(&follower_id) {
                        *next_idx = next_idx.saturating_sub(1);
                        debug!(
                            "Decremented next_index for follower {} to {}",
                            follower_id, *next_idx
                        );
                    }
                }
            }
        }
    }

    /// Apply loop for committed entries
    async fn apply_loop(&self) {
        let mut interval = interval(Duration::from_millis(10));

        loop {
            interval.tick().await;

            let commit_index = self.volatile.commit_index.load(Ordering::Acquire);
            let last_applied = self.volatile.last_applied.load(Ordering::Acquire);

            if commit_index > last_applied {
                let entries_to_apply = {
                    let persistent = self.persistent.read().await;
                    let mut entries = Vec::new();

                    for i in (last_applied + 1)..=commit_index {
                        if let Some(entry) = persistent.get_entry(i) {
                            entries.push(entry.data.clone());
                        }
                    }
                    entries
                };

                // Apply entries to state machine
                for entry in entries_to_apply {
                    if let Err(e) = self.apply_sender.send(entry) {
                        error!("Failed to send entry to apply channel: {}", e);
                        break;
                    }
                }

                self.volatile
                    .last_applied
                    .store(commit_index, Ordering::Release);
                debug!("Applied entries up to index {}", commit_index);
            }
        }
    }

    /// Propose a new log entry (leader only)
    pub async fn propose(&self, entry: LogEntry) -> Result<u64> {
        let state = *self.state.read().await;
        if !matches!(state, RaftState::Leader) {
            return Err(crate::FluxmqError::Replication("Not leader".to_string()));
        }

        let (index, term) = {
            let mut persistent = self.persistent.write().await;
            let index = persistent.log.len() as u64;
            let term = persistent.current_term;

            let raft_entry = RaftLogEntry {
                index,
                term,
                data: entry,
            };

            persistent.log.push(raft_entry);
            (index, term)
        };

        info!("Proposed entry at index {} term {}", index, term);

        // In a full implementation, this would trigger immediate replication
        // For now, entries will be replicated with the next heartbeat

        Ok(index)
    }

    /// Get current leader ID (if known)
    pub async fn get_leader(&self) -> Option<BrokerId> {
        let state = *self.state.read().await;
        match state {
            RaftState::Leader => Some(self.node_id),
            _ => None, // In a full implementation, followers would track current leader
        }
    }

    /// Get current term
    pub async fn get_current_term(&self) -> u64 {
        let persistent = self.persistent.read().await;
        persistent.current_term
    }

    /// Get current state
    pub async fn get_state(&self) -> RaftState {
        *self.state.read().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Message;

    #[tokio::test]
    async fn test_raft_node_creation() {
        let temp_dir = std::env::temp_dir().join("raft_test");
        let storage = Arc::new(HybridStorage::new(temp_dir.to_str().unwrap()).unwrap());
        let (rpc_tx, _rpc_rx) = mpsc::unbounded_channel();
        let (apply_tx, _apply_rx) = mpsc::unbounded_channel();

        let node = RaftNode::new(
            1,
            vec![2, 3],
            storage,
            RaftConfig::default(),
            rpc_tx,
            apply_tx,
        );

        assert_eq!(node.node_id, 1);
        assert_eq!(node.peers, vec![2, 3]);
        assert_eq!(*node.state.read().await, RaftState::Follower);
    }

    #[tokio::test]
    async fn test_persistent_state() {
        let mut state = PersistentState::new();
        assert_eq!(state.current_term, 0);
        assert_eq!(state.voted_for, None);
        assert_eq!(state.last_log_index(), 0);
        assert_eq!(state.last_log_term(), 0);

        // Add some entries
        let entries = vec![
            RaftLogEntry {
                index: 0,
                term: 1,
                data: LogEntry {
                    offset: 0,
                    term: 1,
                    message: Message::default(),
                    timestamp: 0,
                },
            },
            RaftLogEntry {
                index: 1,
                term: 1,
                data: LogEntry {
                    offset: 1,
                    term: 1,
                    message: Message::default(),
                    timestamp: 0,
                },
            },
        ];

        state.append_entries(0, entries);
        assert_eq!(state.last_log_index(), 1);
        assert_eq!(state.last_log_term(), 1);
    }

    #[tokio::test]
    async fn test_leader_state() {
        let peers = vec![2, 3, 4];
        let leader_state = LeaderState::new(&peers, 10);

        assert_eq!(leader_state.next_index.len(), 3);
        assert_eq!(leader_state.match_index.len(), 3);

        for &peer in &peers {
            assert_eq!(leader_state.next_index[&peer], 11);
            assert_eq!(leader_state.match_index[&peer], 0);
        }

        // Test commit index calculation
        let mut leader_state = leader_state;
        leader_state.match_index.insert(2, 5);
        leader_state.match_index.insert(3, 3);
        leader_state.match_index.insert(4, 4);

        // With 4 total nodes (including leader), majority is 3
        // Sorted match indices: [5, 4, 3], so majority index is 4
        let commit_index = leader_state.calculate_commit_index(0, 4);
        assert_eq!(commit_index, 3); // Second highest (majority)
    }
}
