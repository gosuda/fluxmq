use super::{
    BrokerId, LogEntry, NetworkConfig, RaftConfig, RaftMessage, RaftNode, RaftState,
    ReplicationConfig, ReplicationCoordinator, ReplicationNetworkManager,
};
use crate::protocol::{Message, Offset, PartitionId, TopicName};
use crate::storage::HybridStorage;
use crate::Result;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

/// Cluster configuration for multi-broker replication
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This broker's ID
    pub broker_id: BrokerId,
    /// List of all broker IDs in the cluster
    pub brokers: Vec<BrokerId>,
    /// Network addresses for each broker
    pub broker_addresses: HashMap<BrokerId, SocketAddr>,
    /// Replication configuration
    pub replication_config: ReplicationConfig,
    /// Raft consensus configuration
    pub raft_config: RaftConfig,
    /// Network configuration
    pub network_config: NetworkConfig,
    /// Listen address for replication network
    pub listen_addr: SocketAddr,
}

impl ClusterConfig {
    pub fn new_single_node(broker_id: BrokerId, listen_addr: SocketAddr) -> Self {
        Self {
            broker_id,
            brokers: vec![broker_id],
            broker_addresses: HashMap::new(),
            replication_config: ReplicationConfig::default(),
            raft_config: RaftConfig::default(),
            network_config: NetworkConfig::default(),
            listen_addr,
        }
    }

    pub fn new_cluster(
        broker_id: BrokerId,
        brokers: Vec<BrokerId>,
        broker_addresses: HashMap<BrokerId, SocketAddr>,
        listen_addr: SocketAddr,
    ) -> Self {
        Self {
            broker_id,
            brokers,
            broker_addresses,
            replication_config: ReplicationConfig::default(),
            raft_config: RaftConfig::default(),
            network_config: NetworkConfig::default(),
            listen_addr,
        }
    }
}

/// Cluster coordinator that manages Raft consensus and replication
pub struct ClusterCoordinator {
    config: ClusterConfig,
    storage: Arc<HybridStorage>,

    /// Raft nodes for each partition
    raft_nodes: Arc<RwLock<HashMap<(TopicName, PartitionId), Arc<RaftNode>>>>,

    /// Network manager for inter-broker communication
    network_manager: Arc<ReplicationNetworkManager>,

    /// Traditional replication coordinator (for compatibility)
    replication_coordinator: Arc<ReplicationCoordinator>,

    /// Channel for sending RPC messages (bounded to prevent OOM under network partitions)
    rpc_sender: mpsc::Sender<(BrokerId, RaftMessage)>,
    rpc_receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<(BrokerId, RaftMessage)>>>,

    /// Channel for applying committed entries (bounded to provide backpressure).
    /// Uses Arc<str> instead of String to avoid per-message allocation in forwarders.
    apply_sender: mpsc::Sender<(Arc<str>, PartitionId, LogEntry)>,
    apply_receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<(Arc<str>, PartitionId, LogEntry)>>>,
}

impl ClusterCoordinator {
    pub fn new(config: ClusterConfig, storage: Arc<HybridStorage>) -> Self {
        let network_manager = Arc::new(ReplicationNetworkManager::new(
            config.broker_id,
            config.listen_addr,
            config.network_config.clone(),
        ));

        let replication_coordinator = Arc::new(ReplicationCoordinator::new(
            config.broker_id,
            config.replication_config.clone(),
        ));

        const RPC_CHANNEL_CAPACITY: usize = 10_000;
        const APPLY_CHANNEL_CAPACITY: usize = 10_000;
        let (rpc_sender, rpc_receiver) = mpsc::channel(RPC_CHANNEL_CAPACITY);
        let (apply_sender, apply_receiver) = mpsc::channel(APPLY_CHANNEL_CAPACITY);

        Self {
            config,
            storage,
            raft_nodes: Arc::new(RwLock::new(HashMap::new())),
            network_manager,
            replication_coordinator,
            rpc_sender,
            rpc_receiver: Arc::new(tokio::sync::Mutex::new(rpc_receiver)),
            apply_sender,
            apply_receiver: Arc::new(tokio::sync::Mutex::new(apply_receiver)),
        }
    }

    /// Start the cluster coordinator
    pub async fn start(&self) -> Result<()> {
        info!(
            "Starting cluster coordinator for broker {}",
            self.config.broker_id
        );

        // Start network manager
        self.network_manager.start().await?;

        // Register with other brokers
        for (&broker_id, &addr) in &self.config.broker_addresses {
            if broker_id != self.config.broker_id {
                self.network_manager.register_broker(broker_id, addr).await;
            }
        }

        // Connect to all brokers
        self.network_manager.connect_to_all_brokers().await?;

        // Start RPC handler
        self.start_rpc_handler().await;

        // Start apply handler
        self.start_apply_handler().await;

        info!("Cluster coordinator started successfully");
        Ok(())
    }

    /// Create a partition with Raft consensus
    pub async fn create_partition(
        &self,
        topic: &str,
        partition: PartitionId,
        replica_brokers: Vec<BrokerId>,
    ) -> Result<()> {
        info!(
            "Creating partition {}:{} with replicas {:?}",
            topic, partition, replica_brokers
        );

        // Determine if we are part of this partition's replica set
        if !replica_brokers.contains(&self.config.broker_id) {
            debug!("Not part of replica set for {}:{}", topic, partition);
            return Ok(());
        }

        // Create Raft node for this partition
        let peers: Vec<BrokerId> = replica_brokers
            .iter()
            .filter(|&&id| id != self.config.broker_id)
            .copied()
            .collect();

        let partition_apply_sender = {
            let topic: Arc<str> = Arc::from(topic);
            let apply_sender = self.apply_sender.clone();
            let (tx, mut rx) = mpsc::channel(10_000);

            // Spawn a task to forward partition-specific entries.
            // Arc<str> clone is an atomic increment — no String allocation per message.
            tokio::spawn(async move {
                while let Some(entry) = rx.recv().await {
                    if let Err(e) = apply_sender.send((topic.clone(), partition, entry)).await {
                        error!("Failed to forward apply entry: {}", e);
                        break;
                    }
                }
            });

            tx
        };

        let raft_node = Arc::new(RaftNode::new(
            self.config.broker_id,
            peers,
            Arc::clone(&self.storage),
            self.config.raft_config.clone(),
            self.rpc_sender.clone(),
            partition_apply_sender,
        ));

        // Start the Raft node
        raft_node.clone().start().await?;

        // Store the Raft node
        {
            let mut nodes = self.raft_nodes.write().await;
            nodes.insert((topic.to_string(), partition), raft_node);
        }

        // Also register with traditional coordinator for compatibility
        if replica_brokers.first() == Some(&self.config.broker_id) {
            // We are the primary replica (leader candidate)
            self.replication_coordinator
                .become_leader(topic, partition, replica_brokers.clone())
                .await?;
        } else if let Some(&leader_id) = replica_brokers.first() {
            // We are a follower
            self.replication_coordinator
                .become_follower(topic, partition, leader_id)
                .await?;
        }

        info!("Created partition {}:{} successfully", topic, partition);
        Ok(())
    }

    /// Propose a message to a partition (will be replicated via Raft)
    pub async fn propose_message(
        &self,
        topic: &str,
        partition: PartitionId,
        message: Message,
        base_offset: Offset,
    ) -> Result<u64> {
        let raft_node = {
            let nodes = self.raft_nodes.read().await;
            nodes.get(&(topic.to_string(), partition)).cloned()
        };

        if let Some(node) = raft_node {
            let log_entry = LogEntry {
                offset: base_offset,
                term: node.get_current_term().await,
                message,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            };

            node.propose(log_entry).await
        } else {
            Err(crate::FluxmqError::Replication(format!(
                "No Raft node found for partition {}:{}",
                topic, partition
            )))
        }
    }

    /// Get the current leader for a partition
    pub async fn get_partition_leader(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Option<BrokerId> {
        let raft_node = {
            let nodes = self.raft_nodes.read().await;
            nodes.get(&(topic.to_string(), partition)).cloned()
        };

        if let Some(node) = raft_node {
            node.get_leader().await
        } else {
            // Fallback to traditional coordinator
            self.replication_coordinator
                .get_leader(topic, partition)
                .await
        }
    }

    /// Check if this broker is the leader for a partition
    pub async fn is_partition_leader(&self, topic: &str, partition: PartitionId) -> bool {
        let raft_node = {
            let nodes = self.raft_nodes.read().await;
            nodes.get(&(topic.to_string(), partition)).cloned()
        };

        if let Some(node) = raft_node {
            matches!(node.get_state().await, RaftState::Leader)
        } else {
            // Fallback to traditional coordinator
            self.replication_coordinator
                .is_leader(topic, partition)
                .await
        }
    }

    /// Get cluster status information
    pub async fn get_cluster_status(&self) -> ClusterStatus {
        let mut partition_leaders = HashMap::new();
        let mut partition_states = HashMap::new();

        let nodes = self.raft_nodes.read().await;
        for ((topic, partition), node) in nodes.iter() {
            let state = node.get_state().await;
            let leader = node.get_leader().await;

            partition_states.insert((topic.clone(), *partition), state);
            if let Some(leader_id) = leader {
                partition_leaders.insert((topic.clone(), *partition), leader_id);
            }
        }

        let network_stats = self.network_manager.get_stats().await;

        ClusterStatus {
            broker_id: self.config.broker_id,
            partition_leaders,
            partition_states,
            connected_brokers: network_stats.active_connections,
            total_brokers: self.config.brokers.len(),
        }
    }

    /// Start RPC message handler
    async fn start_rpc_handler(&self) {
        let rpc_receiver = Arc::clone(&self.rpc_receiver);
        let network_manager = Arc::clone(&self.network_manager);
        let _raft_nodes = Arc::clone(&self.raft_nodes);

        tokio::spawn(async move {
            let mut receiver = rpc_receiver.lock().await;

            while let Some((target_broker, _message)) = receiver.recv().await {
                // Send RPC to target broker
                if let Err(e) = network_manager
                    .send_replication_message(
                        target_broker,
                        super::ReplicationMessage::Heartbeat {
                            leader_id: 0, // This would be properly implemented
                            term: 0,
                            commit_index: 0,
                        },
                    )
                    .await
                {
                    warn!("Failed to send RPC to broker {}: {}", target_broker, e);
                }
            }
        });
    }

    /// Start apply handler for committed entries
    async fn start_apply_handler(&self) {
        let apply_receiver = Arc::clone(&self.apply_receiver);
        let storage = Arc::clone(&self.storage);

        tokio::spawn(async move {
            let mut receiver = apply_receiver.lock().await;

            while let Some((topic, partition, log_entry)) = receiver.recv().await {
                // Apply the committed entry to storage
                match storage.append_messages(&topic, partition, vec![log_entry.message]) {
                    Ok(offset) => {
                        debug!(
                            "Applied entry to {}:{} at offset {}",
                            topic, partition, offset
                        );
                    }
                    Err(e) => {
                        error!("Failed to apply entry to {}:{}: {}", topic, partition, e);
                    }
                }
            }
        });
    }

    /// Handle incoming replication message (from network)
    pub async fn handle_replication_message(
        &self,
        from_broker: BrokerId,
        _message: super::ReplicationMessage,
    ) -> Result<()> {
        // This would route messages to appropriate Raft nodes
        // For now, we'll just log it
        debug!("Received replication message from broker {}", from_broker);
        Ok(())
    }
}

/// Status information for the cluster
#[derive(Debug, Clone)]
pub struct ClusterStatus {
    pub broker_id: BrokerId,
    pub partition_leaders: HashMap<(TopicName, PartitionId), BrokerId>,
    pub partition_states: HashMap<(TopicName, PartitionId), RaftState>,
    pub connected_brokers: Vec<BrokerId>,
    pub total_brokers: usize,
}

impl ClusterStatus {
    /// Check if the cluster has a healthy majority
    pub fn has_majority(&self) -> bool {
        let connected = self.connected_brokers.len() + 1; // +1 for self
        connected > self.total_brokers / 2
    }

    /// Get leadership information
    pub fn get_leadership_summary(&self) -> HashMap<RaftState, usize> {
        let mut summary = HashMap::new();

        for state in self.partition_states.values() {
            *summary.entry(*state).or_insert(0) += 1;
        }

        summary
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn test_cluster_config_creation() {
        let broker_id = 1;
        let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9093);

        let config = ClusterConfig::new_single_node(broker_id, listen_addr);
        assert_eq!(config.broker_id, broker_id);
        assert_eq!(config.brokers, vec![broker_id]);
        assert_eq!(config.listen_addr, listen_addr);
    }

    #[tokio::test]
    async fn test_cluster_status() {
        let mut status = ClusterStatus {
            broker_id: 1,
            partition_leaders: HashMap::new(),
            partition_states: HashMap::new(),
            connected_brokers: vec![2, 3],
            total_brokers: 3,
        };

        // Test majority calculation
        assert!(status.has_majority()); // 3/3 connected (including self)

        status.connected_brokers = vec![2]; // Only 2/3 connected
        assert!(status.has_majority()); // Still majority

        status.connected_brokers = vec![]; // Only 1/3 connected
        assert!(!status.has_majority()); // No majority

        // Test leadership summary
        status
            .partition_states
            .insert(("topic1".to_string(), 0), RaftState::Leader);
        status
            .partition_states
            .insert(("topic1".to_string(), 1), RaftState::Follower);
        status
            .partition_states
            .insert(("topic2".to_string(), 0), RaftState::Follower);

        let summary = status.get_leadership_summary();
        assert_eq!(summary.get(&RaftState::Leader), Some(&1));
        assert_eq!(summary.get(&RaftState::Follower), Some(&2));
    }
}
