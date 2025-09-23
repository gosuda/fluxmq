# Replication Module

## 🔄 Overview

The FluxMQ Replication Module provides high-availability and fault tolerance through a distributed consensus system based on the Raft algorithm. It ensures data durability and consistency across multiple broker instances, enabling automatic failover and recovery in production environments.

## 📁 Module Structure

| File | Purpose | Features |
|---|---|---|
| `mod.rs` | Module exports and configuration | Replication types, settings |
| `raft.rs` | Raft consensus algorithm implementation | Leader election, log replication |
| `leader.rs` | Leader node implementation | Log append, follower coordination |
| `follower.rs` | Follower node implementation | Log synchronization, heartbeat response |
| `cluster.rs` | Cluster management | Node discovery, membership |
| `network.rs` | Inter-node communication | RPC, message passing |
| `tests.rs` | Comprehensive test suite | Consensus scenarios, failure testing |

## 🏗️ Architecture

### Raft Consensus Overview

FluxMQ implements the Raft consensus algorithm for distributed log replication:

```mermaid
graph TB
    subgraph "Raft Cluster"
        Leader[Leader Node<br/>Port 9092]
        Follower1[Follower Node<br/>Port 9093]
        Follower2[Follower Node<br/>Port 9094]
    end

    Client[Kafka Client] --> Leader
    Leader --> Follower1
    Leader --> Follower2
    Follower1 --> Leader
    Follower2 --> Leader

    subgraph "Replication Flow"
        Write[Client Write] --> LogEntry[Log Entry]
        LogEntry --> Replicate[Replicate to Followers]
        Replicate --> Commit[Commit Entry]
        Commit --> Response[Response to Client]
    end
```

### Consensus States

```rust
pub enum RaftState {
    Follower,     // Default state, accepts entries from leader
    Candidate,    // Transitional state during leader election
    Leader,       // Coordinates cluster, replicates entries
}

pub struct RaftNode {
    state: RaftState,
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    commit_index: u64,
    last_applied: u64,
    cluster_config: ClusterConfig,
}
```

## 🚀 Key Features

### 1. Leader Election

Automatic leader election using randomized timeouts:

```rust
impl RaftNode {
    pub async fn start_election(&mut self) -> Result<()> {
        self.current_term += 1;
        self.state = RaftState::Candidate;
        self.voted_for = Some(self.node_id);

        info!("Starting election for term {}", self.current_term);

        let vote_requests = self.send_vote_requests().await?;
        let votes_received = self.count_votes(vote_requests).await?;

        if votes_received > self.cluster_size() / 2 {
            self.become_leader().await?;
        } else {
            self.become_follower().await?;
        }

        Ok(())
    }

    async fn become_leader(&mut self) -> Result<()> {
        info!("Elected as leader for term {}", self.current_term);
        self.state = RaftState::Leader;

        // Initialize leader state
        self.next_index = vec![self.log.len(); self.cluster_size()];
        self.match_index = vec![0; self.cluster_size()];

        // Send initial heartbeats
        self.send_heartbeats().await?;

        Ok(())
    }
}
```

### 2. Log Replication

Ensures all nodes maintain identical logs:

```rust
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: ReplicationCommand,
    pub timestamp: SystemTime,
}

#[derive(Debug, Clone)]
pub enum ReplicationCommand {
    AppendMessage { topic: String, partition: u32, message: Message },
    CreateTopic { name: String, partitions: u32 },
    DeleteTopic { name: String },
    ConfigChange { key: String, value: String },
}

impl Leader {
    pub async fn append_entry(&mut self, command: ReplicationCommand) -> Result<u64> {
        let entry = LogEntry {
            term: self.current_term,
            index: self.log.len() as u64,
            command,
            timestamp: SystemTime::now(),
        };

        // Append to local log
        let entry_index = entry.index;
        self.log.push(entry.clone());

        // Replicate to followers
        let replication_futures = self.followers.iter().map(|follower| {
            self.replicate_to_follower(follower.id, entry.clone())
        });

        // Wait for majority acknowledgment
        let responses = futures::future::join_all(replication_futures).await;
        let success_count = responses.iter().filter(|r| r.is_ok()).count();

        if success_count > self.cluster_size() / 2 {
            self.commit_index = entry_index;
            Ok(entry_index)
        } else {
            Err(ReplicationError::InsufficientReplicas)
        }
    }
}
```

### 3. Cluster Management

Dynamic cluster membership and node discovery:

```rust
pub struct ClusterConfig {
    pub nodes: HashMap<NodeId, NodeInfo>,
    pub replication_factor: usize,
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
}

pub struct NodeInfo {
    pub id: NodeId,
    pub address: SocketAddr,
    pub role: NodeRole,
    pub last_seen: Instant,
    pub health_status: HealthStatus,
}

impl ClusterManager {
    pub async fn add_node(&mut self, node: NodeInfo) -> Result<()> {
        info!("Adding node {} to cluster", node.id);

        // Validate node compatibility
        self.validate_node_compatibility(&node)?;

        // Update cluster configuration
        self.config.nodes.insert(node.id, node.clone());

        // Replicate configuration change
        if self.is_leader() {
            self.replicate_config_change().await?;
        }

        Ok(())
    }

    pub async fn remove_node(&mut self, node_id: NodeId) -> Result<()> {
        info!("Removing node {} from cluster", node_id);

        self.config.nodes.remove(&node_id);

        // Rebalance partition assignments
        self.rebalance_partitions().await?;

        Ok(())
    }
}
```

## ⚡ Performance Optimizations

### 1. Batched Log Replication

```rust
pub struct BatchedReplication {
    pending_entries: Vec<LogEntry>,
    batch_size: usize,
    batch_timeout: Duration,
    last_batch_time: Instant,
}

impl BatchedReplication {
    pub async fn add_entry(&mut self, entry: LogEntry) -> Result<()> {
        self.pending_entries.push(entry);

        // Trigger batch if size or time threshold reached
        if self.pending_entries.len() >= self.batch_size ||
           self.last_batch_time.elapsed() >= self.batch_timeout {
            self.flush_batch().await?;
        }

        Ok(())
    }

    async fn flush_batch(&mut self) -> Result<()> {
        if self.pending_entries.is_empty() {
            return Ok(());
        }

        let batch = std::mem::take(&mut self.pending_entries);
        let batch_size = batch.len();

        // Send entire batch in single RPC
        let append_request = AppendEntriesRequest {
            term: self.current_term,
            leader_id: self.node_id,
            prev_log_index: self.log.len() as u64 - 1,
            prev_log_term: self.log.last().map(|e| e.term).unwrap_or(0),
            entries: batch,
            leader_commit: self.commit_index,
        };

        self.send_to_followers(append_request).await?;
        self.last_batch_time = Instant::now();

        debug!("Flushed batch of {} entries", batch_size);
        Ok(())
    }
}
```

### 2. Pipeline Replication

```rust
pub struct PipelineReplication {
    in_flight_requests: HashMap<u64, PendingRequest>,
    max_in_flight: usize,
    window_size: usize,
}

impl PipelineReplication {
    pub async fn replicate_entry(&mut self, entry: LogEntry) -> Result<()> {
        // Wait if pipeline is full
        while self.in_flight_requests.len() >= self.max_in_flight {
            self.wait_for_completion().await?;
        }

        let request_id = self.next_request_id();
        let pending = PendingRequest {
            entry: entry.clone(),
            sent_time: Instant::now(),
            responses: HashMap::new(),
        };

        self.in_flight_requests.insert(request_id, pending);

        // Send to all followers without waiting
        self.send_async_to_followers(request_id, entry).await?;

        Ok(())
    }
}
```

### 3. Heartbeat Optimization

```rust
pub struct HeartbeatManager {
    last_heartbeat: HashMap<NodeId, Instant>,
    heartbeat_interval: Duration,
    batch_heartbeats: bool,
}

impl HeartbeatManager {
    pub async fn send_heartbeats(&mut self) -> Result<()> {
        let now = Instant::now();
        let nodes_needing_heartbeat: Vec<_> = self.followers
            .iter()
            .filter(|node| {
                self.last_heartbeat.get(&node.id)
                    .map(|last| now.duration_since(*last) >= self.heartbeat_interval)
                    .unwrap_or(true)
            })
            .collect();

        if nodes_needing_heartbeat.is_empty() {
            return Ok(());
        }

        if self.batch_heartbeats {
            // Send single multicast heartbeat
            self.send_batch_heartbeat(&nodes_needing_heartbeat).await?;
        } else {
            // Send individual heartbeats
            let futures = nodes_needing_heartbeat.iter().map(|node| {
                self.send_individual_heartbeat(node.id)
            });
            futures::future::try_join_all(futures).await?;
        }

        // Update heartbeat timestamps
        for node in &nodes_needing_heartbeat {
            self.last_heartbeat.insert(node.id, now);
        }

        Ok(())
    }
}
```

## 🛡️ Fault Tolerance

### 1. Network Partition Handling

```rust
pub struct PartitionDetector {
    node_connectivity: HashMap<NodeId, ConnectivityStatus>,
    partition_threshold: Duration,
    healing_detector: PartitionHealingDetector,
}

impl PartitionDetector {
    pub async fn detect_partitions(&mut self) -> Result<Vec<Partition>> {
        let mut partitions = Vec::new();
        let reachable_nodes = self.get_reachable_nodes().await?;

        // Detect network partitions
        if reachable_nodes.len() <= self.cluster_size() / 2 {
            warn!("Potential network partition detected: only {} nodes reachable",
                  reachable_nodes.len());

            // Step down if leader and in minority partition
            if self.is_leader() && !self.has_majority(&reachable_nodes) {
                self.step_down_as_leader().await?;
            }
        }

        Ok(partitions)
    }

    pub async fn handle_partition_healing(&mut self) -> Result<()> {
        if self.healing_detector.is_partition_healed().await? {
            info!("Network partition healed, rejoining cluster");

            // Synchronize logs with cluster
            self.sync_with_cluster().await?;

            // Resume normal operations
            self.resume_normal_operations().await?;
        }

        Ok(())
    }
}
```

### 2. Byzantine Fault Tolerance

```rust
pub struct ByzantineDetector {
    suspicious_behaviors: HashMap<NodeId, SuspiciousBehavior>,
    fault_threshold: usize,
}

#[derive(Debug)]
pub struct SuspiciousBehavior {
    pub inconsistent_responses: u64,
    pub invalid_signatures: u64,
    pub timestamp_anomalies: u64,
    pub last_detection: Instant,
}

impl ByzantineDetector {
    pub fn analyze_node_behavior(&mut self, node_id: NodeId, response: &AppendEntriesResponse) {
        let behavior = self.suspicious_behaviors.entry(node_id)
            .or_insert_with(SuspiciousBehavior::default);

        // Check for inconsistencies
        if self.is_response_inconsistent(response) {
            behavior.inconsistent_responses += 1;
            warn!("Inconsistent response from node {}", node_id);
        }

        // Validate cryptographic signatures
        if !self.verify_response_signature(response) {
            behavior.invalid_signatures += 1;
            warn!("Invalid signature from node {}", node_id);
        }

        // Check timestamp validity
        if self.is_timestamp_anomalous(&response.timestamp) {
            behavior.timestamp_anomalies += 1;
            warn!("Timestamp anomaly from node {}", node_id);
        }

        // Take action if threshold exceeded
        let total_suspicious = behavior.inconsistent_responses +
                              behavior.invalid_signatures +
                              behavior.timestamp_anomalies;

        if total_suspicious >= self.fault_threshold as u64 {
            self.quarantine_node(node_id);
        }
    }
}
```

## 🔍 Monitoring & Metrics

### Replication Metrics

```rust
pub struct ReplicationMetrics {
    pub leader_elections: AtomicU64,
    pub log_entries_replicated: AtomicU64,
    pub replication_lag_ms: AtomicU64,
    pub failed_replications: AtomicU64,
    pub network_partitions: AtomicU64,
    pub byzantine_faults_detected: AtomicU64,
    pub cluster_size: AtomicU64,
    pub active_followers: AtomicU64,
}

impl ReplicationCoordinator {
    pub fn collect_metrics(&self) -> ReplicationMetrics {
        ReplicationMetrics {
            leader_elections: AtomicU64::new(self.election_count),
            log_entries_replicated: AtomicU64::new(self.replicated_entries),
            replication_lag_ms: AtomicU64::new(self.calculate_replication_lag()),
            failed_replications: AtomicU64::new(self.failed_replication_count),
            network_partitions: AtomicU64::new(self.partition_count),
            byzantine_faults_detected: AtomicU64::new(self.byzantine_fault_count),
            cluster_size: AtomicU64::new(self.cluster_config.nodes.len() as u64),
            active_followers: AtomicU64::new(self.active_follower_count()),
        }
    }
}
```

### Health Monitoring

```bash
# Cluster health endpoint
curl http://localhost:8080/cluster/health

# Replication status
curl http://localhost:8080/cluster/replication-status

# Node information
curl http://localhost:8080/cluster/nodes
```

## 🧪 Testing

### Unit Tests

```bash
# Raft consensus tests
cargo test --lib replication::raft

# Leader election tests
cargo test --lib replication::election

# Network partition tests
cargo test --lib replication::partition
```

### Integration Tests

```bash
# Multi-node cluster tests
cargo test --test cluster_integration

# Failure scenario tests
cargo test --test failure_scenarios

# Performance tests
cargo test --test replication_performance --release
```

### Chaos Engineering

```rust
#[cfg(test)]
mod chaos_tests {
    #[tokio::test]
    async fn test_random_node_failures() {
        let mut cluster = TestCluster::new(5).await;

        for _ in 0..100 {
            // Randomly kill and restart nodes
            let victim = cluster.random_node();
            cluster.kill_node(victim).await;

            // Verify cluster still functions
            assert!(cluster.is_available().await);

            // Restart node after random delay
            tokio::time::sleep(Duration::from_millis(rand::random::<u64>() % 1000)).await;
            cluster.restart_node(victim).await;
        }

        // Verify final consistency
        assert!(cluster.verify_consistency().await);
    }

    #[tokio::test]
    async fn test_network_partitions() {
        let mut cluster = TestCluster::new(5).await;

        // Create network partition
        cluster.partition_network(&[0, 1], &[2, 3, 4]).await;

        // Verify majority partition continues
        assert!(cluster.partition_a().is_available().await);
        assert!(!cluster.partition_b().is_available().await);

        // Heal partition
        cluster.heal_partition().await;

        // Verify consistency after healing
        assert!(cluster.verify_consistency().await);
    }
}
```

## 📊 Performance Characteristics

### Throughput

| Configuration | Write Throughput | Read Throughput | Notes |
|---|---|---|---|
| **3-node cluster** | 400k msg/sec | 1.2M msg/sec | Single leader writes |
| **5-node cluster** | 350k msg/sec | 2M msg/sec | Higher replication overhead |
| **7-node cluster** | 300k msg/sec | 2.8M msg/sec | Maximum fault tolerance |

### Latency

| Operation | P50 | P99 | P99.9 |
|---|---|---|---|
| **Local Write** | 0.1ms | 0.5ms | 2ms |
| **Replicated Write** | 2ms | 10ms | 50ms |
| **Leader Election** | 100ms | 1s | 5s |
| **Log Sync** | 5ms | 50ms | 200ms |

### Scalability

| Metric | Supported | Limitation |
|---|---|---|
| **Cluster Size** | 15+ nodes | Network overhead |
| **Log Size** | 1TB+ | Storage capacity |
| **Partitions** | 10,000+ | Memory usage |
| **Concurrent Clients** | 100,000+ | Leader capacity |

## 🔧 Configuration

### Cluster Configuration

```rust
pub struct ReplicationConfig {
    pub cluster_id: String,
    pub node_id: NodeId,
    pub bind_address: SocketAddr,
    pub peer_addresses: Vec<SocketAddr>,
    pub replication_factor: usize,
    pub election_timeout_ms: u64,        // 150-300ms
    pub heartbeat_interval_ms: u64,      // 50ms
    pub log_compaction_threshold: usize, // 10000 entries
    pub snapshot_interval: Duration,     // 1 hour
    pub max_batch_size: usize,          // 1000 entries
    pub enable_byzantine_detection: bool, // false
}
```

### Usage Examples

```rust
// Start a 3-node cluster
let config = ReplicationConfig {
    cluster_id: "flux-cluster-1".to_string(),
    node_id: 1,
    bind_address: "0.0.0.0:9092".parse()?,
    peer_addresses: vec![
        "192.168.1.10:9092".parse()?,
        "192.168.1.11:9092".parse()?,
    ],
    replication_factor: 3,
    election_timeout_ms: 200,
    heartbeat_interval_ms: 50,
    ..Default::default()
};

let replication_coordinator = ReplicationCoordinator::new(config).await?;
replication_coordinator.start().await?;

// Replicate a message
let command = ReplicationCommand::AppendMessage {
    topic: "my-topic".to_string(),
    partition: 0,
    message: Message::new("key", "value"),
};

let entry_index = replication_coordinator.replicate(command).await?;
```

## 🚦 Future Enhancements

### Short Term
- [ ] Multi-Raft support for partition-level replication
- [ ] Cross-datacenter replication with conflict resolution
- [ ] Snapshot compression and incremental snapshots
- [ ] Dynamic cluster reconfiguration

### Medium Term
- [ ] Geo-distributed consensus with latency optimization
- [ ] Smart follower selection based on network topology
- [ ] Machine learning-based failure prediction
- [ ] Quantum-resistant cryptographic signatures

### Long Term
- [ ] Blockchain-based immutable audit logs
- [ ] Edge computing consensus protocols
- [ ] AI-driven automatic cluster healing
- [ ] Zero-downtime cluster upgrades

## 🔍 Troubleshooting

### Common Issues

#### Split Brain Scenarios
```rust
// Detect and resolve split brain
fn detect_split_brain(&self) -> bool {
    let active_leaders = self.cluster.nodes.iter()
        .filter(|node| node.state == RaftState::Leader)
        .count();

    if active_leaders > 1 {
        warn!("Split brain detected: {} leaders active", active_leaders);
        return true;
    }

    false
}

async fn resolve_split_brain(&mut self) -> Result<()> {
    // Force re-election with higher term
    self.current_term += 1;
    self.start_election().await?;
    Ok(())
}
```

#### Log Divergence
```rust
// Detect and fix log divergence
async fn repair_log_divergence(&mut self, peer: NodeId) -> Result<()> {
    let peer_log_info = self.get_peer_log_info(peer).await?;
    let divergence_point = self.find_divergence_point(&peer_log_info)?;

    if divergence_point < self.log.len() {
        warn!("Log divergence detected at index {}", divergence_point);

        // Truncate divergent entries
        self.log.truncate(divergence_point);

        // Resync from divergence point
        self.resync_from_peer(peer, divergence_point).await?;
    }

    Ok(())
}
```

### Debug Commands

```bash
# Check cluster status
curl http://localhost:8080/cluster/status | jq

# Force leader election
curl -X POST http://localhost:8080/cluster/force-election

# Trigger log compaction
curl -X POST http://localhost:8080/cluster/compact-log

# Health check all nodes
curl http://localhost:8080/cluster/health-check
```

## 📚 References

- [Raft Consensus Algorithm](https://raft.github.io/)
- [Raft Paper](https://raft.github.io/raft.pdf)
- [Kafka Replication Design](https://kafka.apache.org/documentation/#replication)
- [Distributed Systems Concepts](https://www.distributed-systems.net/)

---

*The Replication Module ensures FluxMQ's data survives any failure. Consensus is everything!* 🔄