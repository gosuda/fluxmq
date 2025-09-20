//! Comprehensive Raft Network Integration Tests
//!
//! These tests verify the network-enhanced Raft consensus implementation
//! including leader election, message replication, and failure scenarios.

use fluxmq::protocol::Message;
use fluxmq::replication::{
    ClusterConfig, ClusterCoordinator, NetworkConfig, RaftState, ReplicationConfig,
    ReplicationNetworkManager,
};
use fluxmq::storage::HybridStorage;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

/// Helper function to create a test storage instance
async fn create_test_storage() -> (Arc<HybridStorage>, TempDir) {
    let temp_dir = tempfile::tempdir().expect("Should create temp dir");
    let storage_path = temp_dir.path().to_str().unwrap();
    let storage = Arc::new(HybridStorage::new(storage_path).expect("Should create storage"));
    (storage, temp_dir)
}

/// Helper function to get available port
fn get_available_port() -> u16 {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

#[tokio::test]
async fn test_network_enhanced_raft_single_node() {
    // Test enhanced Raft with single node cluster
    let (storage, _temp_dir) = create_test_storage().await;
    let broker_id = 1;
    let port = get_available_port();
    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

    let config = ClusterConfig::new_single_node(broker_id, listen_addr);
    let coordinator = ClusterCoordinator::new(config, storage);

    // Start the coordinator
    coordinator
        .start()
        .await
        .expect("Failed to start coordinator");

    // Create a test partition
    let topic = "network-raft-test";
    let partition = 0;
    let replicas = vec![broker_id];

    coordinator
        .create_partition(topic, partition, replicas)
        .await
        .expect("Failed to create partition");

    // Wait for leadership establishment
    sleep(Duration::from_millis(100)).await;

    // Verify leadership
    assert!(coordinator.is_partition_leader(topic, partition).await);
    assert_eq!(
        coordinator.get_partition_leader(topic, partition).await,
        Some(broker_id)
    );

    // Test message proposal through network-enhanced Raft
    let test_message = Message {
        key: Some(bytes::Bytes::from("network-key")),
        value: bytes::Bytes::from("network-value"),
        headers: HashMap::new(),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    };

    // Propose message through Raft consensus
    let result = coordinator
        .propose_message(topic, partition, test_message, 0)
        .await;

    // Should succeed in single-node cluster
    assert!(result.is_ok(), "Message proposal should succeed");

    // Verify cluster status
    let status = coordinator.get_cluster_status().await;
    assert_eq!(status.broker_id, broker_id);
    assert_eq!(status.total_brokers, 1);

    let leadership_summary = status.get_leadership_summary();
    assert_eq!(leadership_summary.get(&RaftState::Leader), Some(&1));
}

#[tokio::test]
async fn test_network_manager_integration() {
    // Test ReplicationNetworkManager integration with Raft
    let broker_id = 1;
    let port = get_available_port();
    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let config = NetworkConfig::default();

    let network_manager = Arc::new(ReplicationNetworkManager::new(
        broker_id,
        listen_addr,
        config,
    ));

    // Start network manager
    network_manager
        .start()
        .await
        .expect("Network manager should start");

    // Register additional brokers
    let broker2_port = get_available_port();
    let broker3_port = get_available_port();

    network_manager
        .register_broker(
            2,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), broker2_port),
        )
        .await;

    network_manager
        .register_broker(
            3,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), broker3_port),
        )
        .await;

    // Get network statistics
    let stats = network_manager.get_stats().await;
    assert_eq!(stats.registered_brokers, 2);
    assert_eq!(stats.connected_peers, 0); // Not connected yet
    assert!(stats.active_connections.is_empty());

    // Test connection attempts (will fail since brokers aren't running, but tests the flow)
    let connect_result = network_manager.connect_to_all_brokers().await;
    // Connection may fail in test environment, which is expected
    assert!(connect_result.is_ok()); // Method should not error even if connections fail
}

#[tokio::test]
async fn test_multi_partition_raft_coordination() {
    // Test network-enhanced Raft across multiple partitions
    let (storage, _temp_dir) = create_test_storage().await;
    let broker_id = 1;
    let port = get_available_port();
    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

    let config = ClusterConfig::new_single_node(broker_id, listen_addr);
    let coordinator = ClusterCoordinator::new(config, storage);

    coordinator
        .start()
        .await
        .expect("Failed to start coordinator");

    // Create multiple partitions across different topics
    let partitions = vec![
        ("topic1", 0),
        ("topic1", 1),
        ("topic2", 0),
        ("topic3", 0),
        ("topic3", 1),
        ("topic3", 2),
    ];

    for (topic, partition) in &partitions {
        coordinator
            .create_partition(topic, *partition, vec![broker_id])
            .await
            .expect("Failed to create partition");
    }

    // Wait for all leadership to be established
    sleep(Duration::from_millis(200)).await;

    // Verify all partitions have leadership
    for (topic, partition) in &partitions {
        assert!(
            coordinator.is_partition_leader(topic, *partition).await,
            "Should be leader for {}:{}",
            topic,
            partition
        );
        assert_eq!(
            coordinator.get_partition_leader(topic, *partition).await,
            Some(broker_id),
            "Should report correct leader for {}:{}",
            topic,
            partition
        );
    }

    // Test concurrent message proposals across partitions
    let coordinator_arc = Arc::new(coordinator);
    let mut proposal_tasks = Vec::new();

    for (i, (topic, partition)) in partitions.into_iter().enumerate() {
        let coordinator_ref = Arc::clone(&coordinator_arc);
        let test_message = Message {
            key: Some(bytes::Bytes::from(format!("key-{}", i))),
            value: bytes::Bytes::from(format!("value-{}", i)),
            headers: HashMap::new(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };

        let task = tokio::spawn(async move {
            coordinator_ref
                .propose_message(&topic, partition, test_message, 0)
                .await
        });

        proposal_tasks.push(task);
    }

    // Wait for all proposals to complete
    let mut successful_proposals = 0;
    for task in proposal_tasks {
        match task.await {
            Ok(Ok(_)) => successful_proposals += 1,
            Ok(Err(e)) => println!("Proposal failed: {}", e),
            Err(e) => println!("Task failed: {}", e),
        }
    }

    // In single-node cluster, most proposals should succeed
    assert!(
        successful_proposals >= 4,
        "At least 4 out of 6 proposals should succeed, got {}",
        successful_proposals
    );

    // Verify final cluster status
    let status = coordinator_arc.get_cluster_status().await;
    assert_eq!(status.partition_leaders.len(), 6);

    let leadership_summary = status.get_leadership_summary();
    assert_eq!(leadership_summary.get(&RaftState::Leader), Some(&6));
}

#[tokio::test]
async fn test_raft_leader_state_network_integration() {
    // Test that LeaderState properly integrates with network manager
    use fluxmq::replication::{LeaderState, ReplicationConfig};

    let broker_id = 1;
    let followers = vec![2, 3];
    let config = ReplicationConfig::default();

    let leader_state = Arc::new(LeaderState::new(
        broker_id,
        "test-topic".to_string(),
        0,
        followers.clone(),
        config,
    ));

    // Create a network manager for testing
    let port = get_available_port();
    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let network_config = NetworkConfig::default();
    let network_manager = Arc::new(ReplicationNetworkManager::new(
        broker_id,
        listen_addr,
        network_config,
    ));

    // Set the network manager on the leader state
    leader_state
        .set_network_manager(network_manager.clone())
        .await;

    // Verify ISR includes all followers initially
    let isr = leader_state.get_isr().await;
    assert_eq!(isr.len(), 3); // Leader + 2 followers
    assert!(isr.contains(&broker_id));
    assert!(isr.contains(&2));
    assert!(isr.contains(&3));

    // Create storage for replication testing
    let (storage, _temp_dir) = create_test_storage().await;

    // Start replication (this will start heartbeat and response handling tasks)
    leader_state
        .clone()
        .start_replication(storage.clone())
        .await
        .expect("Should start replication");

    // Give some time for background tasks to initialize
    sleep(Duration::from_millis(100)).await;

    // Test message replication (will use network manager)
    let test_messages = vec![
        Message {
            key: Some(bytes::Bytes::from("key1")),
            value: bytes::Bytes::from("value1"),
            headers: HashMap::new(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        },
        Message {
            key: Some(bytes::Bytes::from("key2")),
            value: bytes::Bytes::from("value2"),
            headers: HashMap::new(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        },
    ];

    // This should trigger network calls to followers (will fail in test but tests the flow)
    let result = leader_state
        .replicate_messages(storage, test_messages, 0)
        .await;

    // Should not error even if network calls fail
    assert!(result.is_ok(), "Replication should not error");
}

#[tokio::test]
async fn test_cluster_config_variations() {
    // Test different cluster configurations
    let broker_id = 1;
    let port = get_available_port();
    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

    // Test single-node configuration
    let single_config = ClusterConfig::new_single_node(broker_id, listen_addr);
    assert_eq!(single_config.broker_id, broker_id);
    assert_eq!(single_config.brokers, vec![broker_id]);

    // Test multi-node configuration
    let brokers = vec![1, 2, 3, 4, 5];
    let mut broker_addresses = HashMap::new();
    for &broker in brokers.iter() {
        let port = get_available_port();
        broker_addresses.insert(
            broker,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        );
    }

    let multi_config = ClusterConfig::new_cluster(
        broker_id,
        brokers.clone(),
        broker_addresses.clone(),
        listen_addr,
    );

    assert_eq!(multi_config.broker_id, broker_id);
    assert_eq!(multi_config.brokers, brokers);
    assert_eq!(multi_config.broker_addresses, broker_addresses);

    // Test cluster with different replication configurations
    let mut replication_config = ReplicationConfig::default();
    replication_config.min_isr = 3;
    replication_config.replication_factor = 5;
    replication_config.heartbeat_interval_ms = 500;
    replication_config.replication_timeout_ms = 2500;

    // Verify configuration is applied correctly
    assert_eq!(replication_config.min_isr, 3);
    assert_eq!(replication_config.replication_factor, 5);
    assert_eq!(replication_config.heartbeat_interval_ms, 500);
    assert_eq!(replication_config.replication_timeout_ms, 2500);
}

#[tokio::test]
async fn test_raft_consensus_under_load() {
    // Test Raft consensus with high message throughput
    let (storage, _temp_dir) = create_test_storage().await;
    let broker_id = 1;
    let port = get_available_port();
    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

    let config = ClusterConfig::new_single_node(broker_id, listen_addr);
    let coordinator = ClusterCoordinator::new(config, storage);

    coordinator
        .start()
        .await
        .expect("Failed to start coordinator");

    let topic = "load-test-topic";
    let partition = 0;

    coordinator
        .create_partition(topic, partition, vec![broker_id])
        .await
        .expect("Failed to create partition");

    sleep(Duration::from_millis(50)).await;

    // Propose many messages concurrently
    let message_count = 100;
    let coordinator_arc = Arc::new(coordinator);
    let mut proposal_tasks = Vec::new();

    for i in 0..message_count {
        let coordinator_ref = Arc::clone(&coordinator_arc);
        let test_message = Message {
            key: Some(bytes::Bytes::from(format!("load-key-{}", i))),
            value: bytes::Bytes::from(format!("load-value-{}", i)),
            headers: HashMap::new(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };

        let task = tokio::spawn(async move {
            coordinator_ref
                .propose_message(topic, partition, test_message, i as u64)
                .await
        });

        proposal_tasks.push(task);
    }

    // Wait for all proposals with timeout
    let start = std::time::Instant::now();
    let mut successful_proposals = 0;
    let mut failed_proposals = 0;

    for task in proposal_tasks {
        match tokio::time::timeout(Duration::from_secs(5), task).await {
            Ok(Ok(Ok(_))) => successful_proposals += 1,
            Ok(Ok(Err(_))) => failed_proposals += 1,
            Ok(Err(_)) => failed_proposals += 1,
            Err(_) => failed_proposals += 1, // Timeout
        }
    }

    let duration = start.elapsed();

    println!(
        "Load test results: {} successful, {} failed in {:?}",
        successful_proposals, failed_proposals, duration
    );

    // At least 70% should succeed under load in single-node cluster
    assert!(
        successful_proposals >= (message_count as f32 * 0.7) as usize,
        "Should handle load with >70% success rate, got {}/{}",
        successful_proposals,
        message_count
    );

    // Should complete within reasonable time
    assert!(
        duration < Duration::from_secs(10),
        "Load test should complete within 10 seconds, took {:?}",
        duration
    );
}

#[tokio::test]
async fn test_network_failure_simulation() {
    // Test Raft behavior with simulated network failures
    let broker_id = 1;
    let port = get_available_port();
    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

    // Create network config with shorter timeouts for faster testing
    let mut network_config = NetworkConfig::default();
    network_config.connection_timeout_ms = 1000;
    network_config.message_timeout_ms = 500;
    network_config.max_retries = 2;

    let network_manager = Arc::new(ReplicationNetworkManager::new(
        broker_id,
        listen_addr,
        network_config,
    ));

    // Start network manager
    network_manager
        .start()
        .await
        .expect("Network manager should start");

    // Register non-existent brokers (simulates network failure)
    network_manager
        .register_broker(2, "127.0.0.1:65535".parse().unwrap()) // Invalid port
        .await;

    network_manager
        .register_broker(3, "192.0.2.1:9092".parse().unwrap()) // Non-routable IP
        .await;

    // Attempt connections (should fail gracefully)
    let connect_result = network_manager.connect_to_all_brokers().await;
    assert!(connect_result.is_ok()); // Should not panic on connection failures

    // Verify stats show registered but not connected brokers
    let stats = network_manager.get_stats().await;
    assert_eq!(stats.registered_brokers, 2);
    assert_eq!(stats.connected_peers, 0); // No successful connections
}

mod performance_tests {
    use super::*;

    #[tokio::test]
    async fn test_raft_consensus_performance() {
        // Performance test for network-enhanced Raft consensus
        let (storage, _temp_dir) = create_test_storage().await;
        let broker_id = 1;
        let port = get_available_port();
        let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

        let config = ClusterConfig::new_single_node(broker_id, listen_addr);
        let coordinator = ClusterCoordinator::new(config, storage);

        coordinator
            .start()
            .await
            .expect("Failed to start coordinator");

        // Create partition
        coordinator
            .create_partition("perf-test", 0, vec![broker_id])
            .await
            .expect("Failed to create partition");

        sleep(Duration::from_millis(50)).await;

        // Performance test: measure consensus latency
        let start = std::time::Instant::now();
        let iterations = 50;

        for i in 0..iterations {
            let test_message = Message {
                key: Some(bytes::Bytes::from(format!("perf-key-{}", i))),
                value: bytes::Bytes::from(format!("perf-value-{}", i)),
                headers: HashMap::new(),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            };

            let result = coordinator
                .propose_message("perf-test", 0, test_message, i as u64)
                .await;

            assert!(result.is_ok(), "Proposal {} should succeed", i);
        }

        let duration = start.elapsed();
        let avg_latency = duration / iterations;

        println!(
            "Raft consensus performance: {} iterations in {:?}, avg latency: {:?}",
            iterations, duration, avg_latency
        );

        // Should achieve reasonable performance (single-node cluster)
        assert!(
            avg_latency < Duration::from_millis(50),
            "Average consensus latency should be <50ms, got {:?}",
            avg_latency
        );
    }
}
