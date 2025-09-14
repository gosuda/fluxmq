//! Replication system tests

use super::*;
use crate::storage::HybridStorage;
use std::sync::Arc;
use tokio;

#[tokio::test]
async fn test_replication_coordinator_creation() {
    let broker_id = 1;
    let config = ReplicationConfig::default();

    let coordinator = ReplicationCoordinator::new(broker_id, config);

    // Test basic coordinator functionality
    assert!(!coordinator.is_leader("test-topic", 0).await);
    assert_eq!(coordinator.get_leader("test-topic", 0).await, None);
}

#[tokio::test]
async fn test_become_leader() {
    let broker_id = 1;
    let config = ReplicationConfig::default();
    let coordinator = ReplicationCoordinator::new(broker_id, config);

    let replicas = vec![1, 2, 3];
    coordinator
        .become_leader("test-topic", 0, replicas)
        .await
        .unwrap();

    // Verify leadership
    assert!(coordinator.is_leader("test-topic", 0).await);
    assert_eq!(
        coordinator.get_leader("test-topic", 0).await,
        Some(broker_id)
    );

    // Check partition state
    let state = coordinator
        .get_partition_state("test-topic", 0)
        .await
        .unwrap();
    assert_eq!(state.role, ReplicationRole::Leader);
    assert_eq!(state.topic, "test-topic");
    assert_eq!(state.partition, 0);
    assert_eq!(state.replicas, vec![1, 2, 3]);
    assert_eq!(state.in_sync_replicas, vec![broker_id]);
}

#[tokio::test]
async fn test_become_follower() {
    let broker_id = 2;
    let leader_id = 1;
    let config = ReplicationConfig::default();
    let coordinator = ReplicationCoordinator::new(broker_id, config);

    coordinator
        .become_follower("test-topic", 0, leader_id)
        .await
        .unwrap();

    // Verify follower role
    assert!(!coordinator.is_leader("test-topic", 0).await);
    assert_eq!(
        coordinator.get_leader("test-topic", 0).await,
        Some(leader_id)
    );

    // Check partition state
    let state = coordinator
        .get_partition_state("test-topic", 0)
        .await
        .unwrap();
    assert_eq!(state.role, ReplicationRole::Follower { leader_id });
    assert_eq!(state.topic, "test-topic");
    assert_eq!(state.partition, 0);
    assert_eq!(state.replicas, vec![leader_id, broker_id]);
    assert_eq!(state.in_sync_replicas, vec![leader_id]);
}

#[tokio::test]
async fn test_leader_state_creation() {
    let broker_id = 1;
    let topic = "test-topic".to_string();
    let partition = 0;
    let followers = vec![2, 3];
    let config = ReplicationConfig::default();

    let leader_state = LeaderState::new(
        broker_id,
        topic.clone(),
        partition,
        followers.clone(),
        config,
    );

    // Test ISR functionality - this is the main public interface
    let isr = leader_state.get_isr().await;
    assert!(isr.contains(&broker_id)); // Leader is always in ISR
    assert!(isr.len() >= 1); // At least the leader should be in ISR
}

#[tokio::test]
async fn test_follower_state_creation() {
    let broker_id = 2;
    let leader_id = 1;
    let topic = "test-topic".to_string();
    let partition = 0;
    let config = ReplicationConfig::default();

    let follower_state = FollowerState::new(broker_id, topic.clone(), partition, leader_id, config);

    // Test initial state through public interface
    assert_eq!(follower_state.get_current_term().await, 0);
    assert_eq!(follower_state.get_commit_index().await, 0);
    assert_eq!(follower_state.get_last_applied().await, 0);
}

#[tokio::test]
async fn test_replication_manager_creation() {
    let storage = Arc::new(HybridStorage::new("./test-data-replication").unwrap());
    let broker_id = 1;
    let config = ReplicationConfig::default();

    let manager = ReplicationManager::new(broker_id, storage, config);

    // Test adding a leader partition
    let followers = vec![2, 3];
    manager
        .add_leader("test-topic", 0, followers)
        .await
        .unwrap();

    // The add_leader should work without errors
    // In a full integration test, we'd verify replication behavior
}

#[tokio::test]
async fn test_follower_sync_creation() {
    let storage = Arc::new(HybridStorage::new("./test-data-follower").unwrap());
    let broker_id = 2;
    let config = ReplicationConfig::default();

    let sync = FollowerSync::new(broker_id, storage, config);

    // Test adding a follower partition
    let leader_id = 1;
    sync.add_follower("test-topic", 0, leader_id).await.unwrap();

    // Verify follower was added
    let follower = sync.get_follower("test-topic", 0).await;
    assert!(follower.is_some());

    let _follower_state = follower.unwrap();

    // Test initial catch-up state
    assert!(sync.is_caught_up().await); // Should be caught up initially

    // Test status reporting
    let status = sync.get_follower_status().await;
    assert!(status.contains_key(&("test-topic".to_string(), 0)));
}

#[tokio::test]
async fn test_replication_config_defaults() {
    let config = ReplicationConfig::default();

    assert_eq!(config.min_isr, 1);
    assert_eq!(config.replication_factor, 3);
    assert_eq!(config.heartbeat_interval_ms, 1000);
    assert_eq!(config.replication_timeout_ms, 5000);
    assert_eq!(config.max_batch_size, 1000);
}

#[tokio::test]
async fn test_log_entry_creation() {
    use crate::protocol::Message;
    use std::time::{SystemTime, UNIX_EPOCH};

    let message = Message {
        key: Some(bytes::Bytes::from("test-key")),
        value: bytes::Bytes::from("test-value"),
        headers: std::collections::HashMap::new(),
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    };

    let entry = LogEntry {
        offset: 42,
        term: 1,
        message: message.clone(),
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    };

    assert_eq!(entry.offset, 42);
    assert_eq!(entry.term, 1);
    assert_eq!(entry.message.value, message.value);
}
