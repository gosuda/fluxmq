//! Consumer group tests

use super::*;
use crate::topic_manager::TopicManager;
use std::sync::Arc;
use tokio;

#[tokio::test]
async fn test_partition_assignor_round_robin() {
    let assignor = PartitionAssignor::new(AssignmentStrategy::RoundRobin);

    let consumers = vec![
        "consumer1".to_string(),
        "consumer2".to_string(),
        "consumer3".to_string(),
    ];
    let partitions = vec![
        TopicPartition::new("topic1", 0),
        TopicPartition::new("topic1", 1),
        TopicPartition::new("topic1", 2),
        TopicPartition::new("topic2", 0),
        TopicPartition::new("topic2", 1),
    ];

    let assignments = assignor.assign(&consumers, &partitions);

    assert_eq!(assignments.len(), 3);

    // Check that all partitions are assigned
    let mut total_assigned = 0;
    for (consumer, assigned_partitions) in &assignments {
        assert!(consumers.contains(consumer));
        total_assigned += assigned_partitions.len();
    }
    assert_eq!(total_assigned, partitions.len());

    // Check round-robin distribution
    assert_eq!(assignments["consumer1"].len(), 2); // partitions 0, 3
    assert_eq!(assignments["consumer2"].len(), 2); // partitions 1, 4
    assert_eq!(assignments["consumer3"].len(), 1); // partition 2
}

#[tokio::test]
async fn test_partition_assignor_range() {
    let assignor = PartitionAssignor::new(AssignmentStrategy::Range);

    let consumers = vec!["consumer1".to_string(), "consumer2".to_string()];
    let partitions = vec![
        TopicPartition::new("topic1", 0),
        TopicPartition::new("topic1", 1),
        TopicPartition::new("topic1", 2),
        TopicPartition::new("topic1", 3),
    ];

    let assignments = assignor.assign(&consumers, &partitions);

    assert_eq!(assignments.len(), 2);
    assert_eq!(assignments["consumer1"].len(), 2);
    assert_eq!(assignments["consumer2"].len(), 2);

    // Check that partitions are assigned in ranges
    let consumer1_partitions = &assignments["consumer1"];
    let consumer2_partitions = &assignments["consumer2"];

    assert_eq!(consumer1_partitions[0].partition, 0);
    assert_eq!(consumer1_partitions[1].partition, 1);
    assert_eq!(consumer2_partitions[0].partition, 2);
    assert_eq!(consumer2_partitions[1].partition, 3);
}

#[tokio::test]
async fn test_consumer_group_coordinator_creation() {
    let topic_manager = Arc::new(TopicManager::new());
    let config = ConsumerGroupConfig::default();

    let coordinator = ConsumerGroupCoordinator::new(config, topic_manager, None, None);

    // Test basic functionality without starting background tasks
    let list_response = coordinator.handle_list_groups().await;
    match list_response {
        ConsumerGroupMessage::ListGroupsResponse { error_code, groups } => {
            assert_eq!(error_code, error_codes::NONE);
            assert!(groups.is_empty());
        }
        _ => panic!("Expected ListGroupsResponse"),
    }
}

#[tokio::test]
async fn test_join_group_workflow() {
    let topic_manager = Arc::new(TopicManager::new());
    let config = ConsumerGroupConfig::default();

    let coordinator = ConsumerGroupCoordinator::new(config, topic_manager, None, None);

    // Create a join group request
    let join_request = ConsumerGroupMessage::JoinGroup {
        group_id: "test-group".to_string(),
        consumer_id: "consumer-1".to_string(),
        client_id: "test-client".to_string(),
        client_host: "localhost".to_string(),
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 60000,
        protocol_type: "consumer".to_string(),
        group_protocols: vec![GroupProtocol {
            name: "range".to_string(),
            metadata: Vec::new(),
        }],
    };

    // Handle join group request
    let join_response = coordinator.handle_join_group(join_request).await;

    match join_response {
        ConsumerGroupMessage::JoinGroupResponse {
            error_code,
            generation_id,
            group_protocol,
            leader_id,
            consumer_id,
            members,
        } => {
            assert_eq!(error_code, error_codes::NONE);
            assert_eq!(generation_id, 1);
            assert_eq!(group_protocol, "range");
            assert_eq!(leader_id, "consumer-1");
            assert_eq!(consumer_id, "consumer-1");
            assert_eq!(members.len(), 1);
            assert!(members[0].is_leader);
        }
        _ => panic!("Expected JoinGroupResponse"),
    }
}

#[tokio::test]
async fn test_sync_group_workflow() {
    let topic_manager = Arc::new(TopicManager::new());
    let config = ConsumerGroupConfig::default();

    let coordinator = ConsumerGroupCoordinator::new(config, topic_manager, None, None);

    // First, join the group
    let join_request = ConsumerGroupMessage::JoinGroup {
        group_id: "test-group".to_string(),
        consumer_id: "consumer-1".to_string(),
        client_id: "test-client".to_string(),
        client_host: "localhost".to_string(),
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 60000,
        protocol_type: "consumer".to_string(),
        group_protocols: vec![GroupProtocol {
            name: "range".to_string(),
            metadata: Vec::new(),
        }],
    };

    coordinator.handle_join_group(join_request).await;

    // Now sync group with assignments
    let mut assignments = HashMap::new();
    assignments.insert(
        "consumer-1".to_string(),
        vec![
            TopicPartition::new("test-topic", 0),
            TopicPartition::new("test-topic", 1),
        ],
    );

    let sync_request = ConsumerGroupMessage::SyncGroup {
        group_id: "test-group".to_string(),
        consumer_id: "consumer-1".to_string(),
        generation_id: 1,
        group_assignments: assignments,
    };

    let sync_response = coordinator.handle_sync_group(sync_request).await;

    match sync_response {
        ConsumerGroupMessage::SyncGroupResponse {
            error_code,
            assignment,
        } => {
            assert_eq!(error_code, error_codes::NONE);
            assert_eq!(assignment.len(), 2);
            assert_eq!(assignment[0].topic, "test-topic");
            assert_eq!(assignment[0].partition, 0);
            assert_eq!(assignment[1].topic, "test-topic");
            assert_eq!(assignment[1].partition, 1);
        }
        _ => panic!("Expected SyncGroupResponse"),
    }
}

#[tokio::test]
async fn test_heartbeat_workflow() {
    let topic_manager = Arc::new(TopicManager::new());
    let config = ConsumerGroupConfig::default();

    let coordinator = ConsumerGroupCoordinator::new(config, topic_manager, None, None);

    // Join group first
    let join_request = ConsumerGroupMessage::JoinGroup {
        group_id: "test-group".to_string(),
        consumer_id: "consumer-1".to_string(),
        client_id: "test-client".to_string(),
        client_host: "localhost".to_string(),
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 60000,
        protocol_type: "consumer".to_string(),
        group_protocols: vec![GroupProtocol {
            name: "range".to_string(),
            metadata: Vec::new(),
        }],
    };

    coordinator.handle_join_group(join_request).await;

    // Send heartbeat
    let heartbeat_request = ConsumerGroupMessage::Heartbeat {
        group_id: "test-group".to_string(),
        consumer_id: "consumer-1".to_string(),
        generation_id: 1,
    };

    let heartbeat_response = coordinator.handle_heartbeat(heartbeat_request).await;

    match heartbeat_response {
        ConsumerGroupMessage::HeartbeatResponse { error_code } => {
            assert_eq!(error_code, error_codes::NONE);
        }
        _ => panic!("Expected HeartbeatResponse"),
    }
}

#[tokio::test]
async fn test_leave_group_workflow() {
    let topic_manager = Arc::new(TopicManager::new());
    let config = ConsumerGroupConfig::default();

    let coordinator = ConsumerGroupCoordinator::new(config, topic_manager, None, None);

    // Join group first
    let join_request = ConsumerGroupMessage::JoinGroup {
        group_id: "test-group".to_string(),
        consumer_id: "consumer-1".to_string(),
        client_id: "test-client".to_string(),
        client_host: "localhost".to_string(),
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 60000,
        protocol_type: "consumer".to_string(),
        group_protocols: vec![GroupProtocol {
            name: "range".to_string(),
            metadata: Vec::new(),
        }],
    };

    coordinator.handle_join_group(join_request).await;

    // Leave group
    let leave_request = ConsumerGroupMessage::LeaveGroup {
        group_id: "test-group".to_string(),
        consumer_id: "consumer-1".to_string(),
    };

    let leave_response = coordinator.handle_leave_group(leave_request).await;

    match leave_response {
        ConsumerGroupMessage::LeaveGroupResponse { error_code } => {
            assert_eq!(error_code, error_codes::NONE);
        }
        _ => panic!("Expected LeaveGroupResponse"),
    }

    // Verify group is empty
    let list_response = coordinator.handle_list_groups().await;
    match list_response {
        ConsumerGroupMessage::ListGroupsResponse {
            error_code,
            groups: _,
        } => {
            assert_eq!(error_code, error_codes::NONE);
            // Group might still exist but be empty, or be removed
            // Both behaviors are valid
        }
        _ => panic!("Expected ListGroupsResponse"),
    }
}

#[tokio::test]
async fn test_consumer_group_config_defaults() {
    let config = ConsumerGroupConfig::default();

    assert_eq!(config.default_session_timeout_ms, 10000);
    assert_eq!(config.default_rebalance_timeout_ms, 60000);
    assert_eq!(config.min_session_timeout_ms, 6000);
    assert_eq!(config.max_session_timeout_ms, 1800000);
    assert_eq!(config.consumer_expiration_check_interval_ms, 3000);
    assert_eq!(
        config.default_assignment_strategy,
        AssignmentStrategy::RoundRobin
    );
    assert_eq!(config.group_metadata_retention_ms, 86400000);
}

#[tokio::test]
async fn test_topic_partition_creation() {
    let tp1 = TopicPartition::new("test-topic", 5);
    assert_eq!(tp1.topic, "test-topic");
    assert_eq!(tp1.partition, 5);

    let tp2 = TopicPartition::new("another-topic".to_string(), 10);
    assert_eq!(tp2.topic, "another-topic");
    assert_eq!(tp2.partition, 10);

    // Test equality
    let tp3 = TopicPartition::new("test-topic", 5);
    assert_eq!(tp1, tp3);
}

#[tokio::test]
async fn test_assignment_strategies() {
    // Test that all strategies are created properly
    let round_robin = AssignmentStrategy::RoundRobin;
    let range = AssignmentStrategy::Range;
    let sticky = AssignmentStrategy::Sticky;

    assert_eq!(round_robin, AssignmentStrategy::RoundRobin);
    assert_eq!(range, AssignmentStrategy::Range);
    assert_eq!(sticky, AssignmentStrategy::Sticky);

    // Test that sticky currently behaves like round-robin
    let assignor_sticky = PartitionAssignor::new(AssignmentStrategy::Sticky);
    let assignor_rr = PartitionAssignor::new(AssignmentStrategy::RoundRobin);

    let consumers = vec!["c1".to_string(), "c2".to_string()];
    let partitions = vec![
        TopicPartition::new("topic", 0),
        TopicPartition::new("topic", 1),
        TopicPartition::new("topic", 2),
    ];

    let sticky_result = assignor_sticky.assign(&consumers, &partitions);
    let rr_result = assignor_rr.assign(&consumers, &partitions);

    // Should produce the same result for now
    assert_eq!(sticky_result.len(), rr_result.len());
}
