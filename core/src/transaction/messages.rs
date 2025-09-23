//! Kafka Transaction API Message Structures
//!
//! This module defines all request and response structures for Kafka's transaction APIs.
//! These structures match the official Kafka wire protocol specification for transaction support.

use super::TopicPartition;
use crate::protocol::kafka::messages::KafkaRequestHeader;
use serde::{Deserialize, Serialize};

// ============================================================================
// INIT_PRODUCER_ID API (ApiKey = 22)
// ============================================================================

/// InitProducerId request to initialize a transactional producer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitProducerIdRequest {
    pub header: KafkaRequestHeader,
    /// Transactional ID for the producer (nullable)
    pub transactional_id: Option<String>,
    /// Transaction timeout in milliseconds
    pub transaction_timeout_ms: i32,
    /// Current producer ID (-1 if not set)
    pub producer_id: i64,
    /// Current producer epoch (-1 if not set)
    pub producer_epoch: i16,
}

/// InitProducerId response with assigned producer ID and epoch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitProducerIdResponse {
    pub correlation_id: i32,
    /// Error code (0 = success)
    pub error_code: i16,
    /// Assigned producer ID
    pub producer_id: i64,
    /// Assigned producer epoch
    pub producer_epoch: i16,
    /// Response throttle time in milliseconds
    pub throttle_time_ms: i32,
}

// ============================================================================
// ADD_PARTITIONS_TO_TXN API (ApiKey = 24)
// ============================================================================

/// AddPartitionsToTxn request to add partitions to an ongoing transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddPartitionsToTxnRequest {
    pub header: KafkaRequestHeader,
    /// Transactional ID
    pub transactional_id: String,
    /// Producer ID
    pub producer_id: i64,
    /// Producer epoch
    pub producer_epoch: i16,
    /// Topic partitions to add to the transaction
    pub topics: Vec<AddPartitionsToTxnTopic>,
}

/// Topic with partitions to add to transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddPartitionsToTxnTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

/// AddPartitionsToTxn response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddPartitionsToTxnResponse {
    pub correlation_id: i32,
    /// Response throttle time in milliseconds
    pub throttle_time_ms: i32,
    /// Results for each topic
    pub results: Vec<AddPartitionsToTxnTopicResult>,
}

/// Result for each topic in AddPartitionsToTxn response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddPartitionsToTxnTopicResult {
    pub name: String,
    pub results: Vec<AddPartitionsToTxnPartitionResult>,
}

/// Result for each partition in AddPartitionsToTxn response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddPartitionsToTxnPartitionResult {
    pub partition_index: i32,
    pub error_code: i16,
}

// ============================================================================
// ADD_OFFSETS_TO_TXN API (ApiKey = 25)
// ============================================================================

/// AddOffsetsToTxn request to add consumer group offsets to transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddOffsetsToTxnRequest {
    pub header: KafkaRequestHeader,
    /// Transactional ID
    pub transactional_id: String,
    /// Producer ID
    pub producer_id: i64,
    /// Producer epoch
    pub producer_epoch: i16,
    /// Consumer group ID
    pub group_id: String,
}

/// AddOffsetsToTxn response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddOffsetsToTxnResponse {
    pub correlation_id: i32,
    /// Response throttle time in milliseconds
    pub throttle_time_ms: i32,
    /// Error code (0 = success)
    pub error_code: i16,
}

// ============================================================================
// END_TXN API (ApiKey = 26)
// ============================================================================

/// EndTxn request to commit or abort a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndTxnRequest {
    pub header: KafkaRequestHeader,
    /// Transactional ID
    pub transactional_id: String,
    /// Producer ID
    pub producer_id: i64,
    /// Producer epoch
    pub producer_epoch: i16,
    /// true = commit, false = abort
    pub committed: bool,
}

/// EndTxn response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndTxnResponse {
    pub correlation_id: i32,
    /// Response throttle time in milliseconds
    pub throttle_time_ms: i32,
    /// Error code (0 = success)
    pub error_code: i16,
}

// ============================================================================
// WRITE_TXN_MARKERS API (ApiKey = 27)
// ============================================================================

/// WriteTxnMarkers request to write transaction markers to partition logs
/// This is an internal API used by the transaction coordinator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteTxnMarkersRequest {
    pub header: KafkaRequestHeader,
    /// Transaction markers to write
    pub markers: Vec<WritableTxnMarker>,
}

/// Transaction marker to write to partition logs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WritableTxnMarker {
    /// Producer ID
    pub producer_id: i64,
    /// Producer epoch
    pub producer_epoch: i16,
    /// Transaction result (true = COMMIT, false = ABORT)
    pub transaction_result: bool,
    /// Topics and partitions to write markers to
    pub topics: Vec<WritableTxnMarkerTopic>,
    /// Coordinator epoch that wrote the transaction log
    pub coordinator_epoch: i32,
}

/// Topic with partitions for transaction markers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WritableTxnMarkerTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

/// WriteTxnMarkers response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteTxnMarkersResponse {
    pub correlation_id: i32,
    /// Results for each marker
    pub markers: Vec<WritableTxnMarkerResult>,
}

/// Result for each transaction marker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WritableTxnMarkerResult {
    /// Producer ID
    pub producer_id: i64,
    /// Topics with partition results
    pub topics: Vec<WritableTxnMarkerTopicResult>,
}

/// Topic result for transaction markers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WritableTxnMarkerTopicResult {
    pub name: String,
    pub partitions: Vec<WritableTxnMarkerPartitionResult>,
}

/// Partition result for transaction markers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WritableTxnMarkerPartitionResult {
    pub partition_index: i32,
    pub error_code: i16,
}

// ============================================================================
// TXN_OFFSET_COMMIT API (ApiKey = 28)
// ============================================================================

/// TxnOffsetCommit request for transactional offset commits
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnOffsetCommitRequest {
    pub header: KafkaRequestHeader,
    /// Transactional ID
    pub transactional_id: String,
    /// Consumer group ID
    pub group_id: String,
    /// Producer ID
    pub producer_id: i64,
    /// Producer epoch
    pub producer_epoch: i16,
    /// Generation ID for the consumer group
    pub generation_id: i32,
    /// Member ID in the consumer group
    pub member_id: String,
    /// Group instance ID for static membership (nullable)
    pub group_instance_id: Option<String>,
    /// Topics with offset information
    pub topics: Vec<TxnOffsetCommitRequestTopic>,
}

/// Topic with partitions for transactional offset commit
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnOffsetCommitRequestTopic {
    pub name: String,
    pub partitions: Vec<TxnOffsetCommitRequestPartition>,
}

/// Partition offset information for transactional commit
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnOffsetCommitRequestPartition {
    pub partition_index: i32,
    /// Offset to commit
    pub committed_offset: i64,
    /// Leader epoch
    pub committed_leader_epoch: i32,
    /// Optional metadata
    pub committed_metadata: Option<String>,
}

/// TxnOffsetCommit response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnOffsetCommitResponse {
    pub correlation_id: i32,
    /// Response throttle time in milliseconds
    pub throttle_time_ms: i32,
    /// Topics with partition results
    pub topics: Vec<TxnOffsetCommitResponseTopic>,
}

/// Topic result for transactional offset commit
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnOffsetCommitResponseTopic {
    pub name: String,
    pub partitions: Vec<TxnOffsetCommitResponsePartition>,
}

/// Partition result for transactional offset commit
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnOffsetCommitResponsePartition {
    pub partition_index: i32,
    pub error_code: i16,
}

// ============================================================================
// Message Conversion Helpers
// ============================================================================

impl From<&TopicPartition> for AddPartitionsToTxnTopic {
    fn from(tp: &TopicPartition) -> Self {
        Self {
            name: tp.topic.clone(),
            partitions: vec![tp.partition],
        }
    }
}

impl AddPartitionsToTxnTopic {
    /// Create from multiple topic partitions, grouping by topic
    pub fn from_topic_partitions(partitions: &[TopicPartition]) -> Vec<Self> {
        let mut topics = std::collections::HashMap::new();

        for tp in partitions {
            topics
                .entry(tp.topic.clone())
                .or_insert_with(Vec::new)
                .push(tp.partition);
        }

        topics
            .into_iter()
            .map(|(name, partitions)| Self { name, partitions })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::kafka::messages::KafkaRequestHeader;

    #[test]
    fn test_init_producer_id_request() {
        let header = KafkaRequestHeader {
            api_key: 22,
            api_version: 4,
            correlation_id: 1,
            client_id: Some("test-client".to_string()),
        };

        let request = InitProducerIdRequest {
            header,
            transactional_id: Some("test-txn-id".to_string()),
            transaction_timeout_ms: 60000,
            producer_id: -1,
            producer_epoch: -1,
        };

        assert_eq!(request.transactional_id.as_ref().unwrap(), "test-txn-id");
        assert_eq!(request.transaction_timeout_ms, 60000);
    }

    #[test]
    fn test_add_partitions_to_txn_topic_grouping() {
        let partitions = vec![
            TopicPartition::new("topic1".to_string(), 0),
            TopicPartition::new("topic1".to_string(), 1),
            TopicPartition::new("topic2".to_string(), 0),
        ];

        let topics = AddPartitionsToTxnTopic::from_topic_partitions(&partitions);

        assert_eq!(topics.len(), 2);

        let topic1 = topics.iter().find(|t| t.name == "topic1").unwrap();
        assert_eq!(topic1.partitions.len(), 2);
        assert!(topic1.partitions.contains(&0));
        assert!(topic1.partitions.contains(&1));

        let topic2 = topics.iter().find(|t| t.name == "topic2").unwrap();
        assert_eq!(topic2.partitions.len(), 1);
        assert!(topic2.partitions.contains(&0));
    }

    #[test]
    fn test_transaction_marker_serialization() {
        let marker = WritableTxnMarker {
            producer_id: 12345,
            producer_epoch: 0,
            transaction_result: true,
            topics: vec![WritableTxnMarkerTopic {
                name: "test-topic".to_string(),
                partitions: vec![0, 1, 2],
            }],
            coordinator_epoch: 1,
        };

        let serialized = serde_json::to_string(&marker).unwrap();
        let deserialized: WritableTxnMarker = serde_json::from_str(&serialized).unwrap();

        assert_eq!(marker.producer_id, deserialized.producer_id);
        assert_eq!(marker.transaction_result, deserialized.transaction_result);
        assert_eq!(marker.topics.len(), deserialized.topics.len());
    }
}
