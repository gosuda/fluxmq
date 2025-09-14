//! Protocol tests for FluxMQ core functionality
//!
//! Note: The old bincode serialization tests have been removed as FluxMQ
//! now uses Kafka wire protocol exclusively for all communication.
//!
//! For protocol compatibility testing, see the comprehensive test suite
//! in fluxmq-java-tests/ which validates Kafka wire protocol compliance.

#[cfg(test)]
mod tests {
    use crate::{consumer::TopicPartition, protocol::*};
    use bytes::Bytes;

    #[test]
    fn test_message_creation() {
        let message = Message::new("test value");
        assert_eq!(message.value, Bytes::from("test value"));
        assert!(message.key.is_none());
        assert!(message.timestamp > 0);
        assert!(message.headers.is_empty());
    }

    #[test]
    fn test_message_with_key() {
        let message = Message::new("test value").with_key("test key");
        assert_eq!(message.key, Some(Bytes::from("test key")));
        assert_eq!(message.value, Bytes::from("test value"));
    }

    #[test]
    fn test_message_with_header() {
        let message = Message::new("test value").with_header("content-type", "application/json");

        assert_eq!(
            message.headers.get("content-type"),
            Some(&Bytes::from("application/json"))
        );
    }

    #[test]
    fn test_topic_partition_creation() {
        let tp = TopicPartition {
            topic: "test-topic".to_string(),
            partition: 42,
        };

        assert_eq!(tp.topic, "test-topic");
        assert_eq!(tp.partition, 42);
    }

    #[test]
    fn test_broker_metadata_creation() {
        let broker = BrokerMetadata {
            node_id: 1,
            host: "localhost".to_string(),
            port: 9092,
        };

        assert_eq!(broker.node_id, 1);
        assert_eq!(broker.host, "localhost");
        assert_eq!(broker.port, 9092);
    }

    #[test]
    fn test_partition_metadata_creation() {
        let partition = PartitionMetadata {
            id: 0,
            leader: Some(1),
            replicas: vec![1, 2, 3],
            isr: vec![1, 2],
            leader_epoch: 5,
        };

        assert_eq!(partition.id, 0);
        assert_eq!(partition.leader, Some(1));
        assert_eq!(partition.replicas, vec![1, 2, 3]);
        assert_eq!(partition.isr, vec![1, 2]);
        assert_eq!(partition.leader_epoch, 5);
    }

    #[test]
    fn test_topic_metadata_creation() {
        let partition = PartitionMetadata {
            id: 0,
            leader: Some(1),
            replicas: vec![1],
            isr: vec![1],
            leader_epoch: 0,
        };

        let topic = TopicMetadata {
            name: "test-topic".to_string(),
            error_code: 0,
            partitions: vec![partition],
        };

        assert_eq!(topic.name, "test-topic");
        assert_eq!(topic.error_code, 0);
        assert_eq!(topic.partitions.len(), 1);
        assert_eq!(topic.partitions[0].id, 0);
    }

    // Note: Kafka wire protocol encoding/decoding tests are covered by
    // the comprehensive Java client compatibility tests in fluxmq-java-tests/
    // These tests validate actual Kafka protocol compliance with real clients.
}
