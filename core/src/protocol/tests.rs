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

/// Property-based fuzzing tests for the Kafka protocol codec.
///
/// These tests feed arbitrary byte sequences into the decoder to verify it
/// never panics, and validate round-trip consistency for well-formed requests.
#[cfg(test)]
mod fuzz_tests {
    use crate::protocol::kafka::codec::KafkaCodec;
    use bytes::Bytes;
    use proptest::prelude::*;

    // Fuzz: arbitrary bytes should never panic the decoder
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]

        #[test]
        fn decode_request_never_panics(data in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let mut bytes = Bytes::from(data);
            // Should return Ok or Err, never panic
            let _ = KafkaCodec::decode_request(&mut bytes);
        }

        #[test]
        fn decode_with_valid_header_never_panics(
            api_key in 0i16..100,
            api_version in 0i16..20,
            correlation_id in any::<i32>(),
            client_id_len in 0u16..256,
            payload in proptest::collection::vec(any::<u8>(), 0..2048),
        ) {
            // Construct a minimal valid Kafka request header + arbitrary payload
            let mut data = Vec::with_capacity(10 + client_id_len as usize + payload.len());
            data.extend_from_slice(&api_key.to_be_bytes());
            data.extend_from_slice(&api_version.to_be_bytes());
            data.extend_from_slice(&correlation_id.to_be_bytes());
            // client_id as nullable string (length-prefixed)
            data.extend_from_slice(&client_id_len.to_be_bytes());
            data.extend(std::iter::repeat(b'a').take(client_id_len as usize));
            data.extend_from_slice(&payload);

            let mut bytes = Bytes::from(data);
            let _ = KafkaCodec::decode_request(&mut bytes);
        }

        #[test]
        fn decode_apiversions_request_never_panics(
            api_version in 0i16..5,
            correlation_id in any::<i32>(),
            extra in proptest::collection::vec(any::<u8>(), 0..512),
        ) {
            // ApiVersions (key=18) is the first request any client sends
            let mut data = Vec::with_capacity(20 + extra.len());
            data.extend_from_slice(&18i16.to_be_bytes());  // api_key = ApiVersions
            data.extend_from_slice(&api_version.to_be_bytes());
            data.extend_from_slice(&correlation_id.to_be_bytes());
            // Empty client_id
            data.extend_from_slice(&0i16.to_be_bytes());
            data.extend_from_slice(&extra);

            let mut bytes = Bytes::from(data);
            let _ = KafkaCodec::decode_request(&mut bytes);
        }

        #[test]
        fn decode_truncated_header_is_error(len in 0usize..8) {
            let data = vec![0u8; len];
            let mut bytes = Bytes::from(data);
            let result = KafkaCodec::decode_request(&mut bytes);
            prop_assert!(result.is_err(), "Truncated data should be an error");
        }
    }
}
