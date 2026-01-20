//! Kafka Wire Protocol Implementation
//!
//! This module provides compatibility with the Apache Kafka wire protocol,
//! enabling FluxMQ to work with existing Kafka clients and tools.
//!
//! ## Protocol Structure
//!
//! Kafka uses a binary protocol over TCP. All requests have the following format:
//! ```text
//! RequestMessage => MessageSize RequestHeader RequestBody
//! MessageSize => int32
//! RequestHeader => api_key api_version correlation_id client_id
//! api_key => int16
//! api_version => int16
//! correlation_id => int32
//! client_id => nullable_string
//! ```
//!
//! ## Supported APIs
//!
//! ### Priority 1 - Core Messaging
//! - **ApiKey 0**: Produce - Send messages to topics
//! - **ApiKey 1**: Fetch - Consume messages from topics  
//! - **ApiKey 3**: Metadata - Get topic/partition metadata
//!
//! ### Priority 2 - Consumer Groups
//! - **ApiKey 11**: JoinGroup - Join consumer group
//! - **ApiKey 14**: SyncGroup - Sync partition assignments
//! - **ApiKey 12**: Heartbeat - Keep group membership alive
//! - **ApiKey 13**: LeaveGroup - Leave consumer group
//!
//! ### Priority 3 - Advanced Features
//! - **ApiKey 8**: OffsetCommit - Commit consumer offsets
//! - **ApiKey 9**: OffsetFetch - Fetch committed offsets
//! - **ApiKey 10**: ListGroups - List consumer groups

pub mod adapter;
pub mod api_versions;
pub mod codec;
pub mod errors;
pub mod messages;

pub use adapter::*;
pub use api_versions::*;
pub use codec::{KafkaCodec, KafkaCodecError, KafkaFrameCodec};
pub use errors::*;
pub use messages::*;
use tracing::info;

/// Kafka API Keys - Core messaging APIs
pub const API_KEY_PRODUCE: i16 = 0;
pub const API_KEY_FETCH: i16 = 1;
pub const API_KEY_LIST_OFFSETS: i16 = 2;
pub const API_KEY_METADATA: i16 = 3;

/// Kafka API Keys - Consumer group APIs
pub const API_KEY_OFFSET_COMMIT: i16 = 8;
pub const API_KEY_OFFSET_FETCH: i16 = 9;
pub const API_KEY_FIND_COORDINATOR: i16 = 10;
pub const API_KEY_JOIN_GROUP: i16 = 11;
pub const API_KEY_HEARTBEAT: i16 = 12;
pub const API_KEY_LEAVE_GROUP: i16 = 13;
pub const API_KEY_SYNC_GROUP: i16 = 14;

/// Kafka API Keys - Admin APIs
pub const API_KEY_DESCRIBE_GROUPS: i16 = 15;
pub const API_KEY_LIST_GROUPS: i16 = 16;
pub const API_KEY_SASL_HANDSHAKE: i16 = 17;
pub const API_KEY_API_VERSIONS: i16 = 18;
pub const API_KEY_CREATE_TOPICS: i16 = 19;
pub const API_KEY_DELETE_TOPICS: i16 = 20;

/// Kafka API Keys - Transaction APIs (exactly-once semantics)
pub const API_KEY_INIT_PRODUCER_ID: i16 = 22;
pub const API_KEY_ADD_PARTITIONS_TO_TXN: i16 = 24;
pub const API_KEY_ADD_OFFSETS_TO_TXN: i16 = 25;
pub const API_KEY_END_TXN: i16 = 26;
pub const API_KEY_WRITE_TXN_MARKERS: i16 = 27;
pub const API_KEY_TXN_OFFSET_COMMIT: i16 = 28;

/// Kafka API Keys - Configuration APIs
pub const API_KEY_DESCRIBE_CONFIGS: i16 = 32;
pub const API_KEY_ALTER_CONFIGS: i16 = 33;
pub const API_KEY_SASL_AUTHENTICATE: i16 = 36;

/// KIP-714 - Client metrics and observability
pub const API_KEY_GET_TELEMETRY_SUBSCRIPTIONS: i16 = 71;
pub const API_KEY_PUSH_TELEMETRY: i16 = 72;

/// Additional Admin APIs (Kafka 4.1.0 compatibility)
pub const API_KEY_LIST_PARTITION_REASSIGNMENTS: i16 = 46;
pub const API_KEY_OFFSET_DELETE: i16 = 47;
pub const API_KEY_DESCRIBE_CLUSTER: i16 = 60;
pub const API_KEY_DESCRIBE_PRODUCERS: i16 = 61;

/// Phase 2 APIs - Replication and Security
pub const API_KEY_OFFSET_FOR_LEADER_EPOCH: i16 = 23;
pub const API_KEY_DESCRIBE_ACLS: i16 = 29;
pub const API_KEY_CREATE_ACLS: i16 = 30;
pub const API_KEY_DELETE_ACLS: i16 = 31;

/// Protocol detection magic bytes
/// Kafka requests typically start with message length > 0 and reasonable API keys (0-50)
pub fn is_kafka_request(data: &[u8]) -> bool {
    info!(
        "Detecting protocol: data_len={}, first_8_bytes={:?}",
        data.len(),
        if data.len() >= 8 { &data[0..8] } else { data }
    );

    if data.len() < 8 {
        info!("Protocol detection: data too short ({})", data.len());
        return false;
    }

    // Parse message length (first 4 bytes)
    let length = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    info!("Protocol detection: message_length={}", length);

    // Message length should be reasonable (4 bytes to 100MB)
    if length < 4 || length > 100_000_000 {
        info!("Protocol detection: invalid message length {}", length);
        return false;
    }

    // Parse API key (next 2 bytes after length)
    let api_key = i16::from_be_bytes([data[4], data[5]]);
    info!("Protocol detection: api_key={}", api_key);

    // Check if API key is in known Kafka range - now includes transaction APIs
    let is_kafka = matches!(
        api_key,
        API_KEY_PRODUCE
            | API_KEY_FETCH
            | API_KEY_LIST_OFFSETS
            | API_KEY_METADATA
            | API_KEY_OFFSET_COMMIT
            | API_KEY_OFFSET_FETCH
            | API_KEY_LIST_GROUPS
            | API_KEY_JOIN_GROUP
            | API_KEY_HEARTBEAT
            | API_KEY_LEAVE_GROUP
            | API_KEY_SYNC_GROUP
            | API_KEY_DESCRIBE_GROUPS
            | API_KEY_SASL_HANDSHAKE
            | API_KEY_API_VERSIONS
            | API_KEY_CREATE_TOPICS
            | API_KEY_DELETE_TOPICS
            | API_KEY_INIT_PRODUCER_ID
            | API_KEY_ADD_PARTITIONS_TO_TXN
            | API_KEY_ADD_OFFSETS_TO_TXN
            | API_KEY_END_TXN
            | API_KEY_WRITE_TXN_MARKERS
            | API_KEY_TXN_OFFSET_COMMIT
            | API_KEY_DESCRIBE_CONFIGS
            | API_KEY_ALTER_CONFIGS
            | API_KEY_SASL_AUTHENTICATE
            | API_KEY_GET_TELEMETRY_SUBSCRIPTIONS
            | 15..=75 // Allow room for additional Kafka APIs
    );

    info!("Protocol detection result: is_kafka={}", is_kafka);
    is_kafka
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_request_detection() {
        // Valid Kafka produce request header
        let kafka_data = [
            0, 0, 0, 20, // Length: 20 bytes
            0, 0, // API Key: 0 (Produce)
            0, 1, // API Version: 1
            0, 0, 0, 1, // Correlation ID: 1
        ];
        assert!(is_kafka_request(&kafka_data));

        // Invalid: too short
        let short_data = [0, 0, 0];
        assert!(!is_kafka_request(&short_data));

        // Invalid: unreasonable length
        let bad_length = [255, 255, 255, 255, 0, 0];
        assert!(!is_kafka_request(&bad_length));

        // Invalid: unknown API key
        let unknown_api = [
            0, 0, 0, 20, // Length: 20 bytes
            99, 99, // API Key: 99 (unknown)
            0, 1, // API Version: 1
        ];
        assert!(!is_kafka_request(&unknown_api));
    }
}
