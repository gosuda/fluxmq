//! Kafka Wire Protocol Codec
//!
//! This module implements encoding and decoding of Kafka protocol messages
//! according to the official Kafka wire protocol specification.
//!
//! The Kafka protocol uses a binary format with the following structure:
//! - All integers are encoded in network byte order (big-endian)
//! - Strings are length-prefixed with int16 length
//! - Arrays are length-prefixed with int32 length
//! - Nullable fields use -1 to indicate null
//!
//! ## Sub-modules
//!
//! The codec is split into domain-specific sub-modules for maintainability:
//! - [`primitives`] - Wire format encoding/decoding helpers (strings, arrays, varint)
//! - [`frame`] - TCP message framing (length-prefixed messages)
//! - [`produce`] - Produce API (ApiKey 0)
//! - [`fetch`] - Fetch API (ApiKey 1)
//! - [`metadata`] - Metadata (3), ListOffsets (2), ApiVersions (18)
//! - [`consumer_group`] - Consumer Group APIs (ApiKeys 8-16)
//! - [`transaction`] - Transaction APIs (ApiKeys 22-28)
//! - [`admin`] - Admin, Security, and Cluster Coordination APIs

mod admin;
mod consumer_group;
mod fetch;
mod frame;
mod metadata;
mod primitives;
mod produce;
mod transaction;

pub use frame::KafkaFrameCodec;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::{self, Cursor};
use thiserror::Error;
use tracing::debug;

use super::messages::*;
use crate::protocol::kafka::{
    API_KEY_API_VERSIONS, API_KEY_CREATE_TOPICS, API_KEY_DELETE_TOPICS,
    API_KEY_DESCRIBE_GROUPS, API_KEY_FETCH, API_KEY_FIND_COORDINATOR,
    API_KEY_HEARTBEAT, API_KEY_JOIN_GROUP, API_KEY_LEAVE_GROUP, API_KEY_LIST_GROUPS,
    API_KEY_LIST_OFFSETS, API_KEY_METADATA, API_KEY_OFFSET_COMMIT, API_KEY_OFFSET_FETCH,
    API_KEY_PRODUCE, API_KEY_SYNC_GROUP,
    API_KEY_GET_TELEMETRY_SUBSCRIPTIONS,
    API_KEY_INIT_PRODUCER_ID, API_KEY_ADD_PARTITIONS_TO_TXN, API_KEY_ADD_OFFSETS_TO_TXN,
    API_KEY_END_TXN, API_KEY_WRITE_TXN_MARKERS, API_KEY_TXN_OFFSET_COMMIT,
};

#[derive(Debug, Error)]
pub enum KafkaCodecError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid message format: {0}")]
    InvalidFormat(String),
    #[error("Unsupported API version: key={0}, version={1}")]
    UnsupportedVersion(i16, i16),
    #[error("Unsupported API key: {0}")]
    UnsupportedApiKey(i16),
    #[error("Buffer underrun: needed {needed}, available {available}")]
    BufferUnderrun { needed: usize, available: usize },
}

pub type Result<T> = std::result::Result<T, KafkaCodecError>;

/// Kafka protocol codec for encoding/decoding messages
pub struct KafkaCodec;

impl KafkaCodec {
    pub fn new() -> Self {
        Self
    }

    /// Decode a Kafka request from bytes
    ///
    /// This is the public entry point. It wraps the internal decoder with panic
    /// protection to handle truncated/malformed requests gracefully instead of
    /// crashing. The Kafka wire protocol decoders use `Buf::get_*` methods which
    /// panic on insufficient data — this guard converts those panics to errors.
    pub fn decode_request(data: &mut Bytes) -> Result<KafkaRequest> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Self::decode_request_inner(data)
        })) {
            Ok(result) => result,
            Err(_) => Err(KafkaCodecError::BufferUnderrun {
                needed: 0,
                available: data.len(),
            }),
        }
    }

    /// Internal decode implementation (may panic on truncated data)
    fn decode_request_inner(data: &mut Bytes) -> Result<KafkaRequest> {
        if data.len() < 8 {
            return Err(KafkaCodecError::BufferUnderrun {
                needed: 8,
                available: data.len(),
            });
        }

        let mut cursor = Cursor::new(data.as_ref());

        // CRITICAL FIX: Frame decoder already stripped length prefix, don't read it again
        // let _length = cursor.get_i32();

        // Parse request header
        let api_key = cursor.get_i16();

        // DEBUG: Log first 16 bytes of request for debugging framing issues (trace level)
        #[cfg(debug_assertions)]
        if data.len() >= 16 && tracing::enabled!(tracing::Level::TRACE) {
            let first_16 = &data[0..16];
            tracing::trace!("decode_request: api_key={}, data_len={}, first_16_bytes=[{:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x}]",
                  api_key, data.len(),
                  first_16[0], first_16[1], first_16[2], first_16[3],
                  first_16[4], first_16[5], first_16[6], first_16[7],
                  first_16[8], first_16[9], first_16[10], first_16[11],
                  first_16[12], first_16[13], first_16[14], first_16[15]);
        }
        let api_version = cursor.get_i16();
        let correlation_id = cursor.get_i32();

        // JAVA CLIENT FIX: Handle flexible version headers
        // Kafka protocol uses HeaderVersion 2 (compact strings + tagged fields) for
        // various API versions. See is_flexible_version() in primitives.rs for full list.
        let is_flexible_header = match api_key {
            API_KEY_API_VERSIONS => api_version >= 3,
            API_KEY_INIT_PRODUCER_ID => api_version >= 2,
            API_KEY_ADD_PARTITIONS_TO_TXN => api_version >= 2,
            API_KEY_ADD_OFFSETS_TO_TXN => api_version >= 2,
            API_KEY_END_TXN => api_version >= 2,
            API_KEY_TXN_OFFSET_COMMIT => api_version >= 2,
            // Admin APIs with flexible headers
            37 => api_version >= 2,  // CreatePartitions v2+
            21 => api_version >= 2,  // DeleteRecords v2+
            42 => api_version >= 2,  // DeleteGroups v2+
            45 => true,              // AlterPartitionReassignments v0+ (flexible from start)
            32 => api_version >= 4,  // DescribeConfigs v4+
            33 => api_version >= 2,  // AlterConfigs v2+
            44 => true,              // IncrementalAlterConfigs v0+ (flexible from start)
            19 => api_version >= 5,  // CreateTopics v5+
            20 => api_version >= 4,  // DeleteTopics v4+
            16 => api_version >= 3,  // ListGroups v3+
            15 => api_version >= 5,  // DescribeGroups v5+
            11 => api_version >= 6,  // JoinGroup v6+
            14 => api_version >= 4,  // SyncGroup v4+
            13 => api_version >= 4,  // LeaveGroup v4+
            12 => api_version >= 4,  // Heartbeat v4+
            10 => api_version >= 3,  // FindCoordinator v3+
            9 => api_version >= 6,   // OffsetFetch v6+
            8 => api_version >= 8,   // OffsetCommit v8+
            2 => api_version >= 6,   // ListOffsets v6+ (not v4, v5 is NOT flexible)
            1 => api_version >= 12,  // Fetch v12+
            0 => api_version >= 9,   // Produce v9+
            _ => false,
        };

        // IMPORTANT: Per Kafka protocol spec, client_id in RequestHeader ALWAYS uses
        // int16 length prefix (NOT compact encoding) even in flexible versions (HeaderVersion 2).
        let client_id = Self::decode_nullable_string(&mut cursor)?;

        #[cfg(debug_assertions)]
        debug!("Decoded client_id: {:?} for API key {} v{}", client_id, api_key, api_version);

        // Skip tagged fields for flexible versions
        if is_flexible_header && cursor.remaining() > 0 {
            if let Ok(num_tags) = Self::decode_varint(&mut cursor) {
                if num_tags > 0 {
                    #[cfg(debug_assertions)]
                    debug!("Skipping {} tagged fields in header", num_tags);
                    for _ in 0..num_tags {
                        if cursor.remaining() == 0 {
                            break;
                        }
                        if Self::decode_varint(&mut cursor).is_err() {
                            break;
                        }
                        if cursor.remaining() == 0 {
                            break;
                        }
                        match Self::decode_varint(&mut cursor) {
                            Ok(tag_len) => {
                                let advance_len = tag_len as usize;
                                if advance_len <= cursor.remaining() {
                                    cursor.advance(advance_len);
                                } else {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        }

        let header = KafkaRequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
        };

        // Decode request body based on API key
        #[cfg(debug_assertions)]
        debug!("Decoding request for API key {}", api_key);
        match api_key {
            API_KEY_PRODUCE => {
                let request = Self::decode_produce_request(header, &mut cursor)?;
                Ok(KafkaRequest::Produce(request))
            }
            API_KEY_FETCH => {
                let request = Self::decode_fetch_request(header, &mut cursor)?;
                Ok(KafkaRequest::Fetch(request))
            }
            API_KEY_LIST_OFFSETS => {
                let request = Self::decode_list_offsets_request(header, &mut cursor)?;
                Ok(KafkaRequest::ListOffsets(request))
            }
            API_KEY_METADATA => {
                let request = Self::decode_metadata_request(header, &mut cursor)?;
                Ok(KafkaRequest::Metadata(request))
            }
            API_KEY_OFFSET_COMMIT => {
                let request = Self::decode_offset_commit_request(header, &mut cursor)?;
                Ok(KafkaRequest::OffsetCommit(request))
            }
            API_KEY_OFFSET_FETCH => {
                let request = Self::decode_offset_fetch_request(header, &mut cursor)?;
                Ok(KafkaRequest::OffsetFetch(request))
            }
            API_KEY_FIND_COORDINATOR => {
                let request = Self::decode_find_coordinator_request(header, &mut cursor)?;
                Ok(KafkaRequest::FindCoordinator(request))
            }
            API_KEY_LIST_GROUPS => {
                let request = Self::decode_list_groups_request(header, &mut cursor)?;
                Ok(KafkaRequest::ListGroups(request))
            }
            API_KEY_JOIN_GROUP => {
                let request = Self::decode_join_group_request(header, &mut cursor)?;
                Ok(KafkaRequest::JoinGroup(request))
            }
            API_KEY_HEARTBEAT => {
                let request = Self::decode_heartbeat_request(header, &mut cursor)?;
                Ok(KafkaRequest::Heartbeat(request))
            }
            API_KEY_LEAVE_GROUP => {
                let request = Self::decode_leave_group_request(header, &mut cursor)?;
                Ok(KafkaRequest::LeaveGroup(request))
            }
            API_KEY_SYNC_GROUP => {
                let request = Self::decode_sync_group_request(header, &mut cursor)?;
                Ok(KafkaRequest::SyncGroup(request))
            }
            API_KEY_DESCRIBE_GROUPS => {
                let request = Self::decode_describe_groups_request(header, &mut cursor)?;
                Ok(KafkaRequest::DescribeGroups(request))
            }
            API_KEY_API_VERSIONS => {
                let request = Self::decode_api_versions_request(header, &mut cursor)?;
                Ok(KafkaRequest::ApiVersions(request))
            }
            API_KEY_CREATE_TOPICS => {
                let request = Self::decode_create_topics_request(header, &mut cursor)?;
                Ok(KafkaRequest::CreateTopics(request))
            }
            API_KEY_DELETE_TOPICS => {
                let request = Self::decode_delete_topics_request(header, &mut cursor)?;
                Ok(KafkaRequest::DeleteTopics(request))
            }
            17 => { // API_KEY_SASL_HANDSHAKE
                let request = Self::decode_sasl_handshake_request(header, &mut cursor)?;
                Ok(KafkaRequest::SaslHandshake(request))
            }
            32 => { // API_KEY_DESCRIBE_CONFIGS
                let request = Self::decode_describe_configs_request(header, &mut cursor)?;
                Ok(KafkaRequest::DescribeConfigs(request))
            }
            33 => { // API_KEY_ALTER_CONFIGS
                let request = Self::decode_alter_configs_request(header, &mut cursor)?;
                Ok(KafkaRequest::AlterConfigs(request))
            }
            44 => { // API_KEY_INCREMENTAL_ALTER_CONFIGS
                let request = Self::decode_incremental_alter_configs_request(header, &mut cursor)?;
                Ok(KafkaRequest::IncrementalAlterConfigs(request))
            }
            36 => { // API_KEY_SASL_AUTHENTICATE
                let request = Self::decode_sasl_authenticate_request(header, &mut cursor)?;
                Ok(KafkaRequest::SaslAuthenticate(request))
            }
            API_KEY_GET_TELEMETRY_SUBSCRIPTIONS => {
                if cursor.remaining() < 16 {
                    return Err(KafkaCodecError::InvalidFormat("GET_TELEMETRY_SUBSCRIPTIONS insufficient data".to_string()));
                }
                let mut client_instance_id = [0u8; 16];
                cursor.copy_to_slice(&mut client_instance_id);

                let request = KafkaGetTelemetrySubscriptionsRequest {
                    header,
                    client_instance_id,
                };
                Ok(KafkaRequest::GetTelemetrySubscriptions(request))
            }
            API_KEY_INIT_PRODUCER_ID => {
                let request = Self::decode_init_producer_id_request(header, &mut cursor)?;
                Ok(KafkaRequest::InitProducerId(request))
            }
            API_KEY_ADD_PARTITIONS_TO_TXN => {
                let request = Self::decode_add_partitions_to_txn_request(header, &mut cursor)?;
                Ok(KafkaRequest::AddPartitionsToTxn(request))
            }
            API_KEY_ADD_OFFSETS_TO_TXN => {
                let request = Self::decode_add_offsets_to_txn_request(header, &mut cursor)?;
                Ok(KafkaRequest::AddOffsetsToTxn(request))
            }
            API_KEY_END_TXN => {
                let request = Self::decode_end_txn_request(header, &mut cursor)?;
                Ok(KafkaRequest::EndTxn(request))
            }
            API_KEY_WRITE_TXN_MARKERS => {
                let request = Self::decode_write_txn_markers_request(header, &mut cursor)?;
                Ok(KafkaRequest::WriteTxnMarkers(request))
            }
            API_KEY_TXN_OFFSET_COMMIT => {
                let request = Self::decode_txn_offset_commit_request(header, &mut cursor)?;
                Ok(KafkaRequest::TxnOffsetCommit(request))
            }
            21 => { // API_KEY_DELETE_RECORDS
                let request = Self::decode_delete_records_request(header, &mut cursor)?;
                Ok(KafkaRequest::DeleteRecords(request))
            }
            37 => { // API_KEY_CREATE_PARTITIONS
                let request = Self::decode_create_partitions_request(header, &mut cursor)?;
                Ok(KafkaRequest::CreatePartitions(request))
            }
            42 => { // API_KEY_DELETE_GROUPS
                let request = Self::decode_delete_groups_request(header, &mut cursor)?;
                Ok(KafkaRequest::DeleteGroups(request))
            }
            45 => { // API_KEY_ALTER_PARTITION_REASSIGNMENTS
                let request = Self::decode_alter_partition_reassignments_request(header, &mut cursor)?;
                Ok(KafkaRequest::AlterPartitionReassignments(request))
            }
            46 => { // API_KEY_LIST_PARTITION_REASSIGNMENTS
                let request = Self::decode_list_partition_reassignments_request(header, &mut cursor)?;
                Ok(KafkaRequest::ListPartitionReassignments(request))
            }
            47 => { // API_KEY_OFFSET_DELETE
                let request = Self::decode_offset_delete_request(header, &mut cursor)?;
                Ok(KafkaRequest::OffsetDelete(request))
            }
            60 => { // API_KEY_DESCRIBE_CLUSTER
                let request = Self::decode_describe_cluster_request(header, &mut cursor)?;
                Ok(KafkaRequest::DescribeCluster(request))
            }
            61 => { // API_KEY_DESCRIBE_PRODUCERS
                let request = Self::decode_describe_producers_request(header, &mut cursor)?;
                Ok(KafkaRequest::DescribeProducers(request))
            }
            72 => { // API_KEY_PUSH_TELEMETRY
                let request = Self::decode_push_telemetry_request(header, &mut cursor)?;
                Ok(KafkaRequest::PushTelemetry(request))
            }
            23 => { // API_KEY_OFFSET_FOR_LEADER_EPOCH
                let request = Self::decode_offset_for_leader_epoch_request(header, &mut cursor)?;
                Ok(KafkaRequest::OffsetForLeaderEpoch(request))
            }
            29 => { // API_KEY_DESCRIBE_ACLS
                let request = Self::decode_describe_acls_request(header, &mut cursor)?;
                Ok(KafkaRequest::DescribeAcls(request))
            }
            30 => { // API_KEY_CREATE_ACLS
                let request = Self::decode_create_acls_request(header, &mut cursor)?;
                Ok(KafkaRequest::CreateAcls(request))
            }
            31 => { // API_KEY_DELETE_ACLS
                let request = Self::decode_delete_acls_request(header, &mut cursor)?;
                Ok(KafkaRequest::DeleteAcls(request))
            }
            _ => Err(KafkaCodecError::UnsupportedVersion(api_key, api_version)),
        }
    }

    /// Encode a Kafka response to bytes
    pub fn encode_response(response: &KafkaResponse) -> Result<Bytes> {
        let mut buf = BytesMut::new();

        #[cfg(debug_assertions)]
        debug!("encode_response: Starting with empty buffer");

        match response {
            KafkaResponse::Produce(resp) => Self::encode_produce_response(resp, &mut buf)?,
            KafkaResponse::Fetch(resp) => Self::encode_fetch_response(resp, &mut buf)?,
            KafkaResponse::Metadata(resp) => {
                #[cfg(debug_assertions)]
                debug!("encode_response: About to encode Metadata response");
                Self::encode_metadata_response(resp, &mut buf)?;
                #[cfg(debug_assertions)]
                debug!("encode_response: Metadata response encoded, buffer size: {}", buf.len());

                if buf.len() >= 12 {
                    let first_12_bytes = &buf[0..12];
                    #[cfg(debug_assertions)]
                    debug!("encode_response: First 12 bytes after Metadata encoding: [{:02x} {:02x} {:02x} {:02x}] [{:02x} {:02x} {:02x} {:02x}] [{:02x} {:02x} {:02x} {:02x}]",
                          first_12_bytes[0], first_12_bytes[1], first_12_bytes[2], first_12_bytes[3],
                          first_12_bytes[4], first_12_bytes[5], first_12_bytes[6], first_12_bytes[7],
                          first_12_bytes[8], first_12_bytes[9], first_12_bytes[10], first_12_bytes[11]);
                }
            },
            KafkaResponse::OffsetCommit(resp) => {
                Self::encode_offset_commit_response(resp, resp.api_version, &mut buf)?
            }
            KafkaResponse::OffsetFetch(resp) => {
                Self::encode_offset_fetch_response(resp, resp.api_version, &mut buf)?
            }
            KafkaResponse::FindCoordinator(resp) => {
                Self::encode_find_coordinator_response(resp, &mut buf)?
            }
            KafkaResponse::ListGroups(resp) => Self::encode_list_groups_response(resp, &mut buf)?,
            KafkaResponse::JoinGroup(resp) => Self::encode_join_group_response(resp, &mut buf)?,
            KafkaResponse::Heartbeat(resp) => Self::encode_heartbeat_response(resp, &mut buf)?,
            KafkaResponse::LeaveGroup(resp) => Self::encode_leave_group_response(resp, &mut buf)?,
            KafkaResponse::SyncGroup(resp) => Self::encode_sync_group_response(resp, &mut buf)?,
            KafkaResponse::DescribeGroups(resp) => {
                Self::encode_describe_groups_response(resp, &mut buf)?
            }
            KafkaResponse::ListOffsets(resp) => Self::encode_list_offsets_response(resp, &mut buf)?,
            KafkaResponse::ApiVersions(resp) => Self::encode_api_versions_response(resp, &mut buf)?,
            KafkaResponse::CreateTopics(resp) => {
                Self::encode_create_topics_response(resp, &mut buf)?
            }
            KafkaResponse::DeleteTopics(resp) => {
                Self::encode_delete_topics_response(resp, &mut buf)?
            }
            KafkaResponse::DescribeConfigs(resp) => {
                Self::encode_describe_configs_response(resp, &mut buf)?
            }
            KafkaResponse::AlterConfigs(resp) => {
                Self::encode_alter_configs_response(resp, &mut buf)?
            }
            KafkaResponse::IncrementalAlterConfigs(resp) => {
                Self::encode_incremental_alter_configs_response(resp, &mut buf)?
            }
            KafkaResponse::SaslHandshake(resp) => {
                Self::encode_sasl_handshake_response(resp, &mut buf)?
            }
            KafkaResponse::SaslAuthenticate(resp) => {
                Self::encode_sasl_authenticate_response(resp, &mut buf)?
            }
            KafkaResponse::GetTelemetrySubscriptions(resp) => {
                // Flexible format (v0+) - uses compact arrays and tagged fields
                buf.put_i32(resp.header.correlation_id);
                buf.put_u8(0); // Empty tagged fields in header

                buf.put_i32(resp.throttle_time_ms);
                buf.put_i16(resp.error_code);
                buf.extend_from_slice(&resp.client_instance_id);
                buf.put_i32(resp.subscription_id);

                // Accepted compression types - compact array
                Self::encode_varint(&mut buf, (resp.accepted_compression_types.len() + 1) as u64);
                for compression_type in &resp.accepted_compression_types {
                    buf.put_i8(*compression_type);
                }

                buf.put_i32(resp.push_interval_ms);
                buf.put_i32(resp.telemetry_max_bytes);
                buf.put_u8(if resp.delta_temporality { 1 } else { 0 });

                // Requested metrics - compact array of compact strings
                Self::encode_varint(&mut buf, (resp.requested_metrics.len() + 1) as u64);
                for metric in &resp.requested_metrics {
                    Self::encode_compact_string(metric, &mut buf);
                }

                buf.put_u8(0); // Empty tagged fields at end
            }
            // Cluster Coordination APIs (Inter-broker communication)
            KafkaResponse::LeaderAndIsr(resp) => {
                Self::encode_leader_and_isr_response(resp, &mut buf)?;
            }
            KafkaResponse::StopReplica(resp) => {
                Self::encode_stop_replica_response(resp, &mut buf)?;
            }
            KafkaResponse::UpdateMetadata(resp) => {
                Self::encode_update_metadata_response(resp, &mut buf)?;
            }
            KafkaResponse::ControlledShutdown(resp) => {
                Self::encode_controlled_shutdown_response(resp, &mut buf)?;
            }
            // Transaction APIs
            KafkaResponse::InitProducerId(response) => {
                Self::encode_init_producer_id_response(&mut buf, response)?;
            }
            KafkaResponse::AddPartitionsToTxn(response) => {
                Self::encode_add_partitions_to_txn_response(&mut buf, response)?;
            }
            KafkaResponse::AddOffsetsToTxn(response) => {
                Self::encode_add_offsets_to_txn_response(&mut buf, response)?;
            }
            KafkaResponse::EndTxn(response) => {
                Self::encode_end_txn_response(&mut buf, response)?;
            }
            KafkaResponse::WriteTxnMarkers(response) => {
                Self::encode_write_txn_markers_response(&mut buf, response)?;
            }
            KafkaResponse::TxnOffsetCommit(response) => {
                Self::encode_txn_offset_commit_response(&mut buf, response)?;
            }
            // Admin APIs
            KafkaResponse::DeleteRecords(resp) => {
                Self::encode_delete_records_response(resp, &mut buf)?;
            }
            KafkaResponse::CreatePartitions(resp) => {
                Self::encode_create_partitions_response(resp, &mut buf)?;
            }
            KafkaResponse::DeleteGroups(resp) => {
                Self::encode_delete_groups_response(resp, &mut buf)?;
            }
            KafkaResponse::AlterPartitionReassignments(resp) => {
                Self::encode_alter_partition_reassignments_response(resp, &mut buf)?;
            }
            KafkaResponse::ListPartitionReassignments(resp) => {
                Self::encode_list_partition_reassignments_response(resp, &mut buf)?;
            }
            KafkaResponse::OffsetDelete(resp) => {
                Self::encode_offset_delete_response(resp, &mut buf)?;
            }
            KafkaResponse::DescribeCluster(resp) => {
                Self::encode_describe_cluster_response(resp, &mut buf)?;
            }
            KafkaResponse::DescribeProducers(resp) => {
                Self::encode_describe_producers_response(resp, &mut buf)?;
            }
            KafkaResponse::PushTelemetry(resp) => {
                Self::encode_push_telemetry_response(resp, &mut buf)?;
            }
            KafkaResponse::OffsetForLeaderEpoch(resp) => {
                Self::encode_offset_for_leader_epoch_response(resp, &mut buf)?;
            }
            KafkaResponse::DescribeAcls(resp) => {
                Self::encode_describe_acls_response(resp, &mut buf)?;
            }
            KafkaResponse::CreateAcls(resp) => {
                Self::encode_create_acls_response(resp, &mut buf)?;
            }
            KafkaResponse::DeleteAcls(resp) => {
                Self::encode_delete_acls_response(resp, &mut buf)?;
            }
        }

        #[cfg(debug_assertions)]
        debug!("Buffer size: {}, first 8 bytes should be correlation_id + throttle_time", buf.len());
        if buf.len() >= 8 {
            let first_8_bytes = &buf[0..8];
            #[cfg(debug_assertions)]
            debug!("First 8 bytes: [{:02x} {:02x} {:02x} {:02x}] [{:02x} {:02x} {:02x} {:02x}]",
                  first_8_bytes[0], first_8_bytes[1], first_8_bytes[2], first_8_bytes[3],
                  first_8_bytes[4], first_8_bytes[5], first_8_bytes[6], first_8_bytes[7]);
        }

        Ok(buf.freeze())
    }
}
