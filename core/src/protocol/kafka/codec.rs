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

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::{self, Cursor, Read};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};
use tracing::{debug, warn, error};

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
use crate::transaction::messages::{
    InitProducerIdRequest, AddPartitionsToTxnRequest, AddPartitionsToTxnTopic,
    AddOffsetsToTxnRequest, EndTxnRequest, WriteTxnMarkersRequest,
    WritableTxnMarker, WritableTxnMarkerTopic, TxnOffsetCommitRequest,
    TxnOffsetCommitRequestTopic, TxnOffsetCommitRequestPartition,
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
    pub fn decode_request(data: &mut Bytes) -> Result<KafkaRequest> {
        if data.len() < 8 {
            return Err(KafkaCodecError::BufferUnderrun {
                needed: 8,
                available: data.len(),
            });
        }

        let mut cursor = Cursor::new(data.as_ref());

        // CRITICAL FIX: Frame decoder already stripped length prefix, don't read it again
        // Comment out the erroneous length consumption that was causing 4-byte offset
        // let _length = cursor.get_i32();

        // Parse request header
        let api_key = cursor.get_i16();

        // DEBUG: Log first 16 bytes of request for debugging framing issues (trace level)
        #[cfg(debug_assertions)]
        if data.len() >= 16 && tracing::enabled!(tracing::Level::TRACE) {
            let first_16 = &data[0..16];
            tracing::trace!("🔍 decode_request: api_key={}, data_len={}, first_16_bytes=[{:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x}]",
                  api_key, data.len(),
                  first_16[0], first_16[1], first_16[2], first_16[3],
                  first_16[4], first_16[5], first_16[6], first_16[7],
                  first_16[8], first_16[9], first_16[10], first_16[11],
                  first_16[12], first_16[13], first_16[14], first_16[15]);
        }
        let api_version = cursor.get_i16();
        let correlation_id = cursor.get_i32();
        
        // JAVA CLIENT FIX: Handle flexible version headers
        // Kafka protocol uses HeaderVersion 2 (compact strings + tagged fields) for:
        // - ApiVersions v3+
        // - InitProducerId v2+ (API key 22)
        // - AddPartitionsToTxn v2+ (API key 24)
        // - AddOffsetsToTxn v2+ (API key 25)
        // - EndTxn v2+ (API key 26)
        // - TxnOffsetCommit v2+ (API key 28)
        // - CreatePartitions v2+ (API key 37)
        // - DeleteRecords v2+ (API key 21)
        // - DeleteGroups v2+ (API key 42)
        // - AlterPartitionReassignments v0+ (API key 45)
        // - DescribeConfigs v4+ (API key 32)
        // - AlterConfigs v2+ (API key 33)
        // - CreateTopics v5+ (API key 19)
        // - DeleteTopics v4+ (API key 20)
        // - Metadata v9+ (API key 3) - but we use v8
        // - ListGroups v3+ (API key 16)
        // - DescribeGroups v5+ (API key 15)
        // - JoinGroup v6+ (API key 11)
        // - SyncGroup v4+ (API key 14)
        // - LeaveGroup v4+ (API key 13)
        // - Heartbeat v4+ (API key 12)
        // - FindCoordinator v3+ (API key 10)
        // - OffsetFetch v6+ (API key 9)
        // - OffsetCommit v8+ (API key 8)
        // - ListOffsets v4+ (API key 2)
        // - Fetch v12+ (API key 1)
        // - Produce v9+ (API key 0)
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
        // This is for backward compatibility - see RequestHeader.json: "flexibleVersions": "none" for ClientId
        let client_id = Self::decode_nullable_string(&mut cursor)?;

        #[cfg(debug_assertions)]
        debug!("Decoded client_id: {:?} for API key {} v{}", client_id, api_key, api_version);

        // Skip tagged fields for flexible versions
        // Note: tagged fields are a varint count followed by tag+length+value for each tag
        // Usually the count is 0 (a single 0x00 byte)
        if is_flexible_header && cursor.remaining() > 0 {
            // Just read the tagged fields count (usually 0)
            // We don't need to actually process the tags for request headers
            if let Ok(num_tags) = Self::decode_varint(&mut cursor) {
                if num_tags > 0 {
                    #[cfg(debug_assertions)]
                    debug!("Skipping {} tagged fields in header", num_tags);
                    // Skip each tagged field: tag (varint) + length (varint) + data
                    for _ in 0..num_tags {
                        if cursor.remaining() == 0 {
                            break;
                        }
                        // Tag ID
                        if Self::decode_varint(&mut cursor).is_err() {
                            break;
                        }
                        if cursor.remaining() == 0 {
                            break;
                        }
                        // Tag data length
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
                // Simple implementation - just return a basic request
                // Client instance ID (16 bytes UUID)
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
        debug!("🎯 encode_response: Starting with empty buffer");

        match response {
            KafkaResponse::Produce(resp) => Self::encode_produce_response(resp, &mut buf)?,
            KafkaResponse::Fetch(resp) => Self::encode_fetch_response(resp, &mut buf)?,
            KafkaResponse::Metadata(resp) => {
                #[cfg(debug_assertions)]
                debug!("📝 encode_response: About to encode Metadata response");
                Self::encode_metadata_response(resp, &mut buf)?;
                #[cfg(debug_assertions)]
                debug!("✅ encode_response: Metadata response encoded, buffer size: {}", buf.len());
                
                // DEBUG: Check first 12 bytes of encoded response
                if buf.len() >= 12 {
                    let first_12_bytes = &buf[0..12];
                    #[cfg(debug_assertions)]
                    debug!("📊 encode_response: First 12 bytes after Metadata encoding: [{:02x} {:02x} {:02x} {:02x}] [{:02x} {:02x} {:02x} {:02x}] [{:02x} {:02x} {:02x} {:02x}]",
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
                // Header with tagged fields
                buf.put_i32(resp.header.correlation_id);
                buf.put_u8(0); // Empty tagged fields in header

                // Body
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
            // New Admin APIs (Kafka 4.1.0 compatibility)
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

        // CRITICAL FIX: Remove double encoding! 
        // KafkaFrameCodec already adds length prefix, so we don't need it here
        // This was causing [LENGTH_PREFIX][BUFFER_SIZE][CORRELATION_ID] instead of [LENGTH_PREFIX][CORRELATION_ID]
        
        #[cfg(debug_assertions)]
        debug!("🔧 FIXED: Removed double length encoding, returning buffer directly");
        #[cfg(debug_assertions)]
        debug!("🔧 Buffer size: {}, first 8 bytes should be correlation_id + throttle_time", buf.len());
        if buf.len() >= 8 {
            let first_8_bytes = &buf[0..8];
            #[cfg(debug_assertions)]
            debug!("🔧 First 8 bytes: [{:02x} {:02x} {:02x} {:02x}] [{:02x} {:02x} {:02x} {:02x}]", 
                  first_8_bytes[0], first_8_bytes[1], first_8_bytes[2], first_8_bytes[3],
                  first_8_bytes[4], first_8_bytes[5], first_8_bytes[6], first_8_bytes[7]);
        }

        Ok(buf.freeze())
    }

    // ========================================================================
    // PRODUCE REQUEST/RESPONSE
    // ========================================================================

    fn decode_produce_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaProduceRequest> {
        // transactional_id only exists in API v3+
        let transactional_id = if header.api_version >= 3 {
            Self::decode_nullable_string(cursor)?
        } else {
            None
        };
        let acks = cursor.get_i16();
        let timeout_ms = cursor.get_i32();

        let topic_count = cursor.get_i32();
        #[cfg(debug_assertions)]
        debug!("Decoding produce request: topic_count={}", topic_count);
        let mut topic_data = Vec::with_capacity(topic_count as usize);

        for i in 0..topic_count {
            let topic = Self::decode_string(cursor)?;
            #[cfg(debug_assertions)]
            debug!("Decoded topic {}: '{}'", i, topic);
            let partition_count = cursor.get_i32();
            #[cfg(debug_assertions)]
            debug!("Topic '{}' has {} partitions", topic, partition_count);
            let mut partition_data = Vec::with_capacity(partition_count as usize);

            for _ in 0..partition_count {
                let partition = cursor.get_i32();
                let records = Self::decode_nullable_bytes(cursor)?;
                partition_data.push(KafkaPartitionProduceData { partition, records });
            }

            topic_data.push(KafkaTopicProduceData {
                topic,
                partition_data,
            });
        }

        Ok(KafkaProduceRequest {
            header,
            transactional_id,
            acks,
            timeout_ms,
            topic_data,
        })
    }

    fn encode_produce_response(response: &KafkaProduceResponse, buf: &mut BytesMut) -> Result<()> {
        let api_version = response.api_version;
        // Produce becomes flexible at v9
        let flexible = api_version >= 9;

        buf.put_i32(response.header.correlation_id);

        // Response header tagged fields for flexible versions
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields in header
        }

        // Topics array
        if flexible {
            Self::encode_varint(buf, (response.responses.len() + 1) as u64); // Compact array
        } else {
            buf.put_i32(response.responses.len() as i32);
        }

        for topic_response in &response.responses {
            if flexible {
                Self::encode_compact_string(&topic_response.topic, buf);
            } else {
                Self::encode_string(&topic_response.topic, buf);
            }

            // Partitions array
            if flexible {
                Self::encode_varint(buf, (topic_response.partition_responses.len() + 1) as u64);
            } else {
                buf.put_i32(topic_response.partition_responses.len() as i32);
            }

            for partition_response in &topic_response.partition_responses {
                buf.put_i32(partition_response.partition);
                buf.put_i16(partition_response.error_code);
                buf.put_i64(partition_response.base_offset);
                buf.put_i64(partition_response.log_append_time_ms);
                buf.put_i64(partition_response.log_start_offset);

                // Partition-level tagged fields for flexible versions
                if flexible {
                    Self::encode_varint(buf, 0); // No tagged fields
                }
            }

            // Topic-level tagged fields for flexible versions
            if flexible {
                Self::encode_varint(buf, 0); // No tagged fields
            }
        }

        buf.put_i32(response.throttle_time_ms);

        // Response-level tagged fields for flexible versions
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields
        }

        Ok(())
    }

    // ========================================================================
    // FETCH REQUEST/RESPONSE
    // ========================================================================

    fn decode_fetch_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaFetchRequest> {
        #[cfg(debug_assertions)]
        tracing::debug!("Decoding Fetch request: api_version={}", header.api_version);
        let replica_id = cursor.get_i32();
        let max_wait_ms = cursor.get_i32();
        let min_bytes = cursor.get_i32();
        let max_bytes = cursor.get_i32();
        let isolation_level = cursor.get_i8();
        let session_id = cursor.get_i32();
        let session_epoch = cursor.get_i32();

        let topic_count = cursor.get_i32();
        let mut topics = Vec::with_capacity(topic_count as usize);

        for _ in 0..topic_count {
            let topic = Self::decode_string(cursor)?;
            let partition_count = cursor.get_i32();
            let mut partitions = Vec::with_capacity(partition_count as usize);

            for _ in 0..partition_count {
                let partition = cursor.get_i32();
                let current_leader_epoch = cursor.get_i32();
                let fetch_offset = cursor.get_i64();
                let log_start_offset = cursor.get_i64();
                let max_bytes = cursor.get_i32();

                partitions.push(KafkaPartitionFetchData {
                    partition,
                    current_leader_epoch,
                    fetch_offset,
                    log_start_offset,
                    max_bytes,
                });
            }

            topics.push(KafkaTopicFetchData { topic, partitions });
        }

        // Decode forgotten topics (simplified for now)
        let forgotten_topic_count = cursor.get_i32();
        let mut forgotten_topics_data = Vec::with_capacity(forgotten_topic_count as usize);

        for _ in 0..forgotten_topic_count {
            let topic = Self::decode_string(cursor)?;
            let partition_count = cursor.get_i32();
            let mut partitions = Vec::with_capacity(partition_count as usize);

            for _ in 0..partition_count {
                partitions.push(cursor.get_i32());
            }

            forgotten_topics_data.push(KafkaForgottenTopic { topic, partitions });
        }

        Ok(KafkaFetchRequest {
            header,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data,
        })
    }

    fn encode_fetch_response(response: &KafkaFetchResponse, buf: &mut BytesMut) -> Result<()> {
        // Use the actual API version from the request
        let api_version = response.api_version;
        // Fetch API: flexible versions start at v12
        let flexible = api_version >= 12;

        buf.put_i32(response.header.correlation_id);

        // Flexible versions: header tagged fields
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields in header
        }

        // throttle_time_ms is the first field of the response body in v1+
        if api_version >= 1 {
            buf.put_i32(response.throttle_time_ms);
        }

        // FetchResponse v7+ structure:
        // throttle_time_ms (int32) - v1+
        // error_code (int16) - v7+
        // session_id (int32) - v7+
        // responses (array)

        // error_code only in v7+
        if api_version >= 7 {
            buf.put_i16(response.error_code);
        }

        // session_id only in v7+
        if api_version >= 7 {
            buf.put_i32(response.session_id);
        }

        // Topics array
        if flexible {
            // Compact array: length + 1 as varint
            Self::encode_varint(buf, (response.responses.len() + 1) as u64);
        } else {
            buf.put_i32(response.responses.len() as i32);
        }

        for topic_response in &response.responses {
            // Topic name
            if flexible {
                Self::encode_compact_string(&topic_response.topic, buf);
            } else {
                Self::encode_string(&topic_response.topic, buf);
            }

            // Partitions array
            if flexible {
                Self::encode_varint(buf, (topic_response.partitions.len() + 1) as u64);
            } else {
                buf.put_i32(topic_response.partitions.len() as i32);
            }

            for partition_response in &topic_response.partitions {
                buf.put_i32(partition_response.partition);
                buf.put_i16(partition_response.error_code);
                buf.put_i64(partition_response.high_watermark);

                // last_stable_offset only in v4+
                if api_version >= 4 {
                    buf.put_i64(partition_response.last_stable_offset);
                }

                // log_start_offset only in v5+
                if api_version >= 5 {
                    buf.put_i64(partition_response.log_start_offset);
                }

                // aborted_transactions only in v4+ (nullable array)
                // For non-transactional reads, send null (-1) instead of empty array (0)
                if api_version >= 4 {
                    if flexible {
                        // Compact nullable array: 0 means null
                        Self::encode_varint(buf, 0);
                    } else {
                        buf.put_i32(-1); // Null aborted transactions (nullable array)
                    }
                }

                // preferred_read_replica only in v11+
                if api_version >= 11 {
                    buf.put_i32(partition_response.preferred_read_replica);
                }

                #[cfg(debug_assertions)]
                {
                    if let Some(records) = &partition_response.records {
                        #[cfg(debug_assertions)]
                        tracing::debug!("Encoding partition {} records: {} bytes (first 32 bytes: {:?}) for API v{}",
                                      partition_response.partition, records.len(),
                                      &records[..std::cmp::min(32, records.len())], api_version);
                    } else {
                        #[cfg(debug_assertions)]
                        tracing::debug!("Encoding partition {} records: None for API v{}", partition_response.partition, api_version);
                    }
                }

                // Records (compact bytes in flexible versions)
                if flexible {
                    Self::encode_compact_nullable_bytes(&partition_response.records, buf);
                } else {
                    Self::encode_nullable_bytes(&partition_response.records, buf);
                }

                // Partition-level tagged fields (flexible versions)
                if flexible {
                    Self::encode_varint(buf, 0); // No tagged fields
                }
            }

            // Topic-level tagged fields (flexible versions)
            if flexible {
                Self::encode_varint(buf, 0); // No tagged fields
            }
        }

        // Response-level tagged fields (flexible versions)
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields
        }

        Ok(())
    }

    // ========================================================================
    // LIST OFFSETS REQUEST/RESPONSE
    // ========================================================================

    fn decode_list_offsets_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaListOffsetsRequest> {
        // ListOffsets v6+ uses flexible encoding (v4, v5 are NOT flexible)
        let is_flexible = header.api_version >= 6;

        let replica_id = cursor.get_i32();
        let isolation_level = cursor.get_i8();

        // Topic count: compact array (varint) for flexible, i32 for non-flexible
        let topic_count = if is_flexible {
            let varint = Self::decode_varint(cursor)? as i32;
            if varint == 0 { 0 } else { varint - 1 }
        } else {
            cursor.get_i32()
        };

        let mut topics = Vec::with_capacity(topic_count as usize);
        for _ in 0..topic_count {
            // Topic name: compact string for flexible, regular string for non-flexible
            let topic = if is_flexible {
                Self::decode_compact_non_nullable_string(cursor)?
            } else {
                Self::decode_string(cursor)?
            };

            // Partition count: compact array for flexible, i32 for non-flexible
            let partition_count = if is_flexible {
                let varint = Self::decode_varint(cursor)? as i32;
                if varint == 0 { 0 } else { varint - 1 }
            } else {
                cursor.get_i32()
            };

            let mut partitions = Vec::with_capacity(partition_count as usize);
            for _ in 0..partition_count {
                let partition = cursor.get_i32();
                let current_leader_epoch = cursor.get_i32();
                let timestamp = cursor.get_i64();
                partitions.push(KafkaListOffsetsPartition {
                    partition,
                    current_leader_epoch,
                    timestamp,
                });
            }

            // Skip tagged fields for flexible versions
            if is_flexible {
                Self::skip_tagged_fields(cursor)?;
            }

            topics.push(KafkaListOffsetsTopic { topic, partitions });
        }

        // Skip tagged fields at the end for flexible versions
        if is_flexible {
            Self::skip_tagged_fields(cursor)?;
        }

        Ok(KafkaListOffsetsRequest {
            header,
            replica_id,
            isolation_level,
            topics,
        })
    }

    // ========================================================================
    // METADATA REQUEST/RESPONSE
    // ========================================================================

    fn decode_metadata_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaMetadataRequest> {
        #[cfg(debug_assertions)]
        debug!("Decoding Metadata request: api_version={}", header.api_version);
        #[cfg(debug_assertions)]
        debug!("  Cursor position: {}, remaining: {}", cursor.position(), cursor.remaining());
        
        // Debug: Show raw bytes around current cursor position
        let start_pos = cursor.position() as usize;
        let slice = cursor.get_ref();
        let debug_bytes = &slice[start_pos..std::cmp::min(start_pos + 20, slice.len())];
        #[cfg(debug_assertions)]
        debug!("  Raw bytes at cursor: {:02x?}", debug_bytes);
        
        // CRITICAL FIX: Handle flexible versions (v9+) vs non-flexible versions
        // CRITICAL FIX: Request parsing must match Java client's flexible format
        let is_flexible = header.api_version >= 9; // Java client sends flexible v9 requests
        #[cfg(debug_assertions)]
        debug!("  Using flexible format: {}", is_flexible);
        
        let topic_count = if is_flexible {
            // Flexible versions use compact arrays (varint length - 1)
            let varint_value = Self::decode_varint(cursor)? as i32;
            if varint_value == 0 {
                -1 // null array indicator
            } else {
                varint_value - 1 // compact arrays subtract 1 from length
            }
        } else {
            // Non-flexible versions use standard int32 array length
            cursor.get_i32()
        };
        
        #[cfg(debug_assertions)]
        debug!("  Topic count read: {} (flexible={})", topic_count, is_flexible);
        #[cfg(debug_assertions)]
        debug!("  Cursor after topic_count: pos={}, remaining={}", cursor.position(), cursor.remaining());
        let topics = if topic_count == -1 {
            #[cfg(debug_assertions)]
            debug!("  Topics: null (topic_count = -1)");
            None
        } else if topic_count == 0 {
            #[cfg(debug_assertions)]
            debug!("  Topics: empty array (topic_count = 0)");
            Some(Vec::new())
        } else {
            #[cfg(debug_assertions)]
            debug!("  Parsing {} topics...", topic_count);
            let mut topics = Vec::with_capacity(topic_count as usize);
            for i in 0..topic_count {
                // Handle topic parsing for flexible vs non-flexible versions
                if is_flexible {
                    // In flexible versions, topics are MetadataRequestTopic objects
                    // Each topic has: topicId (uuid, v10+), name (compact string)
                    if header.api_version >= 10 {
                        // Skip topicId (16 bytes UUID) - not implemented yet
                        cursor.set_position(cursor.position() + 16);
                    }
                    
                    // Read topic name as compact string
                    let topic_name = Self::decode_compact_string(cursor)?
                        .unwrap_or_else(|| "".to_string());
                    #[cfg(debug_assertions)]
                    debug!("    Topic {} (flexible): '{}'", i, topic_name);
                    topics.push(topic_name);
                    
                    // Skip tagged fields for this topic (just read the count)
                    let _tagged_fields_count = Self::decode_varint(cursor)?;
                } else {
                    // Non-flexible versions just have topic name as regular string
                    let topic_name = Self::decode_string(cursor)?;
                    #[cfg(debug_assertions)]
                    debug!("    Topic {} (non-flexible): '{}'", i, topic_name);
                    topics.push(topic_name);
                }
            }
            #[cfg(debug_assertions)]
            debug!("  Final topics parsed: {:?}", topics);
            Some(topics)
        };

        // Parse remaining fields based on version and format
        let allow_auto_topic_creation = if header.api_version >= 4 && cursor.remaining() > 0 {
            if is_flexible {
                cursor.get_u8() != 0  // In flexible versions, bool is u8
            } else {
                cursor.get_i8() != 0  // In non-flexible versions, bool is i8
            }
        } else {
            true // Default value for older versions
        };

        let include_cluster_authorized_operations =
            if header.api_version >= 8 && header.api_version <= 10 && cursor.remaining() > 0 {
                if is_flexible {
                    cursor.get_u8() != 0
                } else {
                    cursor.get_i8() != 0
                }
            } else {
                false // Default value for older versions or v11+
            };

        let include_topic_authorized_operations =
            if header.api_version >= 8 && cursor.remaining() > 0 {
                if is_flexible {
                    cursor.get_u8() != 0
                } else {
                    cursor.get_i8() != 0
                }
            } else {
                false // Default value for older versions
            };

        // For flexible versions, consume top-level tagged fields
        if is_flexible && cursor.remaining() > 0 {
            let _tagged_fields_count = Self::decode_varint(cursor)?;
            #[cfg(debug_assertions)]
            debug!("  Skipped {} top-level tagged fields", _tagged_fields_count);
        }

        Ok(KafkaMetadataRequest {
            header,
            topics,
            allow_auto_topic_creation,
            include_cluster_authorized_operations,
            include_topic_authorized_operations,
        })
    }

    fn encode_metadata_response(
        response: &KafkaMetadataResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // CRITICAL TEST: Try flexible encoding for v9+ as per Kafka spec
        let is_flexible = response.api_version >= 9; // Metadata v9+ uses flexible versions
        #[cfg(debug_assertions)]
        debug!("🔧 MetadataResponse encoding: version={}, flexible={}", response.api_version, is_flexible);
        
        #[cfg(debug_assertions)]
        debug!("Encoding Metadata response v{}, flexible={}", response.api_version, is_flexible);
        #[cfg(debug_assertions)]
        debug!("  - correlation_id: {}", response.header.correlation_id);
        #[cfg(debug_assertions)]
        debug!("  - throttle_time_ms: {}", response.throttle_time_ms);
        #[cfg(debug_assertions)]
        debug!("  - brokers count: {}", response.brokers.len());
        #[cfg(debug_assertions)]
        debug!("  - topics count: {}", response.topics.len());
        
        // CRITICAL FIX: Java Kafka client expects correlation_id at the FIRST position after length prefix
        // All Kafka responses must start with correlation_id as the first field
        // This is consistent across all Kafka APIs including Metadata v0-v11
        
        #[cfg(debug_assertions)]
        debug!("🔍 BEFORE correlation_id encoding: buffer is empty, size={}", buf.len());
        buf.put_i32(response.header.correlation_id);
        #[cfg(debug_assertions)]
        debug!("✅ AFTER correlation_id encoding: buffer size={}, correlation_id={}", buf.len(), response.header.correlation_id);
        // DEBUG: Show exact hex bytes at start of response
        if buf.len() >= 4 {
            let first_4_bytes = &buf[0..4];
            #[cfg(debug_assertions)]
            debug!("  - CORRELATION_ID BYTES: [{:02x}, {:02x}, {:02x}, {:02x}] = {}",
                  first_4_bytes[0], first_4_bytes[1], first_4_bytes[2], first_4_bytes[3],
                  i32::from_be_bytes([first_4_bytes[0], first_4_bytes[1], first_4_bytes[2], first_4_bytes[3]]));
        }

        // Flexible versions: header tagged fields (v9+)
        if is_flexible {
            Self::encode_varint(buf, 0); // No tagged fields in header
        }

        // throttle_time_ms position depends on version
        // v0-v2: NO throttle_time_ms field
        // v3+: throttle_time_ms after correlation_id (2nd field)
        if response.api_version >= 3 {
            buf.put_i32(response.throttle_time_ms);
            #[cfg(debug_assertions)]
            debug!("  - Wrote throttle_time_ms after correlation_id (v3+), buffer size: {}", buf.len());
        } else {
            #[cfg(debug_assertions)]
            debug!("  - Skipped throttle_time_ms (v0-v2 don't have it), buffer size: {}", buf.len());
        }

        // Brokers array - CRITICAL for Java Kafka 4.1 compatibility
        #[cfg(debug_assertions)]
        debug!("  - CRITICAL: Encoding brokers array, count: {}", response.brokers.len());
        if response.brokers.is_empty() {
            error!("  - ERROR: brokers array is empty! This will cause Java client parsing failure");
        }
        // CRITICAL FIX: Kafka v9+ uses compact array encoding (unsigned varint)
        // Based on Kafka 4.1.0 source: _writable.writeUnsignedVarint(brokers.size() + 1)
        if is_flexible {
            Self::encode_varint(buf, (response.brokers.len() + 1) as u64);
            #[cfg(debug_assertions)]
            debug!("  - KAFKA v9+ FIX: Using compact array encoding for brokers, varint length: {}", response.brokers.len() + 1);
        } else {
            buf.put_i32(response.brokers.len() as i32);
            #[cfg(debug_assertions)]
            debug!("  - KAFKA v0-8: Using standard i32 array encoding for brokers, length: {}", response.brokers.len());
        }
        #[cfg(debug_assertions)]
        debug!("  - Wrote brokers array length, buffer size: {}", buf.len());

        for (i, broker) in response.brokers.iter().enumerate() {
            #[cfg(debug_assertions)]
            debug!("    broker[{}]: node_id={}, host='{}', port={}", i, broker.node_id, broker.host, broker.port);
            buf.put_i32(broker.node_id);
            if is_flexible {
                Self::encode_compact_string(&broker.host, buf);
            } else {
                Self::encode_string(&broker.host, buf);
            }
            buf.put_i32(broker.port);
            if is_flexible {
                Self::encode_compact_nullable_string(&broker.rack, buf);
            } else {
                Self::encode_nullable_string(&broker.rack, buf);
            }
            #[cfg(debug_assertions)]
            debug!("    broker[{}] encoded, buffer size: {}", i, buf.len());
            
            // Tagged fields for broker (v9+)
            if is_flexible {
                Self::encode_empty_tagged_fields(buf);
                #[cfg(debug_assertions)]
                debug!("    broker[{}] tagged fields encoded, buffer size: {}", i, buf.len());
            }
        }

        if is_flexible {
            Self::encode_compact_nullable_string(&response.cluster_id, buf);
        } else {
            Self::encode_nullable_string(&response.cluster_id, buf);
        }
        buf.put_i32(response.controller_id);

        // Topics array
        #[cfg(debug_assertions)]
        debug!("  - Encoding topics array, actual count: {}", response.topics.len());
        #[cfg(debug_assertions)]
        debug!("  - Topics in response: {:?}", response.topics.iter().map(|t| &t.topic).collect::<Vec<_>>());
        if is_flexible {
            // CRITICAL FIX: Use correct compact array encoding for topics  
            let array_len = response.topics.len();
            let encoded_len = array_len + 1;
            Self::encode_varint(buf, encoded_len as u64);
            #[cfg(debug_assertions)]
            debug!("  - FIXED topics compact array: length={}, encoded as varint {} (Java will read {})", 
                  array_len, encoded_len, array_len);
        } else {
            buf.put_i32(response.topics.len() as i32);
        }
        #[cfg(debug_assertions)]
        debug!("  - Wrote topics array length, buffer size: {}", buf.len());

        for (i, topic) in response.topics.iter().enumerate() {
            #[cfg(debug_assertions)]
            debug!("    topic[{}]: name='{}', partitions={}", i, topic.topic, topic.partitions.len());
            buf.put_i16(topic.error_code);
            if is_flexible {
                Self::encode_compact_string(&topic.topic, buf);
            } else {
                Self::encode_string(&topic.topic, buf);
            }
            buf.put_i8(if topic.is_internal { 1 } else { 0 });

            // Partitions array
            if is_flexible {
                // CRITICAL FIX: Partitions compact array - encode as length + 1 for ALL arrays
                let array_len = topic.partitions.len();
                let encoded_len = array_len + 1;
                Self::encode_varint(buf, encoded_len as u64);
                #[cfg(debug_assertions)]
                debug!("    FIXED partitions compact array: length={}, encoded as varint {} (Java will read {})", 
                       array_len, encoded_len, array_len);
            } else {
                buf.put_i32(topic.partitions.len() as i32);
            }

            for (partition_idx, partition) in topic.partitions.iter().enumerate() {
                #[cfg(debug_assertions)]
                debug!("      partition[{}]: id={}, leader={}", 
                       partition_idx, partition.partition, partition.leader);
                
                // DEFENSIVE: Validate partition data before encoding
                if partition.partition < 0 || partition.partition > 1000000 {
                    warn!("      partition[{}]: Invalid partition ID {}, using 0", 
                          partition_idx, partition.partition);
                    buf.put_i16(partition.error_code);
                    buf.put_i32(0); // Safe default partition
                    buf.put_i32(0); // Safe default leader
                } else {
                    buf.put_i16(partition.error_code);
                    buf.put_i32(partition.partition);
                    buf.put_i32(partition.leader);
                }
                
                // leader_epoch only for v7+ (added in version 7)
                if response.api_version >= 7 {
                    buf.put_i32(partition.leader_epoch);
                }

                // DEFENSIVE: Safe replica nodes array encoding
                let safe_replica_nodes = if partition.replica_nodes.len() > 10000 {
                    warn!("      partition[{}]: Too many replica nodes ({}), limiting to empty", 
                          partition_idx, partition.replica_nodes.len());
                    Vec::new()
                } else {
                    partition.replica_nodes.clone()
                };
                
                if is_flexible {
                    // CRITICAL FIX: Replica nodes compact array
                    let array_len = safe_replica_nodes.len();
                    let encoded_len = array_len + 1;
                    Self::encode_varint(buf, encoded_len as u64);
                    #[cfg(debug_assertions)]
                    debug!("      partition[{}]: replica_nodes compact array len={}", 
                           partition_idx, array_len);
                } else {
                    buf.put_i32(safe_replica_nodes.len() as i32);
                }
                for (replica_idx, replica) in safe_replica_nodes.iter().enumerate() {
                    if *replica < 0 || *replica > 1000000 {
                        warn!("      partition[{}]: Invalid replica node {}, using 0", 
                              partition_idx, replica);
                        buf.put_i32(0);
                    } else {
                        buf.put_i32(*replica);
                    }
                    if replica_idx >= 100 { // Safety limit
                        warn!("      partition[{}]: Too many replica nodes, truncating at 100", partition_idx);
                        break;
                    }
                }

                // DEFENSIVE: Safe ISR nodes array encoding
                let safe_isr_nodes = if partition.isr_nodes.len() > 10000 {
                    warn!("      partition[{}]: Too many ISR nodes ({}), limiting to empty", 
                          partition_idx, partition.isr_nodes.len());
                    Vec::new()
                } else {
                    partition.isr_nodes.clone()
                };
                
                if is_flexible {
                    // CRITICAL FIX: ISR nodes compact array
                    let array_len = safe_isr_nodes.len();
                    let encoded_len = array_len + 1;
                    Self::encode_varint(buf, encoded_len as u64);
                    #[cfg(debug_assertions)]
                    debug!("      partition[{}]: isr_nodes compact array len={}", 
                           partition_idx, array_len);
                } else {
                    buf.put_i32(safe_isr_nodes.len() as i32);
                }
                for (isr_idx, isr) in safe_isr_nodes.iter().enumerate() {
                    if *isr < 0 || *isr > 1000000 {
                        warn!("      partition[{}]: Invalid ISR node {}, using 0", 
                              partition_idx, isr);
                        buf.put_i32(0);
                    } else {
                        buf.put_i32(*isr);
                    }
                    if isr_idx >= 100 { // Safety limit
                        warn!("      partition[{}]: Too many ISR nodes, truncating at 100", partition_idx);
                        break;
                    }
                }

                // DEFENSIVE: Safe offline replicas array (v5+)
                if response.api_version >= 5 {
                    let safe_offline_replicas = if partition.offline_replicas.len() > 10000 {
                        warn!("      partition[{}]: Too many offline replicas ({}), limiting to empty", 
                              partition_idx, partition.offline_replicas.len());
                        Vec::new()
                    } else {
                        partition.offline_replicas.clone()
                    };
                    
                    if is_flexible {
                        // CRITICAL FIX: Offline replicas compact array
                        let array_len = safe_offline_replicas.len();
                        let encoded_len = array_len + 1;
                        Self::encode_varint(buf, encoded_len as u64);
                        #[cfg(debug_assertions)]
                        debug!("      partition[{}]: offline_replicas compact array len={}", 
                               partition_idx, array_len);
                    } else {
                        buf.put_i32(safe_offline_replicas.len() as i32);
                    }
                    for (offline_idx, offline) in safe_offline_replicas.iter().enumerate() {
                        if *offline < 0 || *offline > 1000000 {
                            warn!("      partition[{}]: Invalid offline replica {}, using 0", 
                                  partition_idx, offline);
                            buf.put_i32(0);
                        } else {
                            buf.put_i32(*offline);
                        }
                        if offline_idx >= 100 { // Safety limit
                            warn!("      partition[{}]: Too many offline replicas, truncating at 100", partition_idx);
                            break;
                        }
                    }
                }

                // Tagged fields for partition (v9+)
                if is_flexible {
                    Self::encode_empty_tagged_fields(buf);
                }
                #[cfg(debug_assertions)]
                debug!("      partition[{}]: encoding complete", partition_idx);
            }

            // Topic authorized operations (v8+)
            if response.api_version >= 8 {
                buf.put_i32(topic.topic_authorized_operations);
            }
            
            // Tagged fields for topic (v9+)
            if is_flexible {
                Self::encode_empty_tagged_fields(buf);
            }
        }

        // Cluster authorized operations (v8+)
        if response.api_version >= 8 {
            buf.put_i32(response.cluster_authorized_operations);
        }
        
        // Tagged fields for response (v9+)
        if is_flexible {
            Self::encode_empty_tagged_fields(buf);
            #[cfg(debug_assertions)]
            debug!("  - Added response-level tagged fields, final buffer size: {}", buf.len());
        }
        
        // v2 does NOT have throttle_time_ms - removing incorrect implementation
        
        #[cfg(debug_assertions)]
        debug!("Completed Metadata response encoding, total bytes: {}", buf.len());
        
        // DEBUG: Log first 100 bytes of response for Java client debugging
        if buf.len() >= 10 {
            let preview: Vec<u8> = buf[0..std::cmp::min(100, buf.len())].to_vec();
            #[cfg(debug_assertions)]
            debug!("Response bytes (first 100): {:?}", preview);
        }
        
        // CRITICAL DEBUG: Show exact structure for Java client correlation_id analysis
        if buf.len() >= 20 {
            let first_20_bytes = &buf[0..20];
            #[cfg(debug_assertions)]
            debug!("  - FULL METADATA STRUCTURE (20 bytes):");
            #[cfg(debug_assertions)]
            debug!("    Bytes 00-03: [{:02x} {:02x} {:02x} {:02x}] = correlation_id: {}", 
                  first_20_bytes[0], first_20_bytes[1], first_20_bytes[2], first_20_bytes[3],
                  i32::from_be_bytes([first_20_bytes[0], first_20_bytes[1], first_20_bytes[2], first_20_bytes[3]]));
            #[cfg(debug_assertions)]
            debug!("    Bytes 04-07: [{:02x} {:02x} {:02x} {:02x}] = throttle_time: {}", 
                  first_20_bytes[4], first_20_bytes[5], first_20_bytes[6], first_20_bytes[7],
                  i32::from_be_bytes([first_20_bytes[4], first_20_bytes[5], first_20_bytes[6], first_20_bytes[7]]));
            #[cfg(debug_assertions)]
            debug!("    Byte 08: [{:02x}] = broker_count_varint: {}", first_20_bytes[8], first_20_bytes[8]);
            #[cfg(debug_assertions)]
            debug!("    Bytes 09-12: [{:02x} {:02x} {:02x} {:02x}] = broker_node_id: {}", 
                  first_20_bytes[9], first_20_bytes[10], first_20_bytes[11], first_20_bytes[12],
                  i32::from_be_bytes([first_20_bytes[9], first_20_bytes[10], first_20_bytes[11], first_20_bytes[12]]));
            #[cfg(debug_assertions)]
            debug!("    Byte 13: [{:02x}] = host_length_varint: {}", first_20_bytes[13], first_20_bytes[13]);
            #[cfg(debug_assertions)]
            debug!("    Bytes 14-19: [{:02x} {:02x} {:02x} {:02x} {:02x} {:02x}] = host_prefix: '{}'", 
                  first_20_bytes[14], first_20_bytes[15], first_20_bytes[16], first_20_bytes[17], first_20_bytes[18], first_20_bytes[19],
                  String::from_utf8_lossy(&first_20_bytes[14..20]));
            
            // CRITICAL: Try to understand where Java reads correlation_id=680 (0x02A8)
            #[cfg(debug_assertions)]
            debug!("  - CHECKING FOR 680 (0x02A8) IN RESPONSE:");
            for i in 0..=(buf.len().saturating_sub(4)) {
                if i + 3 < buf.len() {
                    let value = i32::from_be_bytes([buf[i], buf[i+1], buf[i+2], buf[i+3]]);
                    if value == 680 {
                        #[cfg(debug_assertions)]
                        debug!("    Found 680 at byte offset {}: [{:02x} {:02x} {:02x} {:02x}]", i, buf[i], buf[i+1], buf[i+2], buf[i+3]);
                    }
                }
            }
        }
        
        Ok(())
    }

    // ========================================================================
    // LIST OFFSETS REQUEST/RESPONSE
    // ========================================================================

    fn encode_list_offsets_response(
        response: &KafkaListOffsetsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let api_version = response.api_version;
        // ListOffsets API: flexible versions start at v6 (not v4)
        let flexible = api_version >= 6;

        buf.put_i32(response.header.correlation_id);

        // Flexible versions: header tagged fields
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields in header
        }

        buf.put_i32(response.throttle_time_ms);

        // Topics array
        if flexible {
            Self::encode_varint(buf, (response.topics.len() + 1) as u64);
        } else {
            buf.put_i32(response.topics.len() as i32);
        }

        for topic in &response.topics {
            // Topic name
            if flexible {
                Self::encode_compact_string(&topic.topic, buf);
            } else {
                Self::encode_string(&topic.topic, buf);
            }

            // Partitions array
            if flexible {
                Self::encode_varint(buf, (topic.partitions.len() + 1) as u64);
            } else {
                buf.put_i32(topic.partitions.len() as i32);
            }

            for partition in &topic.partitions {
                buf.put_i32(partition.partition);
                buf.put_i16(partition.error_code);
                buf.put_i64(partition.timestamp);
                buf.put_i64(partition.offset);
                buf.put_i32(partition.leader_epoch);

                // Partition-level tagged fields (flexible versions)
                if flexible {
                    Self::encode_varint(buf, 0); // No tagged fields
                }
            }

            // Topic-level tagged fields (flexible versions)
            if flexible {
                Self::encode_varint(buf, 0); // No tagged fields
            }
        }

        // Response-level tagged fields (flexible versions)
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields
        }

        Ok(())
    }

    // ========================================================================
    // OFFSET COMMIT REQUEST/RESPONSE
    // ========================================================================

    /// Decode OffsetCommit Request
    /// Versions:
    ///   v0: basic (group_id, topics with partition/offset/metadata)
    ///   v1: adds generation_id, member_id, timestamp in partitions
    ///   v2-v4: adds retention_time_ms, removes timestamp from partitions
    ///   v5: removes retention_time_ms (server-controlled)
    ///   v6: adds committed_leader_epoch in partitions
    ///   v7: adds group_instance_id
    ///   v8+: flexible versions (compact encoding)
    fn decode_offset_commit_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaOffsetCommitRequest> {
        let api_version = header.api_version;
        let is_flexible = api_version >= 8;

        // Decode group_id
        let group_id = if is_flexible {
            Self::decode_compact_non_nullable_string(cursor)?
        } else {
            Self::decode_string(cursor)?
        };

        // v1+ has generation_id and member_id
        let generation_id = if api_version >= 1 {
            cursor.get_i32()
        } else {
            -1
        };

        let consumer_id = if api_version >= 1 {
            if is_flexible {
                Self::decode_compact_non_nullable_string(cursor)?
            } else {
                Self::decode_string(cursor)?
            }
        } else {
            String::new()
        };

        // v7+ has group_instance_id (nullable)
        let group_instance_id = if api_version >= 7 {
            if is_flexible {
                Self::decode_compact_string(cursor)?
            } else {
                Self::decode_nullable_string(cursor)?
            }
        } else {
            None
        };

        // v2-v4 has retention_time_ms (removed in v5+)
        let retention_time_ms = if api_version >= 2 && api_version <= 4 {
            cursor.get_i64()
        } else {
            -1
        };

        // Decode topics array
        let topic_count = if is_flexible {
            match Self::decode_compact_array_len(cursor)? {
                Some(n) => n,
                None => 0,
            }
        } else {
            cursor.get_i32() as usize
        };

        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            // Decode topic name
            let topic = if is_flexible {
                Self::decode_compact_non_nullable_string(cursor)?
            } else {
                Self::decode_string(cursor)?
            };

            // Decode partitions array
            let partition_count = if is_flexible {
                match Self::decode_compact_array_len(cursor)? {
                    Some(n) => n,
                    None => 0,
                }
            } else {
                cursor.get_i32() as usize
            };

            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let partition = cursor.get_i32();
                let offset = cursor.get_i64();

                // v6+ has committed_leader_epoch
                let committed_leader_epoch = if api_version >= 6 {
                    cursor.get_i32()
                } else {
                    -1
                };

                // v1 only has timestamp field in partitions
                // v0 and v2+ do NOT have timestamp field
                let timestamp = if api_version == 1 {
                    cursor.get_i64()
                } else {
                    -1
                };

                // Decode metadata (nullable string)
                let metadata = if is_flexible {
                    Self::decode_compact_string(cursor)?
                } else {
                    Self::decode_nullable_string(cursor)?
                };

                // Skip partition-level tagged fields for flexible versions
                if is_flexible {
                    Self::skip_tagged_fields(cursor)?;
                }

                partitions.push(KafkaOffsetCommitPartition {
                    partition,
                    offset,
                    committed_leader_epoch,
                    timestamp,
                    metadata,
                });
            }

            // Skip topic-level tagged fields for flexible versions
            if is_flexible {
                Self::skip_tagged_fields(cursor)?;
            }

            topics.push(KafkaOffsetCommitTopic { topic, partitions });
        }

        // Skip request-level tagged fields for flexible versions
        if is_flexible {
            Self::skip_tagged_fields(cursor)?;
        }

        Ok(KafkaOffsetCommitRequest {
            header,
            group_id,
            generation_id,
            consumer_id,
            group_instance_id,
            retention_time_ms,
            topics,
        })
    }

    fn encode_offset_commit_response(
        response: &KafkaOffsetCommitResponse,
        api_version: i16,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let flexible = api_version >= 8;

        buf.put_i32(response.header.correlation_id);

        // Response header tagged fields for flexible versions
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields in header
        }

        // throttle_time_ms (v3+)
        if api_version >= 3 {
            buf.put_i32(response.throttle_time_ms);
        }

        // Topics array
        if flexible {
            // Compact array: length + 1 as varint
            Self::encode_varint(buf, (response.topics.len() + 1) as u64);
        } else {
            buf.put_i32(response.topics.len() as i32);
        }

        for topic in &response.topics {
            // Topic name
            if flexible {
                Self::encode_compact_string(&topic.topic, buf);
            } else {
                Self::encode_string(&topic.topic, buf);
            }

            // Partitions array
            if flexible {
                Self::encode_varint(buf, (topic.partitions.len() + 1) as u64);
            } else {
                buf.put_i32(topic.partitions.len() as i32);
            }

            for partition in &topic.partitions {
                buf.put_i32(partition.partition);
                buf.put_i16(partition.error_code);

                // Partition-level tagged fields for flexible versions
                if flexible {
                    Self::encode_varint(buf, 0); // No tagged fields
                }
            }

            // Topic-level tagged fields for flexible versions
            if flexible {
                Self::encode_varint(buf, 0); // No tagged fields
            }
        }

        // Response-level tagged fields for flexible versions
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields
        }

        Ok(())
    }

    // ========================================================================
    // OFFSET FETCH REQUEST/RESPONSE
    // ========================================================================

    fn decode_offset_fetch_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaOffsetFetchRequest> {
        let group_id = Self::decode_string(cursor)?;
        let topic_count = cursor.get_i32();

        let topics = if topic_count == -1 {
            None // Fetch all topics
        } else {
            let mut topics = Vec::with_capacity(topic_count as usize);

            for _ in 0..topic_count {
                let topic = Self::decode_string(cursor)?;
                let partition_count = cursor.get_i32();
                let mut partitions = Vec::with_capacity(partition_count as usize);

                for _ in 0..partition_count {
                    partitions.push(cursor.get_i32());
                }

                topics.push(KafkaOffsetFetchTopic { topic, partitions });
            }

            Some(topics)
        };

        Ok(KafkaOffsetFetchRequest {
            header,
            group_id,
            topics,
        })
    }

    fn encode_offset_fetch_response(
        response: &KafkaOffsetFetchResponse,
        api_version: i16,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // OffsetFetch becomes flexible at v6
        let flexible = api_version >= 6;

        buf.put_i32(response.header.correlation_id);

        // Response header tagged fields for flexible versions
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields in header
        }

        // throttle_time_ms (v3+)
        if api_version >= 3 {
            buf.put_i32(response.throttle_time_ms);
        }

        // Topics array
        if flexible {
            // Compact array: length + 1 as varint
            Self::encode_varint(buf, (response.topics.len() + 1) as u64);
        } else {
            buf.put_i32(response.topics.len() as i32);
        }

        for topic in &response.topics {
            // Topic name
            if flexible {
                Self::encode_compact_string(&topic.topic, buf);
            } else {
                Self::encode_string(&topic.topic, buf);
            }

            // Partitions array
            if flexible {
                Self::encode_varint(buf, (topic.partitions.len() + 1) as u64);
            } else {
                buf.put_i32(topic.partitions.len() as i32);
            }

            for partition in &topic.partitions {
                buf.put_i32(partition.partition);
                buf.put_i64(partition.offset);

                // leader_epoch (v5+)
                if api_version >= 5 {
                    buf.put_i32(partition.leader_epoch);
                }

                // metadata
                if flexible {
                    Self::encode_compact_nullable_string(&partition.metadata, buf);
                } else {
                    Self::encode_nullable_string(&partition.metadata, buf);
                }

                buf.put_i16(partition.error_code);

                // Partition-level tagged fields for flexible versions
                if flexible {
                    Self::encode_varint(buf, 0); // No tagged fields
                }
            }

            // Topic-level tagged fields for flexible versions
            if flexible {
                Self::encode_varint(buf, 0); // No tagged fields
            }
        }

        // error_code (v2+)
        if api_version >= 2 {
            buf.put_i16(response.error_code);
        }

        // Response-level tagged fields for flexible versions
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields
        }

        Ok(())
    }

    // ========================================================================
    // FIND COORDINATOR REQUEST/RESPONSE
    // ========================================================================

    fn decode_find_coordinator_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaFindCoordinatorRequest> {
        let coordinator_key = Self::decode_string(cursor)?;
        let coordinator_type = if header.api_version >= 1 && cursor.remaining() > 0 {
            cursor.get_i8()
        } else {
            0 // Default to GROUP coordinator for v0
        };

        Ok(KafkaFindCoordinatorRequest {
            header,
            coordinator_key,
            coordinator_type,
        })
    }

    fn encode_find_coordinator_response(
        response: &KafkaFindCoordinatorResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // FindCoordinatorResponse has different formats per version:
        // v0: error_code, node_id, host, port (NO throttle_time_ms, NO error_message!)
        // v1-v2: throttle_time_ms, error_code, error_message, node_id, host, port
        // v3+: flexible version (compact strings, tagged fields)
        let version = response.api_version;
        let is_flexible = version >= 3;

        // Response header
        buf.put_i32(response.header.correlation_id);

        if is_flexible {
            // Flexible version: tagged fields after header
            buf.put_u8(0); // No tagged fields in header
        }

        // v1+ includes throttle_time_ms
        if version >= 1 {
            buf.put_i32(response.throttle_time_ms);
        }

        // error_code - all versions
        buf.put_i16(response.error_code);

        // v1+ includes error_message
        if version >= 1 {
            if is_flexible {
                Self::encode_compact_nullable_string(&response.error_message, buf);
            } else {
                Self::encode_nullable_string(&response.error_message, buf);
            }
        }

        // node_id - all versions
        buf.put_i32(response.node_id);

        // host - all versions
        if is_flexible {
            Self::encode_compact_string(&response.host, buf);
        } else {
            Self::encode_string(&response.host, buf);
        }

        // port - all versions
        buf.put_i32(response.port);

        if is_flexible {
            // Tagged fields at end
            buf.put_u8(0); // No tagged fields
        }

        Ok(())
    }

    // ========================================================================
    // LIST GROUPS REQUEST/RESPONSE
    // ========================================================================

    fn decode_list_groups_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaListGroupsRequest> {
        let api_version = header.api_version;
        let is_flexible = api_version >= 3;

        let states_filter = if is_flexible {
            // Flexible version (v3+): compact array and compact strings
            let states_filter_count = match Self::decode_compact_array_len(cursor)? {
                Some(n) => n,
                None => 0,
            };
            let mut states_filter = Vec::with_capacity(states_filter_count.min(100));
            for _ in 0..states_filter_count {
                states_filter.push(Self::decode_compact_non_nullable_string(cursor)?);
            }
            // Skip request-level tagged fields
            Self::skip_tagged_fields(cursor)?;
            states_filter
        } else {
            // Non-flexible version (v0-v2)
            let states_filter_count = cursor.get_i32();
            let mut states_filter = Vec::with_capacity(states_filter_count as usize);
            for _ in 0..states_filter_count {
                states_filter.push(Self::decode_string(cursor)?);
            }
            states_filter
        };

        Ok(KafkaListGroupsRequest {
            header,
            states_filter,
        })
    }

    fn encode_list_groups_response(
        response: &KafkaListGroupsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let is_flexible = response.api_version >= 3;

        // Response header
        buf.put_i32(response.header.correlation_id);
        if is_flexible {
            buf.put_u8(0); // Header tagged fields
        }

        // Throttle time
        buf.put_i32(response.throttle_time_ms);

        // Error code
        buf.put_i16(response.error_code);

        // Groups array
        if is_flexible {
            Self::encode_compact_array_len(response.groups.len(), buf);
            for group in &response.groups {
                Self::encode_compact_string(&group.group_id, buf);
                Self::encode_compact_string(&group.protocol_type, buf);
                Self::encode_compact_string(&group.group_state, buf);
                // Per-element tagged fields
                buf.put_u8(0);
            }
            // Response-level tagged fields
            buf.put_u8(0);
        } else {
            buf.put_i32(response.groups.len() as i32);
            for group in &response.groups {
                Self::encode_string(&group.group_id, buf);
                Self::encode_string(&group.protocol_type, buf);
                Self::encode_string(&group.group_state, buf);
            }
        }

        Ok(())
    }

    // ========================================================================
    // HEARTBEAT REQUEST/RESPONSE
    // ========================================================================

    fn decode_heartbeat_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaHeartbeatRequest> {
        let group_id = Self::decode_string(cursor)?;
        let generation_id = cursor.get_i32();
        let consumer_id = Self::decode_string(cursor)?;

        // group_instance_id is only present in API version 4 and above (KIP-345)
        let group_instance_id = if header.api_version >= 4 {
            Self::decode_nullable_string(cursor)?
        } else {
            None
        };

        Ok(KafkaHeartbeatRequest {
            header,
            group_id,
            generation_id,
            consumer_id,
            group_instance_id,
        })
    }

    fn encode_heartbeat_response(
        response: &KafkaHeartbeatResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let api_version = response.api_version;
        // Heartbeat becomes flexible at v4
        let flexible = api_version >= 4;

        buf.put_i32(response.header.correlation_id);

        // Response header tagged fields for flexible versions
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields in header
        }

        buf.put_i32(response.throttle_time_ms);
        buf.put_i16(response.error_code);

        // Response-level tagged fields for flexible versions
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields
        }

        Ok(())
    }

    // ========================================================================
    // LEAVE GROUP REQUEST/RESPONSE
    // ========================================================================

    fn decode_leave_group_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaLeaveGroupRequest> {
        let group_id = Self::decode_string(cursor)?;
        let consumer_id = Self::decode_string(cursor)?;

        // group_instance_id is only present in API version 4 and above (KIP-345)
        let group_instance_id = if header.api_version >= 4 {
            Self::decode_nullable_string(cursor)?
        } else {
            None
        };

        Ok(KafkaLeaveGroupRequest {
            header,
            group_id,
            consumer_id,
            group_instance_id,
        })
    }

    fn encode_leave_group_response(
        response: &KafkaLeaveGroupResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let api_version = response.api_version;
        // LeaveGroup becomes flexible at v4
        let flexible = api_version >= 4;

        buf.put_i32(response.header.correlation_id);

        // Response header tagged fields for flexible versions
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields in header
        }

        buf.put_i32(response.throttle_time_ms);
        buf.put_i16(response.error_code);

        // v3+ adds members array with member responses
        // For now, we send empty members array for v3+
        if api_version >= 3 {
            if flexible {
                // Compact array: length + 1
                Self::encode_varint(buf, 1); // Empty array: 0 + 1 = 1
            } else {
                buf.put_i32(0); // Standard array length
            }
        }

        // Response-level tagged fields for flexible versions
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields
        }

        Ok(())
    }

    // ========================================================================
    // SYNC GROUP REQUEST/RESPONSE
    // ========================================================================

    fn decode_sync_group_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaSyncGroupRequest> {
        let group_id = Self::decode_string(cursor)?;

        // Defensive: check remaining bytes before reading i32
        let remaining = cursor.get_ref().len() - cursor.position() as usize;
        if remaining < 4 {
            return Err(KafkaCodecError::InvalidFormat(format!(
                "SyncGroup: insufficient bytes for generation_id: {} remaining",
                remaining
            )));
        }
        let generation_id = cursor.get_i32();
        let consumer_id = Self::decode_string(cursor)?;

        // group_instance_id is only present in API version 3 and above
        let group_instance_id = if header.api_version >= 3 {
            Self::decode_nullable_string(cursor)?
        } else {
            None
        };

        // protocol_type and protocol_name were added in version 5
        // v0-v2: no group_instance_id, no protocol_type/name
        // v3-v4: group_instance_id added
        // v5+: protocol_type and protocol_name added
        let protocol_type = if header.api_version >= 5 {
            Self::decode_string(cursor)?
        } else {
            String::new()
        };

        let protocol_name = if header.api_version >= 5 {
            Self::decode_string(cursor)?
        } else {
            String::new()
        };

        // Defensive: check remaining bytes before reading assignment_count
        let remaining = cursor.get_ref().len() - cursor.position() as usize;
        if remaining < 4 {
            // No assignments - this is valid for a follower consumer
            return Ok(KafkaSyncGroupRequest {
                header,
                group_id,
                generation_id,
                consumer_id,
                group_instance_id,
                protocol_type,
                protocol_name,
                assignments: Vec::new(),
            });
        }

        let assignment_count = cursor.get_i32();
        let mut assignments = Vec::with_capacity(assignment_count.max(0) as usize);

        for _ in 0..assignment_count.max(0) {
            let consumer_id = Self::decode_string(cursor)?;
            let assignment = Self::decode_bytes(cursor)?;
            assignments.push(KafkaSyncGroupAssignment {
                consumer_id,
                assignment,
            });
        }

        Ok(KafkaSyncGroupRequest {
            header,
            group_id,
            generation_id,
            consumer_id,
            group_instance_id,
            protocol_type,
            protocol_name,
            assignments,
        })
    }

    fn encode_sync_group_response(
        response: &KafkaSyncGroupResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let api_version = response.api_version;
        // SyncGroup becomes flexible at v4
        let flexible = api_version >= 4;

        buf.put_i32(response.header.correlation_id);

        // Response header tagged fields for flexible versions
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields in header
        }

        // throttle_time_ms is only present in version 1 and above
        if api_version >= 1 {
            buf.put_i32(response.throttle_time_ms);
        }

        buf.put_i16(response.error_code);

        // protocol_type and protocol_name were added in version 5
        // v0: error_code, assignment
        // v1-v2: + throttle_time_ms
        // v3-v4: + group_instance_id (request only)
        // v5+: + protocol_type, protocol_name
        if api_version >= 5 {
            if flexible {
                Self::encode_compact_string(&response.protocol_type, buf);
                Self::encode_compact_string(&response.protocol_name, buf);
            } else {
                Self::encode_string(&response.protocol_type, buf);
                Self::encode_string(&response.protocol_name, buf);
            }
        }

        // Encode assignment bytes
        if flexible {
            Self::encode_compact_bytes(&response.assignment, buf);
        } else {
            Self::encode_bytes(&response.assignment, buf);
        }

        // Response-level tagged fields for flexible versions
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields
        }

        Ok(())
    }

    // ========================================================================
    // JOIN GROUP REQUEST/RESPONSE (Simplified)
    // ========================================================================

    fn decode_join_group_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaJoinGroupRequest> {
        let group_id = Self::decode_string(cursor)?;
        let session_timeout_ms = cursor.get_i32();

        // rebalance_timeout_ms is only present in API version 1 and above
        let rebalance_timeout_ms = if header.api_version >= 1 {
            cursor.get_i32()
        } else {
            session_timeout_ms // Use session timeout as rebalance timeout for v0
        };

        let member_id = Self::decode_string(cursor)?;

        // group_instance_id is only present in API version 5 and above
        let group_instance_id = if header.api_version >= 5 {
            Self::decode_nullable_string(cursor)?
        } else {
            None
        };

        let protocol_type = Self::decode_string(cursor)?;

        let protocol_count = cursor.get_i32();
        let mut protocols = Vec::with_capacity(protocol_count as usize);

        for _ in 0..protocol_count {
            let name = Self::decode_string(cursor)?;
            let metadata = Self::decode_bytes(cursor)?;
            protocols.push(KafkaJoinGroupProtocol { name, metadata });
        }

        Ok(KafkaJoinGroupRequest {
            header,
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            protocols,
        })
    }

    fn encode_join_group_response(
        response: &KafkaJoinGroupResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let api_version = response.api_version;
        // JoinGroup becomes flexible at v6
        let flexible = api_version >= 6;

        buf.put_i32(response.header.correlation_id);

        // Response header tagged fields for flexible versions
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields in header
        }

        // throttle_time_ms is only present in version 2 and above
        if api_version >= 2 {
            buf.put_i32(response.throttle_time_ms);
        }

        buf.put_i16(response.error_code);
        buf.put_i32(response.generation_id);

        // JoinGroup response field order for v0-v6:
        //   protocol_name, leader, member_id, members
        // For v7+: protocol_type is added before protocol_name
        if api_version >= 7 {
            if flexible {
                Self::encode_compact_nullable_string(&Some(response.protocol_type.clone()), buf);
            } else {
                Self::encode_string(&response.protocol_type, buf);
            }
        }

        // protocol_name (nullable in v7+)
        if api_version >= 7 {
            if flexible {
                Self::encode_compact_nullable_string(&Some(response.protocol_name.clone()), buf);
            } else {
                Self::encode_nullable_string(&Some(response.protocol_name.clone()), buf);
            }
        } else {
            if flexible {
                Self::encode_compact_string(&response.protocol_name, buf);
            } else {
                Self::encode_string(&response.protocol_name, buf);
            }
        }

        // leader
        if flexible {
            Self::encode_compact_string(&response.leader, buf);
        } else {
            Self::encode_string(&response.leader, buf);
        }

        // member_id
        if flexible {
            Self::encode_compact_string(&response.member_id, buf);
        } else {
            Self::encode_string(&response.member_id, buf);
        }

        // members array
        if flexible {
            Self::encode_varint(buf, (response.members.len() + 1) as u64);
        } else {
            buf.put_i32(response.members.len() as i32);
        }

        for member in &response.members {
            if flexible {
                Self::encode_compact_string(&member.member_id, buf);
            } else {
                Self::encode_string(&member.member_id, buf);
            }

            // group_instance_id is only present in version 5 and above
            if api_version >= 5 {
                if flexible {
                    Self::encode_compact_nullable_string(&member.group_instance_id, buf);
                } else {
                    Self::encode_nullable_string(&member.group_instance_id, buf);
                }
            }

            // metadata (bytes)
            if flexible {
                Self::encode_compact_bytes(&member.metadata, buf);
            } else {
                Self::encode_bytes(&member.metadata, buf);
            }

            // Member-level tagged fields for flexible versions
            if flexible {
                Self::encode_varint(buf, 0); // No tagged fields
            }
        }

        // Response-level tagged fields for flexible versions
        if flexible {
            Self::encode_varint(buf, 0); // No tagged fields
        }

        Ok(())
    }

    // ========================================================================
    // DESCRIBE GROUPS REQUEST/RESPONSE
    // ========================================================================

    fn decode_describe_groups_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaDescribeGroupsRequest> {
        let api_version = header.api_version;
        let is_flexible = api_version >= 5;

        let (groups, include_authorized_operations) = if is_flexible {
            // Flexible version (v5+): compact arrays and compact strings
            let group_count = match Self::decode_compact_array_len(cursor)? {
                Some(n) => n,
                None => 0,
            };
            let mut groups = Vec::with_capacity(group_count.min(1000));
            for _ in 0..group_count {
                groups.push(Self::decode_compact_non_nullable_string(cursor)?);
            }

            // include_authorized_operations (boolean)
            let include_authorized_operations = if cursor.remaining() > 0 {
                cursor.get_i8() != 0
            } else {
                false
            };

            // Skip request-level tagged fields
            Self::skip_tagged_fields(cursor)?;

            (groups, include_authorized_operations)
        } else {
            // Non-flexible version (v0-v4)
            let group_count = cursor.get_i32();
            let mut groups = Vec::with_capacity(group_count as usize);
            for _ in 0..group_count {
                groups.push(Self::decode_string(cursor)?);
            }
            let include_authorized_operations = if cursor.remaining() > 0 {
                cursor.get_i8() != 0
            } else {
                false
            };
            (groups, include_authorized_operations)
        };

        Ok(KafkaDescribeGroupsRequest {
            header,
            groups,
            include_authorized_operations,
        })
    }

    fn encode_describe_groups_response(
        response: &KafkaDescribeGroupsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let is_flexible = response.api_version >= 5;

        // Response header
        buf.put_i32(response.header.correlation_id);
        if is_flexible {
            buf.put_u8(0); // Header tagged fields
        }

        // Throttle time
        buf.put_i32(response.throttle_time_ms);

        // Groups array
        if is_flexible {
            Self::encode_compact_array_len(response.groups.len(), buf);
            for group in &response.groups {
                buf.put_i16(group.error_code);
                Self::encode_compact_string(&group.group_id, buf);
                Self::encode_compact_string(&group.group_state, buf);
                Self::encode_compact_string(&group.protocol_type, buf);
                Self::encode_compact_string(&group.protocol_data, buf);

                Self::encode_compact_array_len(group.members.len(), buf);
                for member in &group.members {
                    Self::encode_compact_string(&member.member_id, buf);
                    Self::encode_compact_nullable_string(&member.group_instance_id, buf);
                    Self::encode_compact_string(&member.client_id, buf);
                    Self::encode_compact_string(&member.client_host, buf);
                    Self::encode_compact_bytes(&member.member_metadata, buf);
                    Self::encode_compact_bytes(&member.member_assignment, buf);
                    // Member tagged fields
                    buf.put_u8(0);
                }

                buf.put_i32(group.authorized_operations);
                // Group tagged fields
                buf.put_u8(0);
            }
            // Response-level tagged fields
            buf.put_u8(0);
        } else {
            buf.put_i32(response.groups.len() as i32);
            for group in &response.groups {
                buf.put_i16(group.error_code);
                Self::encode_string(&group.group_id, buf);
                Self::encode_string(&group.group_state, buf);
                Self::encode_string(&group.protocol_type, buf);
                Self::encode_string(&group.protocol_data, buf);

                buf.put_i32(group.members.len() as i32);
                for member in &group.members {
                    Self::encode_string(&member.member_id, buf);
                    Self::encode_nullable_string(&member.group_instance_id, buf);
                    Self::encode_string(&member.client_id, buf);
                    Self::encode_string(&member.client_host, buf);
                    Self::encode_bytes(&member.member_metadata, buf);
                    Self::encode_bytes(&member.member_assignment, buf);
                }

                buf.put_i32(group.authorized_operations);
            }
        }

        Ok(())
    }

    // ========================================================================
    // API VERSIONS REQUEST/RESPONSE
    // ========================================================================

    fn decode_api_versions_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaApiVersionsRequest> {
        #[cfg(debug_assertions)]
        debug!(
            "Decoding ApiVersions request: api_version={}, remaining_bytes={}, correlation_id={}",
            header.api_version,
            cursor.remaining(),
            header.correlation_id
        );

        // Version-aware decoding for ApiVersions  
        let (client_software_name, client_software_version) = if header.api_version >= 3 {
            // v3+: Use KIP-482 flexible versions with compact strings
            #[cfg(debug_assertions)]
            debug!("Decoding ApiVersions v3+ with KIP-482 flexible versions");
            #[cfg(debug_assertions)]
            debug!("Remaining bytes before field parsing: {}", cursor.remaining());
            
            // Show hex bytes for debugging
            let remaining_bytes = cursor.remaining();
            if remaining_bytes > 0 {
                let pos = cursor.position() as usize;
                let slice = cursor.get_ref();
                let hex_bytes = &slice[pos..pos+std::cmp::min(remaining_bytes, 32)];
                #[cfg(debug_assertions)]
                debug!("Next {} hex bytes: {}", hex_bytes.len(), hex_bytes.iter().map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(""));
            }
            
            // FIXED: Use proper varint decoding for Java 4.1 compatibility
            // Java 4.1 uses: readUnsignedVarint() - 1 for compact string lengths
            
            // First compact string: client_software_name
            let name = if cursor.remaining() > 0 {
                match Self::decode_varint(cursor) {
                    Ok(varint_len) => {
                        #[cfg(debug_assertions)]
                        debug!("First varint length: {}", varint_len);
                        if varint_len == 0 {
                            #[cfg(debug_assertions)]
                            debug!("Null string for client_software_name");
                            None
                        } else {
                            let actual_len = (varint_len - 1) as usize; // Java: readUnsignedVarint() - 1
                            if cursor.remaining() >= actual_len {
                                let mut buf = vec![0u8; actual_len];
                                cursor.copy_to_slice(&mut buf);
                                match String::from_utf8(buf) {
                                    Ok(name_str) => {
                                        #[cfg(debug_assertions)]
                                        debug!("Decoded client_software_name (len={}): {:?}", actual_len, name_str);
                                        Some(name_str)
                                    }
                                    Err(e) => {
                                        warn!("Invalid UTF-8 in client_software_name: {}", e);
                                        None
                                    }
                                }
                            } else {
                                warn!("Invalid string length: {} (remaining: {})", actual_len, cursor.remaining());
                                None
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to decode varint for client_software_name: {}", e);
                        None
                    }
                }
            } else {
                None
            };
            
            // Second compact string: client_software_version  
            let version = if cursor.remaining() > 0 {
                match Self::decode_varint(cursor) {
                    Ok(varint_len) => {
                        #[cfg(debug_assertions)]
                        debug!("Second varint length: {}", varint_len);
                        if varint_len == 0 {
                            #[cfg(debug_assertions)]
                            debug!("Null string for client_software_version");
                            None
                        } else {
                            let actual_len = (varint_len - 1) as usize; // Java: readUnsignedVarint() - 1
                            if cursor.remaining() >= actual_len {
                                let mut buf = vec![0u8; actual_len];
                                cursor.copy_to_slice(&mut buf);
                                match String::from_utf8(buf) {
                                    Ok(version_str) => {
                                        #[cfg(debug_assertions)]
                                        debug!("Decoded client_software_version (len={}): {:?}", actual_len, version_str);
                                        Some(version_str)
                                    }
                                    Err(e) => {
                                        warn!("Invalid UTF-8 in client_software_version: {}", e);
                                        None
                                    }
                                }
                            } else {
                                warn!("Invalid string length: {} (remaining: {})", actual_len, cursor.remaining());
                                None
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to decode varint for client_software_version: {}", e);
                        None
                    }
                }
            } else {
                None
            };
            
            // Tagged fields: decode number of tagged fields
            if cursor.remaining() > 0 {
                match Self::decode_varint(cursor) {
                    Ok(num_tagged_fields) => {
                        #[cfg(debug_assertions)]
                        debug!("Tagged fields count: {}", num_tagged_fields);
                        // Skip tagged fields for now (TODO: implement proper tagged field parsing)
                        for i in 0..num_tagged_fields {
                            // Each tagged field has: tag (varint) + size (varint) + data
                            if let (Ok(tag), Ok(size)) = (Self::decode_varint(cursor), Self::decode_varint(cursor)) {
                                #[cfg(debug_assertions)]
                                debug!("  Tagged field {}: tag={}, size={}", i, tag, size);
                                // CRITICAL FIX: Check bounds before advancing
                                let advance_size = size as usize;
                                if cursor.remaining() >= advance_size {
                                    cursor.advance(advance_size);
                                    #[cfg(debug_assertions)]
                                    debug!("  Advanced by {} bytes, remaining: {}", advance_size, cursor.remaining());
                                } else {
                                    warn!("  Cannot advance by {} bytes, only {} remaining. Consuming all remaining bytes.", 
                                          advance_size, cursor.remaining());
                                    cursor.advance(cursor.remaining()); // Consume what we have
                                    break; // Exit tagged fields loop
                                }
                            } else {
                                warn!("Failed to decode tagged field {}", i);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to decode tagged fields count: {}", e);
                        // Consume remaining bytes as a fallback
                        let remaining = cursor.remaining();
                        if remaining > 0 {
                            #[cfg(debug_assertions)]
                            debug!("Consuming {} remaining bytes", remaining);
                            cursor.advance(remaining);
                        }
                    }
                }
            }
            
            #[cfg(debug_assertions)]
            debug!("Successfully parsed Java 4.1 ApiVersions format");
            
            (name, version)
        } else {
            // v0-v2: No client software fields
            #[cfg(debug_assertions)]
            debug!("Decoding ApiVersions v0-v2 (no client software fields)");
            (None, None)
        };

        #[cfg(debug_assertions)]
        debug!(
            "Decoded ApiVersions: name={:?}, version={:?}",
            client_software_name, client_software_version
        );

        Ok(KafkaApiVersionsRequest {
            header,
            client_software_name,
            client_software_version,
        })
    }

    fn encode_api_versions_response(
        response: &KafkaApiVersionsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // Kafka ApiVersions response format - version aware
        buf.put_i32(response.header.correlation_id);
        
        // Version-aware response encoding based on request version
        if response.api_version >= 3 {
            // v3+ uses flexible versions with tagged fields (KIP-482)
            #[cfg(debug_assertions)]
            debug!("Encoding ApiVersions v{} with flexible versions (FIXED ORDER)", response.api_version);
            
            // DEBUG: Log response structure for investigation
            #[cfg(debug_assertions)]
            debug!("ApiVersions v{} response structure:", response.api_version);
            #[cfg(debug_assertions)]
            debug!("  correlation_id: {}", response.header.correlation_id);  
            #[cfg(debug_assertions)]
            debug!("  error_code: {}", response.error_code);
            #[cfg(debug_assertions)]
            debug!("  api_keys count: {}", response.api_keys.len());
            #[cfg(debug_assertions)]
            debug!("  throttle_time_ms: {}", response.throttle_time_ms);
            
            // 1. ErrorCode (i16) - FIRST field after correlation_id
            #[cfg(debug_assertions)]
            debug!("  Adding error_code: {}", response.error_code);
            buf.put_i16(response.error_code);
            
            // 2. API Keys compact array - SECOND field
            let compact_length = (response.api_keys.len() as u64) + 1;
            #[cfg(debug_assertions)]
            debug!("  compact_length (raw): {}", response.api_keys.len());
            #[cfg(debug_assertions)]
            debug!("  compact_length (encoded): {}", compact_length);
            
            Self::encode_varint(buf, compact_length);
            
            for (i, api_key) in response.api_keys.iter().enumerate() {
                if i < 5 {  // Only log first 5 for brevity
                    #[cfg(debug_assertions)]
                    debug!("  api_key[{}]: key={}, min={}, max={}", i, api_key.api_key, api_key.min_version, api_key.max_version);
                }
                buf.put_i16(api_key.api_key);
                buf.put_i16(api_key.min_version);
                buf.put_i16(api_key.max_version);
                
                // Tagged fields for each API key (empty for now)
                buf.put_u8(0); // empty tagged fields
            }
            
            // 3. ThrottleTimeMs - THIRD field (correct Kafka specification position)
            #[cfg(debug_assertions)]
            debug!("  Adding throttle_time_ms: {} at CORRECT position", response.throttle_time_ms);
            buf.put_i32(response.throttle_time_ms);

            // Add new fields for v3+ Java client compatibility
            if response.api_version >= 3 {
                // cluster_id (compact nullable string)
                #[cfg(debug_assertions)]
                debug!("  Adding cluster_id: {:?}", response.cluster_id);
                Self::encode_compact_nullable_string(&response.cluster_id, buf);
                
                // controller_id (i32) - Added for v3+
                if let Some(controller_id) = response.controller_id {
                    #[cfg(debug_assertions)]
                    debug!("  Adding controller_id: {}", controller_id);
                    buf.put_i32(controller_id);
                } else {
                    #[cfg(debug_assertions)]
                    debug!("  Adding controller_id: -1 (no controller)");
                    buf.put_i32(-1); // -1 indicates no controller
                }
                
                // supported_features (compact array) - Added for v3+
                #[cfg(debug_assertions)]
                debug!("  Adding supported_features count: {}", response.supported_features.len());
                Self::encode_compact_array_len(response.supported_features.len(), buf);
                for feature in &response.supported_features {
                    Self::encode_compact_string(feature, buf);
                }
            }

            // Top-level tagged fields (empty for now)  
            #[cfg(debug_assertions)]
            debug!("  Adding final tagged fields marker: 0x00");
            buf.put_u8(0); // empty tagged fields
        } else {
            // v0-v2 uses traditional fixed arrays
            #[cfg(debug_assertions)]
            debug!("Encoding ApiVersions v{} with standard format", response.api_version);
            buf.put_i32(response.api_keys.len() as i32);
            
            for api_key in &response.api_keys {
                buf.put_i16(api_key.api_key);
                buf.put_i16(api_key.min_version);
                buf.put_i16(api_key.max_version);
            }
            
            // 3. ThrottleTimeMs (for v1-v2, after api_keys)
            if response.api_version >= 1 {
                #[cfg(debug_assertions)]
                debug!("  Adding throttle_time_ms: {} at end of v0-v2 response", response.throttle_time_ms);
                buf.put_i32(response.throttle_time_ms);
            }
        }
        
        Ok(())
    }

    // ========================================================================
    // PRIMITIVE ENCODING/DECODING HELPERS
    // ========================================================================

    fn decode_string(cursor: &mut Cursor<&[u8]>) -> Result<String> {
        let len = cursor.get_i16();
        #[cfg(debug_assertions)]
        debug!("decode_string: length = {}", len);
        if len == -1 {
            return Err(KafkaCodecError::InvalidFormat(
                "Expected non-null string".to_string(),
            ));
        }

        if len == 0 {
            #[cfg(debug_assertions)]
            debug!("decode_string: empty string");
            return Ok(String::new());
        }

        // Note: len is i16, so already bounded to -32768..32767
        // No need for upper bound check since i16 max is 32767

        // Check if we have enough bytes remaining
        let remaining = cursor.get_ref().len() - cursor.position() as usize;
        if len as usize > remaining {
            // DEFENSIVE: Log detailed information for debugging framing issues
            warn!("Buffer underrun in decode_string: requested {} bytes, only {} available at position {}. Buffer total length: {}",
                  len, remaining, cursor.position(), cursor.get_ref().len());
            return Err(KafkaCodecError::InvalidFormat(format!(
                "String length {} exceeds remaining buffer size {}. This may indicate a framing error or corrupt message.", 
                len, remaining
            )));
        }
        
        let mut buf = vec![0u8; len as usize];
        cursor.copy_to_slice(&mut buf);
        let result = String::from_utf8(buf)
            .map_err(|e| KafkaCodecError::InvalidFormat(format!("Invalid UTF-8: {}", e)))?;
        #[cfg(debug_assertions)]
        debug!("decode_string: result = '{}'", result);
        Ok(result)
    }

    fn decode_nullable_string(cursor: &mut Cursor<&[u8]>) -> Result<Option<String>> {
        let len = cursor.get_i16();
        if len == -1 {
            return Ok(None);
        }

        if len == 0 {
            return Ok(Some(String::new()));
        }

        // Note: len is i16, so already bounded to -32768..32767
        // No need for upper bound check since i16 max is 32767

        // Check if we have enough bytes remaining
        let remaining = cursor.get_ref().len() - cursor.position() as usize;
        if len as usize > remaining {
            // DEFENSIVE: If buffer underrun, return a graceful error instead of panic
            warn!("Buffer underrun in decode_nullable_string: requested {} bytes, only {} available at position {}. Buffer total length: {}",
                  len, remaining, cursor.position(), cursor.get_ref().len());
            return Err(KafkaCodecError::InvalidFormat(format!(
                "Nullable string length {} exceeds remaining buffer size {}. This may indicate a framing error or corrupt message.", 
                len, remaining
            )));
        }
        
        let mut buf = vec![0u8; len as usize];
        cursor.copy_to_slice(&mut buf);
        let s = String::from_utf8(buf)
            .map_err(|e| KafkaCodecError::InvalidFormat(format!("Invalid UTF-8: {}", e)))?;
        Ok(Some(s))
    }


    fn decode_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Bytes> {
        let len = cursor.get_i32();
        if len == -1 {
            return Err(KafkaCodecError::InvalidFormat(
                "Expected non-null bytes".to_string(),
            ));
        }

        if len == 0 {
            return Ok(Bytes::new());
        }

        // Check if we have enough bytes remaining
        let remaining = cursor.get_ref().len() - cursor.position() as usize;
        if len as usize > remaining {
            return Err(KafkaCodecError::InvalidFormat(format!(
                "Bytes length {} exceeds remaining buffer size {}", len, remaining
            )));
        }
        
        let mut buf = vec![0u8; len as usize];
        cursor.copy_to_slice(&mut buf);
        Ok(Bytes::from(buf))
    }

    fn decode_nullable_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Option<Bytes>> {
        let len = cursor.get_i32();
        if len == -1 {
            return Ok(None);
        }

        if len == 0 {
            return Ok(Some(Bytes::new()));
        }

        // Check if we have enough bytes remaining
        let remaining = cursor.get_ref().len() - cursor.position() as usize;
        if len as usize > remaining {
            return Err(KafkaCodecError::InvalidFormat(format!(
                "Nullable bytes length {} exceeds remaining buffer size {}", len, remaining
            )));
        }
        
        let mut buf = vec![0u8; len as usize];
        cursor.copy_to_slice(&mut buf);
        Ok(Some(Bytes::from(buf)))
    }

    fn encode_string(s: &str, buf: &mut BytesMut) {
        buf.put_i16(s.len() as i16);
        buf.put_slice(s.as_bytes());
    }

    fn encode_nullable_string(s: &Option<String>, buf: &mut BytesMut) {
        match s {
            Some(s) => {
                buf.put_i16(s.len() as i16);
                buf.put_slice(s.as_bytes());
            }
            None => buf.put_i16(-1),
        }
    }

    fn encode_bytes(bytes: &Bytes, buf: &mut BytesMut) {
        buf.put_i32(bytes.len() as i32);
        buf.put_slice(bytes);
    }

    fn encode_nullable_bytes(bytes: &Option<Bytes>, buf: &mut BytesMut) {
        match bytes {
            Some(bytes) => {
                buf.put_i32(bytes.len() as i32);
                buf.put_slice(bytes);
            }
            None => buf.put_i32(-1),
        }
    }

    // ========================================================================
    // CREATE TOPICS REQUEST/RESPONSE
    // ========================================================================

    fn decode_create_topics_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaCreateTopicsRequest> {
        let topic_count = cursor.get_i32();
        let mut topics = Vec::with_capacity(topic_count as usize);

        for _ in 0..topic_count {
            let name = Self::decode_string(cursor)?;
            let num_partitions = cursor.get_i32();
            let replication_factor = cursor.get_i16();

            // Skip assignments (empty for simple case)
            let assignment_count = cursor.get_i32();
            for _ in 0..assignment_count {
                let _partition_id = cursor.get_i32();
                let replica_count = cursor.get_i32();
                for _ in 0..replica_count {
                    let _broker_id = cursor.get_i32();
                }
            }

            // Read configs
            let config_count = cursor.get_i32();
            let mut configs = Vec::new();
            for _ in 0..config_count {
                let config_name = Self::decode_string(cursor)?;
                let config_value = Self::decode_nullable_string(cursor)?;
                configs.push(KafkaCreatableTopicConfigs {
                    name: config_name,
                    value: config_value,
                    read_only: false,
                    config_source: 0,
                    is_sensitive: false,
                });
            }

            topics.push(KafkaCreatableTopic {
                name,
                num_partitions,
                replication_factor,
                assignments: vec![],
                configs: Some(configs),
            });
        }

        let timeout_ms = cursor.get_i32();
        let validate_only = if cursor.remaining() > 0 {
            cursor.get_u8() != 0
        } else {
            false
        };

        Ok(KafkaCreateTopicsRequest {
            header,
            topics,
            timeout_ms,
            validate_only,
        })
    }

    fn encode_create_topics_response(
        response: &KafkaCreateTopicsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let api_version = response.api_version;
        let flexible = api_version >= 5;

        // Correlation ID
        buf.put_i32(response.header.correlation_id);

        // Header tagged fields (flexible versions)
        if flexible {
            Self::encode_varint(buf, 0); // num_tagged_fields = 0
        }

        // Throttle time ms
        buf.put_i32(response.throttle_time_ms);

        // Topics array
        if flexible {
            Self::encode_varint(buf, (response.topics.len() + 1) as u64);
        } else {
            buf.put_i32(response.topics.len() as i32);
        }

        for topic in &response.topics {
            // Topic name
            if flexible {
                Self::encode_compact_string(&topic.name, buf);
            } else {
                Self::encode_string(&topic.name, buf);
            }

            // Topic ID (v7+)
            if api_version >= 7 {
                // UUID (16 bytes) - use nil UUID for now
                buf.put_slice(&[0u8; 16]);
            }

            // Error code
            buf.put_i16(topic.error_code);

            // Error message
            if flexible {
                Self::encode_compact_nullable_string(&topic.error_message, buf);
            } else {
                Self::encode_nullable_string(&topic.error_message, buf);
            }

            // Num partitions (v5+)
            if api_version >= 5 {
                buf.put_i32(topic.num_partitions.unwrap_or(-1));
            }

            // Replication factor (v5+)
            if api_version >= 5 {
                buf.put_i16(topic.replication_factor.unwrap_or(-1));
            }

            // Configs array (empty for basic response)
            if flexible {
                Self::encode_varint(buf, 1); // Empty array = 1 (length + 1)
            } else {
                buf.put_i32(0);
            }

            // Tagged fields for topic (flexible versions)
            if flexible {
                Self::encode_varint(buf, 0); // num_tagged_fields = 0
            }
        }

        // Response-level tagged fields (flexible versions)
        if flexible {
            Self::encode_varint(buf, 0); // num_tagged_fields = 0
        }

        Ok(())
    }

    /// Decode DeleteTopics request
    fn decode_delete_topics_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaDeleteTopicsRequest> {
        // DeleteTopics v4+ uses flexible encoding
        let is_flexible = header.api_version >= 4;

        // Topic count: compact array for flexible, i32 for non-flexible
        let topic_count = if is_flexible {
            let varint = Self::decode_varint(cursor)? as i32;
            if varint == 0 { 0 } else { varint - 1 }
        } else {
            cursor.get_i32()
        };

        let mut topic_names = Vec::with_capacity(topic_count as usize);

        for _ in 0..topic_count {
            // For v6+, topics are structs with name and topic_id fields
            if header.api_version >= 6 {
                // Topic name (compact nullable string for v6+)
                let topic_name = Self::decode_compact_string(cursor)?;
                // Topic ID (16 bytes UUID) - skip it
                if cursor.remaining() >= 16 {
                    cursor.advance(16);
                }
                // Skip tagged fields
                Self::skip_tagged_fields(cursor)?;
                if let Some(name) = topic_name {
                    topic_names.push(name);
                }
            } else if is_flexible {
                let topic_name = Self::decode_compact_non_nullable_string(cursor)?;
                topic_names.push(topic_name);
            } else {
                let topic_name = Self::decode_string(cursor)?;
                topic_names.push(topic_name);
            }
        }

        // API version dependent fields - for now we support v0
        let timeout_ms = if header.api_version >= 1 {
            cursor.get_i32()
        } else {
            5000 // Default timeout
        };

        // Skip tagged fields at the end for flexible versions
        if is_flexible {
            Self::skip_tagged_fields(cursor)?;
        }

        Ok(KafkaDeleteTopicsRequest {
            correlation_id: header.correlation_id,
            client_id: header.client_id,
            api_version: header.api_version,
            topic_names,
            timeout_ms,
        })
    }

    /// Encode DeleteTopics response
    fn encode_delete_topics_response(
        response: &KafkaDeleteTopicsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let api_version = response.api_version;
        let flexible = api_version >= 4;

        // Correlation ID
        buf.put_i32(response.correlation_id);

        // Header tagged fields (flexible versions)
        if flexible {
            Self::encode_varint(buf, 0); // num_tagged_fields = 0
        }

        // Throttle time (API v1+)
        buf.put_i32(response.throttle_time_ms);

        // Response array
        if flexible {
            Self::encode_varint(buf, (response.responses.len() + 1) as u64);
        } else {
            buf.put_i32(response.responses.len() as i32);
        }

        for topic_response in &response.responses {
            // Topic name
            if flexible {
                Self::encode_compact_string(&topic_response.name, buf);
            } else {
                Self::encode_string(&topic_response.name, buf);
            }

            // Topic ID (v6+)
            if api_version >= 6 {
                // UUID (16 bytes) - use nil UUID for now
                buf.put_slice(&[0u8; 16]);
            }

            // Error code
            buf.put_i16(topic_response.error_code);

            // Error message (v5+)
            if api_version >= 5 {
                if flexible {
                    Self::encode_compact_nullable_string(&topic_response.error_message, buf);
                } else {
                    Self::encode_nullable_string(&topic_response.error_message, buf);
                }
            }

            // Tagged fields for topic (flexible versions)
            if flexible {
                Self::encode_varint(buf, 0); // num_tagged_fields = 0
            }
        }

        // Response-level tagged fields (flexible versions)
        if flexible {
            Self::encode_varint(buf, 0); // num_tagged_fields = 0
        }

        Ok(())
    }

    /// Decode SASL Handshake request
    fn decode_sasl_handshake_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaSaslHandshakeRequest> {
        let mechanism = Self::decode_string(cursor)?;

        Ok(KafkaSaslHandshakeRequest {
            correlation_id: header.correlation_id,
            client_id: header.client_id,
            mechanism,
        })
    }

    /// Encode SASL Handshake response
    fn encode_sasl_handshake_response(
        response: &KafkaSaslHandshakeResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // Correlation ID
        buf.put_i32(response.correlation_id);
        
        // Error code
        buf.put_i16(response.error_code);
        
        // Supported mechanisms array
        buf.put_i32(response.mechanisms.len() as i32);
        for mechanism in &response.mechanisms {
            Self::encode_string(mechanism, buf);
        }
        
        Ok(())
    }

    /// Decode SASL Authenticate request
    fn decode_sasl_authenticate_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaSaslAuthenticateRequest> {
        // Read auth bytes length and data
        let auth_bytes_length = cursor.get_i32();
        let mut auth_bytes = vec![0u8; auth_bytes_length as usize];
        cursor.read_exact(&mut auth_bytes).map_err(|_| {
            KafkaCodecError::InvalidFormat("Failed to read SASL auth bytes".to_string())
        })?;

        Ok(KafkaSaslAuthenticateRequest {
            correlation_id: header.correlation_id,
            client_id: header.client_id,
            auth_bytes,
        })
    }

    /// Encode SASL Authenticate response
    fn encode_sasl_authenticate_response(
        response: &KafkaSaslAuthenticateResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // Correlation ID
        buf.put_i32(response.correlation_id);
        
        // Error code
        buf.put_i16(response.error_code);
        
        // Error message (nullable)
        Self::encode_nullable_string(&response.error_message, buf);
        
        // Auth bytes length and data
        buf.put_i32(response.auth_bytes.len() as i32);
        buf.put_slice(&response.auth_bytes);
        
        // Session lifetime (API v1+)
        buf.put_i64(response.session_lifetime_ms);
        
        Ok(())
    }

    // ========================================================================
    // DESCRIBE CONFIGS REQUEST/RESPONSE
    // ========================================================================

    fn decode_describe_configs_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaDescribeConfigsRequest> {
        // DescribeConfigs v4+ uses flexible encoding
        let is_flexible = header.api_version >= 4;

        // Resources array length
        let resources_length = if is_flexible {
            let varint = Self::decode_varint(cursor)? as i32;
            if varint == 0 { 0 } else { varint - 1 }
        } else {
            cursor.get_i32()
        };

        let mut resources = Vec::with_capacity(resources_length as usize);

        for _ in 0..resources_length {
            // Resource type
            let resource_type = cursor.get_i8();

            // Resource name
            let resource_name = if is_flexible {
                Self::decode_compact_non_nullable_string(cursor)?
            } else {
                Self::decode_string(cursor)?
            };

            // Configuration keys array
            let config_keys_length = if is_flexible {
                let varint = Self::decode_varint(cursor)? as i32;
                if varint == 0 { -1 } else { varint - 1 }  // 0 means null array
            } else {
                cursor.get_i32()
            };

            let configuration_keys = if config_keys_length == -1 {
                None
            } else {
                let mut keys = Vec::with_capacity(config_keys_length as usize);
                for _ in 0..config_keys_length {
                    let key = if is_flexible {
                        Self::decode_compact_non_nullable_string(cursor)?
                    } else {
                        Self::decode_string(cursor)?
                    };
                    keys.push(key);
                }
                Some(keys)
            };

            // Skip tagged fields for flexible versions
            if is_flexible {
                Self::skip_tagged_fields(cursor)?;
            }

            resources.push(KafkaConfigResource {
                resource_type,
                resource_name,
                configuration_keys,
            });
        }

        // Include synonyms (API v1+)
        let include_synonyms = if header.api_version >= 1 {
            cursor.get_u8() != 0
        } else {
            false
        };

        // Include documentation (API v3+)
        let include_documentation = if header.api_version >= 3 {
            cursor.get_u8() != 0
        } else {
            false
        };

        // Skip tagged fields at the end for flexible versions
        if is_flexible {
            Self::skip_tagged_fields(cursor)?;
        }

        Ok(KafkaDescribeConfigsRequest {
            correlation_id: header.correlation_id,
            client_id: header.client_id,
            api_version: header.api_version,
            resources,
            include_synonyms,
            include_documentation,
        })
    }

    fn encode_describe_configs_response(
        response: &KafkaDescribeConfigsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let api_version = response.api_version;
        let flexible = api_version >= 4;

        // Correlation ID
        buf.put_i32(response.correlation_id);

        // Header tagged fields (flexible versions)
        if flexible {
            Self::encode_varint(buf, 0); // num_tagged_fields = 0
        }

        // Throttle time (API v0+)
        buf.put_i32(response.throttle_time_ms);

        // Results array
        if flexible {
            Self::encode_varint(buf, (response.results.len() + 1) as u64);
        } else {
            buf.put_i32(response.results.len() as i32);
        }

        for result in &response.results {
            // Error code
            buf.put_i16(result.error_code);

            // Error message (nullable string)
            if flexible {
                Self::encode_compact_nullable_string(&result.error_message, buf);
            } else {
                Self::encode_nullable_string(&result.error_message, buf);
            }

            // Resource type
            buf.put_i8(result.resource_type);

            // Resource name
            if flexible {
                Self::encode_compact_string(&result.resource_name, buf);
            } else {
                Self::encode_string(&result.resource_name, buf);
            }

            // Configs array
            if flexible {
                Self::encode_varint(buf, (result.configs.len() + 1) as u64);
            } else {
                buf.put_i32(result.configs.len() as i32);
            }

            for config in &result.configs {
                // Config name
                if flexible {
                    Self::encode_compact_string(&config.name, buf);
                } else {
                    Self::encode_string(&config.name, buf);
                }

                // Config value (nullable string)
                if flexible {
                    Self::encode_compact_nullable_string(&config.value, buf);
                } else {
                    Self::encode_nullable_string(&config.value, buf);
                }

                // Read only
                buf.put_u8(if config.read_only { 1 } else { 0 });

                // Is default (API v0 only, removed in v1+)
                if api_version < 1 {
                    buf.put_u8(if config.is_default { 1 } else { 0 });
                }

                // Config source (API v1+)
                if api_version >= 1 {
                    buf.put_i8(config.config_source);
                }

                // Is sensitive
                buf.put_u8(if config.is_sensitive { 1 } else { 0 });

                // Synonyms array (API v1+)
                if api_version >= 1 {
                    if flexible {
                        Self::encode_varint(buf, (config.synonyms.len() + 1) as u64);
                    } else {
                        buf.put_i32(config.synonyms.len() as i32);
                    }
                    for synonym in &config.synonyms {
                        if flexible {
                            Self::encode_compact_string(&synonym.name, buf);
                            Self::encode_compact_nullable_string(&synonym.value, buf);
                        } else {
                            Self::encode_string(&synonym.name, buf);
                            Self::encode_nullable_string(&synonym.value, buf);
                        }
                        buf.put_i8(synonym.source);

                        // Tagged fields for synonym (flexible versions)
                        if flexible {
                            Self::encode_varint(buf, 0); // num_tagged_fields = 0
                        }
                    }
                }

                // Config type (API v3+)
                if api_version >= 3 {
                    buf.put_i8(config.config_type);
                }

                // Documentation (API v3+)
                if api_version >= 3 {
                    if flexible {
                        Self::encode_compact_nullable_string(&config.documentation, buf);
                    } else {
                        Self::encode_nullable_string(&config.documentation, buf);
                    }
                }

                // Tagged fields for config (flexible versions)
                if flexible {
                    Self::encode_varint(buf, 0); // num_tagged_fields = 0
                }
            }

            // Tagged fields for result (flexible versions)
            if flexible {
                Self::encode_varint(buf, 0); // num_tagged_fields = 0
            }
        }

        // Response-level tagged fields (flexible versions)
        if flexible {
            Self::encode_varint(buf, 0); // num_tagged_fields = 0
        }

        Ok(())
    }

    // ========================================================================
    // ALTER CONFIGS REQUEST/RESPONSE (API KEY 33)
    // ========================================================================
    
    fn decode_alter_configs_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaAlterConfigsRequest> {
        // AlterConfigs v2+ uses flexible encoding
        let is_flexible = header.api_version >= 2;

        // Resources array length
        let resources_length = if is_flexible {
            let varint = Self::decode_varint(cursor)? as i32;
            if varint == 0 { 0 } else { varint - 1 }
        } else {
            cursor.get_i32()
        };

        let mut resources = Vec::with_capacity(resources_length as usize);

        for _ in 0..resources_length {
            // Resource type
            let resource_type = cursor.get_i8();

            // Resource name
            let resource_name = if is_flexible {
                Self::decode_compact_non_nullable_string(cursor)?
            } else {
                Self::decode_string(cursor)?
            };

            // Configs array length
            let configs_length = if is_flexible {
                let varint = Self::decode_varint(cursor)? as i32;
                if varint == 0 { 0 } else { varint - 1 }
            } else {
                cursor.get_i32()
            };

            let mut configs = Vec::with_capacity(configs_length as usize);

            for _ in 0..configs_length {
                // Config name
                let name = if is_flexible {
                    Self::decode_compact_non_nullable_string(cursor)?
                } else {
                    Self::decode_string(cursor)?
                };

                // Config value (nullable string)
                let value = if is_flexible {
                    Self::decode_compact_string(cursor)?
                } else {
                    Self::decode_nullable_string(cursor)?
                };

                // Skip tagged fields for flexible versions
                if is_flexible {
                    Self::skip_tagged_fields(cursor)?;
                }

                configs.push(KafkaAlterableConfig { name, value });
            }

            // Skip tagged fields for flexible versions
            if is_flexible {
                Self::skip_tagged_fields(cursor)?;
            }

            resources.push(KafkaAlterConfigsResource {
                resource_type,
                resource_name,
                configs,
            });
        }

        // Validate only (API v0+)
        let validate_only = cursor.get_u8() != 0;

        // Skip tagged fields at the end for flexible versions
        if is_flexible {
            Self::skip_tagged_fields(cursor)?;
        }

        Ok(KafkaAlterConfigsRequest {
            correlation_id: header.correlation_id,
            client_id: header.client_id,
            api_version: header.api_version,
            resources,
            validate_only,
        })
    }

    fn encode_alter_configs_response(
        response: &KafkaAlterConfigsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let api_version = response.api_version;
        let flexible = api_version >= 2;

        // Correlation ID
        buf.put_i32(response.correlation_id);

        // Header tagged fields (flexible versions)
        if flexible {
            Self::encode_varint(buf, 0); // num_tagged_fields = 0
        }

        // Throttle time (API v0+)
        buf.put_i32(response.throttle_time_ms);

        // Responses array
        if flexible {
            Self::encode_varint(buf, (response.responses.len() + 1) as u64);
        } else {
            buf.put_i32(response.responses.len() as i32);
        }

        for resource_response in &response.responses {
            // Error code
            buf.put_i16(resource_response.error_code);

            // Error message (nullable string)
            if flexible {
                Self::encode_compact_nullable_string(&resource_response.error_message, buf);
            } else {
                Self::encode_nullable_string(&resource_response.error_message, buf);
            }

            // Resource type
            buf.put_i8(resource_response.resource_type);

            // Resource name
            if flexible {
                Self::encode_compact_string(&resource_response.resource_name, buf);
            } else {
                Self::encode_string(&resource_response.resource_name, buf);
            }

            // Tagged fields for resource response (flexible versions)
            if flexible {
                Self::encode_varint(buf, 0); // num_tagged_fields = 0
            }
        }

        // Response-level tagged fields (flexible versions)
        if flexible {
            Self::encode_varint(buf, 0); // num_tagged_fields = 0
        }

        Ok(())
    }

    // ========================================================================
    // INCREMENTAL ALTER CONFIGS API (ApiKey = 44)
    // ========================================================================

    fn decode_incremental_alter_configs_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaIncrementalAlterConfigsRequest> {
        // IncrementalAlterConfigs is always flexible (from v0)
        let is_flexible = true;

        // Resources array length
        let resources_length = if is_flexible {
            let varint = Self::decode_varint(cursor)? as i32;
            if varint == 0 { 0 } else { varint - 1 }
        } else {
            cursor.get_i32()
        };

        let mut resources = Vec::with_capacity(resources_length as usize);

        for _ in 0..resources_length {
            // Resource type
            let resource_type = cursor.get_i8();

            // Resource name
            let resource_name = Self::decode_compact_non_nullable_string(cursor)?;

            // Configs array length
            let configs_length = {
                let varint = Self::decode_varint(cursor)? as i32;
                if varint == 0 { 0 } else { varint - 1 }
            };

            let mut configs = Vec::with_capacity(configs_length as usize);

            for _ in 0..configs_length {
                // Config name
                let name = Self::decode_compact_non_nullable_string(cursor)?;

                // Config operation (0=SET, 1=DELETE, 2=APPEND, 3=SUBTRACT)
                let config_operation = cursor.get_i8();

                // Config value (nullable string)
                let value = Self::decode_compact_string(cursor)?;

                // Skip tagged fields for config entry
                Self::skip_tagged_fields(cursor)?;

                configs.push(KafkaIncrementalAlterableConfig {
                    name,
                    config_operation,
                    value,
                });
            }

            // Skip tagged fields for resource
            Self::skip_tagged_fields(cursor)?;

            resources.push(KafkaIncrementalAlterConfigsResource {
                resource_type,
                resource_name,
                configs,
            });
        }

        // Validate only
        let validate_only = cursor.get_u8() != 0;

        // Skip tagged fields at the end
        Self::skip_tagged_fields(cursor)?;

        Ok(KafkaIncrementalAlterConfigsRequest {
            correlation_id: header.correlation_id,
            client_id: header.client_id,
            api_version: header.api_version,
            resources,
            validate_only,
        })
    }

    fn encode_incremental_alter_configs_response(
        response: &KafkaIncrementalAlterConfigsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // IncrementalAlterConfigs is always flexible (from v0)

        // Correlation ID
        buf.put_i32(response.correlation_id);

        // Header tagged fields (flexible versions)
        Self::encode_varint(buf, 0); // num_tagged_fields = 0

        // Throttle time (API v0+)
        buf.put_i32(response.throttle_time_ms);

        // Responses array (compact array)
        Self::encode_varint(buf, (response.responses.len() + 1) as u64);

        for resource_response in &response.responses {
            // Error code
            buf.put_i16(resource_response.error_code);

            // Error message (compact nullable string)
            Self::encode_compact_nullable_string(&resource_response.error_message, buf);

            // Resource type
            buf.put_i8(resource_response.resource_type);

            // Resource name (compact string)
            Self::encode_compact_string(&resource_response.resource_name, buf);

            // Tagged fields for resource response
            Self::encode_varint(buf, 0); // num_tagged_fields = 0
        }

        // Response-level tagged fields
        Self::encode_varint(buf, 0); // num_tagged_fields = 0

        Ok(())
    }

    /// Encode varint (unsigned variable-length integer)

    // ========================================================================
    // TAGGED FIELDS AND FLEXIBLE VERSIONS SUPPORT (KIP-482)
    // ========================================================================

    /// Encodes a varint (variable-length integer) according to LEB128 encoding
    #[allow(dead_code)]
    fn encode_varint(buf: &mut BytesMut, value: u64) {
        let mut val = value;
        loop {
            let mut byte = (val & 0x7F) as u8;
            val >>= 7;
            if val != 0 {
                byte |= 0x80;
            }
            buf.put_u8(byte);
            if val == 0 {
                break;
            }
        }
    }

    /// Decodes a varint (variable-length integer) according to LEB128 encoding
    fn decode_varint(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
        let mut result = 0u64;
        let mut shift = 0;
        
        loop {
            if shift >= 64 {
                return Err(KafkaCodecError::InvalidFormat("Varint too large".to_string()));
            }
            
            if !cursor.has_remaining() {
                return Err(KafkaCodecError::InvalidFormat("Incomplete varint".to_string()));
            }
            
            let byte = cursor.get_u8();
            result |= ((byte & 0x7F) as u64) << shift;
            
            if (byte & 0x80) == 0 {
                break;
            }
            
            shift += 7;
        }
        
        Ok(result)
    }


    /// Decodes a compact string (nullable string with varint length) for flexible versions
    fn decode_compact_string(cursor: &mut Cursor<&[u8]>) -> Result<Option<String>> {
        let len = Self::decode_varint(cursor)?;
        
        if len == 0 {
            // Null string
            return Ok(None);
        }
        
        if len == 1 {
            // Empty string
            return Ok(Some(String::new()));
        }
        
        // Actual length is len - 1
        let actual_len = (len - 1) as usize;
        if cursor.remaining() < actual_len {
            return Err(KafkaCodecError::InvalidFormat(
                "Not enough bytes for compact string".to_string()
            ));
        }
        
        let mut buf = vec![0u8; actual_len];
        cursor.copy_to_slice(&mut buf);
        let result = String::from_utf8(buf)
            .map_err(|e| KafkaCodecError::InvalidFormat(format!("Invalid UTF-8: {}", e)))?;
        
        Ok(Some(result))
    }

    /// Encodes an empty tagged field array (just a varint 0)
    #[allow(dead_code)]
    fn encode_empty_tagged_fields(buf: &mut BytesMut) {
        // Empty tagged field array = varint 0
        buf.put_u8(0);
    }

    /// Decodes tagged fields (for now, just skip them) - with improved error handling
    #[allow(dead_code)]
    fn decode_tagged_fields(cursor: &mut Cursor<&[u8]>) -> Result<()> {
        // If no remaining bytes, assume empty tagged fields
        if cursor.remaining() == 0 {
            #[cfg(debug_assertions)]
            debug!("No remaining bytes for tagged fields, treating as empty");
            return Ok(());
        }
        
        let num_fields = match Self::decode_varint(cursor) {
            Ok(n) => n,
            Err(e) => {
                #[cfg(debug_assertions)]
                debug!("Failed to decode tagged field count, treating as empty: {}", e);
                // If we can't even read the count, treat as empty tagged fields
                return Ok(());
            }
        };
        
        #[cfg(debug_assertions)]
        debug!("Decoding {} tagged fields", num_fields);
        
        // For each tagged field, skip tag and data
        for i in 0..num_fields {
            #[cfg(debug_assertions)]
            debug!("Processing tagged field {}/{}", i + 1, num_fields);
            
            // Skip tag
            let tag = match Self::decode_varint(cursor) {
                Ok(tag) => tag,
                Err(e) => {
                    #[cfg(debug_assertions)]
                    debug!("Failed to decode tag {}: {}, stopping tagged field parsing", i, e);
                    break;
                }
            };
            
            // Skip data length
            let data_len = match Self::decode_varint(cursor) {
                Ok(len) => len,
                Err(e) => {
                    #[cfg(debug_assertions)]
                    debug!("Failed to decode data length for tag {}: {}, stopping tagged field parsing", tag, e);
                    break;
                }
            };
            
            #[cfg(debug_assertions)]
            debug!("Tagged field: tag={}, data_len={}", tag, data_len);
            
            // Skip data bytes with bounds checking
            if cursor.remaining() < data_len as usize {
                #[cfg(debug_assertions)]
                debug!(
                    "Not enough bytes for tagged field data: needed={}, available={}, treating as end of fields",
                    data_len, cursor.remaining()
                );
                break;
            }
            cursor.advance(data_len as usize);
            #[cfg(debug_assertions)]
            debug!("Successfully skipped tagged field data ({} bytes)", data_len);
        }
        
        #[cfg(debug_assertions)]
        debug!("Completed tagged fields parsing");
        Ok(())
    }

    /// Encodes a compact string (string with varint length) for flexible versions
    #[allow(dead_code)]
    fn encode_compact_string(s: &str, buf: &mut BytesMut) {
        // CRITICAL FIX: Compact string encoding MUST use varint consistently
        // Java Kafka expects: readUnsignedVarint() - 1 = actual_length
        let len = s.len();
        let varint_len = len as u64 + 1;
        Self::encode_varint(buf, varint_len);
        #[cfg(debug_assertions)]
        debug!("  - FIXED compact string '{}': length={}, encoded as varint {} (Java will read {})",
              s, len, varint_len, len);
        buf.put_slice(s.as_bytes());
    }

    /// Encodes a compact nullable string (with varint length) for flexible versions
    #[allow(dead_code)]
    fn encode_compact_nullable_string(s: &Option<String>, buf: &mut BytesMut) {
        match s {
            Some(s) => {
                // CRITICAL FIX: Compact nullable string MUST use varint consistently
                // Java Kafka expects: readUnsignedVarint() - 1 = actual_length
                let len = s.len();
                let varint_len = len as u64 + 1;
                Self::encode_varint(buf, varint_len);
                #[cfg(debug_assertions)]
                debug!("  - FIXED compact nullable string '{}': length={}, encoded as varint {} (Java will read {})",
                      s, len, varint_len, len);
                buf.put_slice(s.as_bytes());
            }
            None => {
                // CRITICAL FIX: Null string MUST be encoded as single byte 0, not varint
                buf.put_u8(0);
                #[cfg(debug_assertions)]
                debug!("  - FIXED compact nullable string: null, encoded as 0x00");
            }
        }
    }


    /// Encodes a compact array (array with varint length) for flexible versions
    #[allow(dead_code)]
    fn encode_compact_array_len(len: usize, buf: &mut BytesMut) {
        // Compact arrays use length + 1 encoding (0 = null, 1 = empty, 2+ = length + 1)
        Self::encode_varint(buf, len as u64 + 1);
    }

    /// Decodes a compact array length (returns actual count, not encoded value)
    /// Returns None if array is null (encoded as 0)
    #[allow(dead_code)]
    fn decode_compact_array_len(cursor: &mut Cursor<&[u8]>) -> Result<Option<usize>> {
        let encoded = Self::decode_varint(cursor)?;
        if encoded == 0 {
            // Null array
            Ok(None)
        } else {
            // Actual length is encoded - 1
            Ok(Some((encoded - 1) as usize))
        }
    }

    /// Decodes a compact string (non-nullable) for flexible versions
    /// Unlike decode_compact_string which returns Option<String>, this requires non-null
    #[allow(dead_code)]
    fn decode_compact_non_nullable_string(cursor: &mut Cursor<&[u8]>) -> Result<String> {
        let len = Self::decode_varint(cursor)?;

        if len == 0 {
            // Null string - should not happen for non-nullable
            return Err(KafkaCodecError::InvalidFormat(
                "Expected non-null compact string".to_string(),
            ));
        }

        if len == 1 {
            // Empty string
            return Ok(String::new());
        }

        // Actual length is len - 1
        let actual_len = (len - 1) as usize;
        if cursor.remaining() < actual_len {
            return Err(KafkaCodecError::InvalidFormat(format!(
                "Compact string length {} exceeds remaining buffer {}",
                actual_len, cursor.remaining()
            )));
        }

        let mut buf = vec![0u8; actual_len];
        cursor.copy_to_slice(&mut buf);
        String::from_utf8(buf)
            .map_err(|e| KafkaCodecError::InvalidFormat(format!("Invalid UTF-8: {}", e)))
    }

    /// Encodes compact bytes (bytes with varint length) for flexible versions
    #[allow(dead_code)]
    fn encode_compact_bytes(bytes: &Bytes, buf: &mut BytesMut) {
        // Compact bytes use length + 1 encoding
        let len = bytes.len();
        Self::encode_varint(buf, len as u64 + 1);
        buf.put_slice(bytes);
    }

    /// Encodes compact nullable bytes for flexible versions
    #[allow(dead_code)]
    fn encode_compact_nullable_bytes(bytes: &Option<Bytes>, buf: &mut BytesMut) {
        match bytes {
            Some(b) => {
                let len = b.len();
                Self::encode_varint(buf, len as u64 + 1);
                buf.put_slice(b);
            }
            None => {
                // Null bytes = 0
                buf.put_u8(0);
            }
        }
    }

    /// Decodes compact bytes for flexible versions
    #[allow(dead_code)]
    fn decode_compact_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Bytes> {
        let len = Self::decode_varint(cursor)?;

        if len == 0 {
            return Err(KafkaCodecError::InvalidFormat(
                "Expected non-null compact bytes".to_string(),
            ));
        }

        if len == 1 {
            return Ok(Bytes::new());
        }

        let actual_len = (len - 1) as usize;
        if cursor.remaining() < actual_len {
            return Err(KafkaCodecError::InvalidFormat(format!(
                "Compact bytes length {} exceeds remaining buffer {}",
                actual_len, cursor.remaining()
            )));
        }

        let mut buf = vec![0u8; actual_len];
        cursor.copy_to_slice(&mut buf);
        Ok(Bytes::from(buf))
    }

    /// Decodes compact nullable bytes for flexible versions
    #[allow(dead_code)]
    fn decode_compact_nullable_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Option<Bytes>> {
        let len = Self::decode_varint(cursor)?;

        if len == 0 {
            return Ok(None);
        }

        if len == 1 {
            return Ok(Some(Bytes::new()));
        }

        let actual_len = (len - 1) as usize;
        if cursor.remaining() < actual_len {
            return Err(KafkaCodecError::InvalidFormat(format!(
                "Compact nullable bytes length {} exceeds remaining buffer {}",
                actual_len, cursor.remaining()
            )));
        }

        let mut buf = vec![0u8; actual_len];
        cursor.copy_to_slice(&mut buf);
        Ok(Some(Bytes::from(buf)))
    }

    /// Helper to determine if an API version uses flexible encoding
    /// Based on Kafka protocol specifications
    #[allow(dead_code)]
    fn is_flexible_version(api_key: i16, api_version: i16) -> bool {
        match api_key {
            API_KEY_PRODUCE => api_version >= 9,
            API_KEY_FETCH => api_version >= 12,
            API_KEY_LIST_OFFSETS => api_version >= 6,
            API_KEY_METADATA => api_version >= 9,
            API_KEY_OFFSET_COMMIT => api_version >= 8,
            API_KEY_OFFSET_FETCH => api_version >= 6,
            API_KEY_FIND_COORDINATOR => api_version >= 3,
            API_KEY_JOIN_GROUP => api_version >= 6,
            API_KEY_HEARTBEAT => api_version >= 4,
            API_KEY_LEAVE_GROUP => api_version >= 4,
            API_KEY_SYNC_GROUP => api_version >= 4,
            API_KEY_DESCRIBE_GROUPS => api_version >= 5,
            API_KEY_LIST_GROUPS => api_version >= 3,
            API_KEY_API_VERSIONS => api_version >= 3,
            API_KEY_CREATE_TOPICS => api_version >= 5,
            API_KEY_DELETE_TOPICS => api_version >= 4,
            API_KEY_INIT_PRODUCER_ID => api_version >= 2,
            API_KEY_ADD_PARTITIONS_TO_TXN => api_version >= 2,
            API_KEY_ADD_OFFSETS_TO_TXN => api_version >= 2,
            API_KEY_END_TXN => api_version >= 2,
            API_KEY_TXN_OFFSET_COMMIT => api_version >= 2,
            32 => api_version >= 4, // DescribeConfigs
            33 => api_version >= 2, // AlterConfigs
            _ => false,
        }
    }

    /// Skip tagged fields (for decoding requests where we don't need the field values)
    #[allow(dead_code)]
    fn skip_tagged_fields(cursor: &mut Cursor<&[u8]>) -> Result<()> {
        if cursor.remaining() == 0 {
            return Ok(());
        }

        // Read the number of tagged fields (varint)
        let num_fields = match Self::decode_varint(cursor) {
            Ok(n) => n,
            Err(_) => return Ok(()), // If we can't read varint, just assume 0 fields
        };

        // If num_fields is 0, we're done
        if num_fields == 0 {
            return Ok(());
        }

        for _ in 0..num_fields {
            // Check if there's enough data for at least tag and length varints
            if cursor.remaining() < 2 {
                // Not enough data, just return OK (might be trailing garbage)
                return Ok(());
            }

            // Skip tag
            if Self::decode_varint(cursor).is_err() {
                return Ok(()); // Can't read tag, just return
            }

            // Get data length
            let data_len = match Self::decode_varint(cursor) {
                Ok(len) => len as usize,
                Err(_) => return Ok(()), // Can't read length, just return
            };

            if cursor.remaining() < data_len {
                // Not enough data for this field, just skip what we can
                let skip = cursor.remaining();
                cursor.advance(skip);
                return Ok(());
            }
            cursor.advance(data_len);
        }

        Ok(())
    }

    // ========================================================================
    // TRANSACTION API RESPONSE ENCODING
    // ========================================================================

    /// Encode InitProducerId response (API Key 22)
    /// Flexible versions: 2+ (uses compact encoding with tagged fields)
    fn encode_init_producer_id_response(
        buf: &mut BytesMut,
        response: &KafkaInitProducerIdResponse,
    ) -> Result<()> {
        let is_flexible = response.api_version >= 2;

        // Response header
        buf.put_i32(response.correlation_id);

        // For flexible versions (v2+), add response header tagged fields
        if is_flexible {
            buf.put_u8(0); // Empty tagged fields in response header
        }

        // Response body
        buf.put_i32(response.throttle_time_ms);
        buf.put_i16(response.error_code);
        buf.put_i64(response.producer_id);
        buf.put_i16(response.producer_epoch);

        // For flexible versions (v2+), add response body tagged fields
        if is_flexible {
            buf.put_u8(0); // Empty tagged fields in response body
        }

        Ok(())
    }

    /// Encode AddPartitionsToTxn response (API Key 24)
    /// Flexible versions: 3+ (uses compact encoding with tagged fields)
    fn encode_add_partitions_to_txn_response(
        buf: &mut BytesMut,
        response: &KafkaAddPartitionsToTxnResponse,
    ) -> Result<()> {
        let is_flexible = response.api_version >= 3;

        // Response header
        buf.put_i32(response.correlation_id);

        // For flexible versions (v3+), add response header tagged fields
        if is_flexible {
            buf.put_u8(0); // Empty tagged fields in response header
        }

        // Response body
        buf.put_i32(response.throttle_time_ms);

        // Encode ResultsByTopicV3AndBelow (used for v0-v3)
        // For v4+, this would be ResultsByTransaction instead
        if is_flexible {
            // Flexible encoding: compact array (length + 1 encoded as varint)
            Self::encode_compact_array_len(response.results.len(), buf);
            for topic_result in &response.results {
                // Compact string
                Self::encode_compact_string(&topic_result.name, buf);
                // Compact array for partition results
                Self::encode_compact_array_len(topic_result.results.len(), buf);
                for partition_result in &topic_result.results {
                    buf.put_i32(partition_result.partition_index);
                    buf.put_i16(partition_result.error_code);
                    // Empty tagged fields for each partition result
                    buf.put_u8(0);
                }
                // Empty tagged fields for each topic result
                buf.put_u8(0);
            }
            // Empty tagged fields in response body
            buf.put_u8(0);
        } else {
            // Non-flexible encoding: standard int32 array lengths
            buf.put_i32(response.results.len() as i32);
            for topic_result in &response.results {
                Self::encode_string(&topic_result.name, buf);
                buf.put_i32(topic_result.results.len() as i32);
                for partition_result in &topic_result.results {
                    buf.put_i32(partition_result.partition_index);
                    buf.put_i16(partition_result.error_code);
                }
            }
        }
        Ok(())
    }

    /// Encode AddOffsetsToTxn response (API Key 25)
    fn encode_add_offsets_to_txn_response(
        buf: &mut BytesMut,
        response: &KafkaAddOffsetsToTxnResponse,
    ) -> Result<()> {
        buf.put_i32(response.correlation_id);
        buf.put_i32(response.throttle_time_ms);
        buf.put_i16(response.error_code);
        Ok(())
    }

    /// Encode EndTxn response (API Key 26)
    /// Flexible versions: 3+ (uses compact encoding with tagged fields)
    fn encode_end_txn_response(
        buf: &mut BytesMut,
        response: &KafkaEndTxnResponse,
    ) -> Result<()> {
        let is_flexible = response.api_version >= 3;

        // Response header
        buf.put_i32(response.correlation_id);

        // For flexible versions (v3+), add response header tagged fields
        if is_flexible {
            buf.put_u8(0); // Empty tagged fields in response header
        }

        // Response body
        buf.put_i32(response.throttle_time_ms);
        buf.put_i16(response.error_code);

        // For flexible versions (v3+), add response body tagged fields
        if is_flexible {
            buf.put_u8(0); // Empty tagged fields in response body
        }

        Ok(())
    }

    /// Encode WriteTxnMarkers response (API Key 27)
    fn encode_write_txn_markers_response(
        buf: &mut BytesMut,
        response: &KafkaWriteTxnMarkersResponse,
    ) -> Result<()> {
        buf.put_i32(response.correlation_id);
        // Encode markers array
        buf.put_i32(response.markers.len() as i32);
        for marker_result in &response.markers {
            buf.put_i64(marker_result.producer_id);
            // Encode topics array
            buf.put_i32(marker_result.topics.len() as i32);
            for topic_result in &marker_result.topics {
                Self::encode_string(&topic_result.name, buf);
                // Encode partition results array
                buf.put_i32(topic_result.partitions.len() as i32);
                for partition_result in &topic_result.partitions {
                    buf.put_i32(partition_result.partition_index);
                    buf.put_i16(partition_result.error_code);
                }
            }
        }
        Ok(())
    }

    /// Encode TxnOffsetCommit response (API Key 28)
    fn encode_txn_offset_commit_response(
        buf: &mut BytesMut,
        response: &KafkaTxnOffsetCommitResponse,
    ) -> Result<()> {
        buf.put_i32(response.correlation_id);
        buf.put_i32(response.throttle_time_ms);
        // Encode topics array
        buf.put_i32(response.topics.len() as i32);
        for topic_result in &response.topics {
            Self::encode_string(&topic_result.name, buf);
            // Encode partition results array
            buf.put_i32(topic_result.partitions.len() as i32);
            for partition_result in &topic_result.partitions {
                buf.put_i32(partition_result.partition_index);
                buf.put_i16(partition_result.error_code);
            }
        }
        Ok(())
    }

    // ========================================================================
    // TRANSACTION API REQUEST DECODERS
    // ========================================================================

    /// Decode InitProducerId request (API Key 22)
    /// Supports v0-v4 (v2+ are flexible versions with compact strings)
    fn decode_init_producer_id_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<InitProducerIdRequest> {
        // v2+ uses flexible encoding (compact strings + tagged fields)
        let is_flexible = header.api_version >= 2;

        #[cfg(debug_assertions)]
        debug!("decode_init_producer_id_request: version={}, is_flexible={}, remaining={}",
               header.api_version, is_flexible, cursor.remaining());

        // Transactional ID (nullable string)
        let transactional_id = if is_flexible {
            #[cfg(debug_assertions)]
            debug!("Decoding compact string for transactional_id, remaining bytes: {}", cursor.remaining());
            Self::decode_compact_string(cursor)?
        } else {
            Self::decode_nullable_string(cursor)?
        };

        #[cfg(debug_assertions)]
        debug!("Decoded transactional_id: {:?}", transactional_id);

        // Transaction timeout (ms)
        let transaction_timeout_ms = cursor.get_i32();

        // Producer ID (v3+)
        let producer_id = if header.api_version >= 3 {
            cursor.get_i64()
        } else {
            -1
        };

        // Producer epoch (v3+)
        let producer_epoch = if header.api_version >= 3 {
            cursor.get_i16()
        } else {
            -1
        };

        // Note: In v4+, enable_2pc and keep_prepared_txn are in tagged fields, not fixed fields
        // Skip tagged fields for flexible versions (this includes v4+ optional fields)
        if is_flexible {
            let _ = Self::decode_tagged_fields(cursor);
        }

        Ok(InitProducerIdRequest {
            header,
            transactional_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        })
    }

    /// Decode AddPartitionsToTxn request (API Key 24)
    fn decode_add_partitions_to_txn_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<AddPartitionsToTxnRequest> {
        let is_flexible = header.api_version >= 3;

        // Transactional ID
        let transactional_id = if is_flexible {
            Self::decode_compact_string(cursor)?.unwrap_or_default()
        } else {
            Self::decode_string(cursor)?
        };

        // Producer ID
        let producer_id = cursor.get_i64();

        // Producer epoch
        let producer_epoch = cursor.get_i16();

        // Topics array
        let topics_len = if is_flexible {
            (Self::decode_varint(cursor)? as i32) - 1
        } else {
            cursor.get_i32()
        };

        let mut topics = Vec::with_capacity(topics_len.max(0) as usize);
        for _ in 0..topics_len {
            let name = if is_flexible {
                Self::decode_compact_string(cursor)?.unwrap_or_default()
            } else {
                Self::decode_string(cursor)?
            };

            let partitions_len = if is_flexible {
                (Self::decode_varint(cursor)? as i32) - 1
            } else {
                cursor.get_i32()
            };

            let mut partitions = Vec::with_capacity(partitions_len.max(0) as usize);
            for _ in 0..partitions_len {
                partitions.push(cursor.get_i32());
            }

            // Skip tagged fields for each topic in flexible version
            if is_flexible {
                let _ = Self::decode_tagged_fields(cursor);
            }

            topics.push(AddPartitionsToTxnTopic { name, partitions });
        }

        // Skip tagged fields for flexible versions
        if is_flexible {
            let _ = Self::decode_tagged_fields(cursor);
        }

        Ok(AddPartitionsToTxnRequest {
            header,
            transactional_id,
            producer_id,
            producer_epoch,
            topics,
        })
    }

    /// Decode AddOffsetsToTxn request (API Key 25)
    fn decode_add_offsets_to_txn_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<AddOffsetsToTxnRequest> {
        let is_flexible = header.api_version >= 3;

        // Transactional ID
        let transactional_id = if is_flexible {
            Self::decode_compact_string(cursor)?.unwrap_or_default()
        } else {
            Self::decode_string(cursor)?
        };

        // Producer ID
        let producer_id = cursor.get_i64();

        // Producer epoch
        let producer_epoch = cursor.get_i16();

        // Group ID
        let group_id = if is_flexible {
            Self::decode_compact_string(cursor)?.unwrap_or_default()
        } else {
            Self::decode_string(cursor)?
        };

        // Skip tagged fields for flexible versions
        if is_flexible {
            let _ = Self::decode_tagged_fields(cursor);
        }

        Ok(AddOffsetsToTxnRequest {
            header,
            transactional_id,
            producer_id,
            producer_epoch,
            group_id,
        })
    }

    /// Decode EndTxn request (API Key 26)
    fn decode_end_txn_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<EndTxnRequest> {
        let is_flexible = header.api_version >= 3;

        // Transactional ID
        let transactional_id = if is_flexible {
            Self::decode_compact_string(cursor)?.unwrap_or_default()
        } else {
            Self::decode_string(cursor)?
        };

        // Producer ID
        let producer_id = cursor.get_i64();

        // Producer epoch
        let producer_epoch = cursor.get_i16();

        // Committed (transaction result: true=commit, false=abort)
        let committed = cursor.get_u8() != 0;

        // Skip tagged fields for flexible versions
        if is_flexible {
            let _ = Self::decode_tagged_fields(cursor);
        }

        Ok(EndTxnRequest {
            header,
            transactional_id,
            producer_id,
            producer_epoch,
            committed,
        })
    }

    /// Decode WriteTxnMarkers request (API Key 27)
    fn decode_write_txn_markers_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<WriteTxnMarkersRequest> {
        let is_flexible = header.api_version >= 1;

        // Markers array
        let markers_len = if is_flexible {
            (Self::decode_varint(cursor)? as i32) - 1
        } else {
            cursor.get_i32()
        };

        let mut markers = Vec::with_capacity(markers_len.max(0) as usize);
        for _ in 0..markers_len {
            let producer_id = cursor.get_i64();
            let producer_epoch = cursor.get_i16();
            let coordinator_epoch = cursor.get_i32();
            let transaction_result = cursor.get_u8() != 0;

            // Topics array
            let topics_len = if is_flexible {
                (Self::decode_varint(cursor)? as i32) - 1
            } else {
                cursor.get_i32()
            };

            let mut topics = Vec::with_capacity(topics_len.max(0) as usize);
            for _ in 0..topics_len {
                let name = if is_flexible {
                    Self::decode_compact_string(cursor)?.unwrap_or_default()
                } else {
                    Self::decode_string(cursor)?
                };

                let partitions_len = if is_flexible {
                    (Self::decode_varint(cursor)? as i32) - 1
                } else {
                    cursor.get_i32()
                };

                let mut partitions = Vec::with_capacity(partitions_len.max(0) as usize);
                for _ in 0..partitions_len {
                    partitions.push(cursor.get_i32());
                }

                if is_flexible {
                    let _ = Self::decode_tagged_fields(cursor);
                }

                topics.push(WritableTxnMarkerTopic { name, partitions });
            }

            if is_flexible {
                let _ = Self::decode_tagged_fields(cursor);
            }

            markers.push(WritableTxnMarker {
                producer_id,
                producer_epoch,
                coordinator_epoch,
                transaction_result,
                topics,
            });
        }

        if is_flexible {
            let _ = Self::decode_tagged_fields(cursor);
        }

        Ok(WriteTxnMarkersRequest {
            header,
            markers,
        })
    }

    /// Decode TxnOffsetCommit request (API Key 28)
    fn decode_txn_offset_commit_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<TxnOffsetCommitRequest> {
        let is_flexible = header.api_version >= 3;

        // Transactional ID
        let transactional_id = if is_flexible {
            Self::decode_compact_string(cursor)?.unwrap_or_default()
        } else {
            Self::decode_string(cursor)?
        };

        // Group ID
        let group_id = if is_flexible {
            Self::decode_compact_string(cursor)?.unwrap_or_default()
        } else {
            Self::decode_string(cursor)?
        };

        // Producer ID
        let producer_id = cursor.get_i64();

        // Producer epoch
        let producer_epoch = cursor.get_i16();

        // Generation ID (v3+)
        let generation_id = if header.api_version >= 3 {
            cursor.get_i32()
        } else {
            -1
        };

        // Member ID (v3+)
        let member_id = if header.api_version >= 3 {
            if is_flexible {
                Self::decode_compact_string(cursor)?.unwrap_or_default()
            } else {
                Self::decode_string(cursor)?
            }
        } else {
            String::new()
        };

        // Group instance ID (v3+, nullable)
        let group_instance_id = if header.api_version >= 3 {
            if is_flexible {
                Self::decode_compact_string(cursor)?
            } else {
                Self::decode_nullable_string(cursor)?
            }
        } else {
            None
        };

        // Topics array
        let topics_len = if is_flexible {
            (Self::decode_varint(cursor)? as i32) - 1
        } else {
            cursor.get_i32()
        };

        let mut topics = Vec::with_capacity(topics_len.max(0) as usize);
        for _ in 0..topics_len {
            let name = if is_flexible {
                Self::decode_compact_string(cursor)?.unwrap_or_default()
            } else {
                Self::decode_string(cursor)?
            };

            let partitions_len = if is_flexible {
                (Self::decode_varint(cursor)? as i32) - 1
            } else {
                cursor.get_i32()
            };

            let mut partitions = Vec::with_capacity(partitions_len.max(0) as usize);
            for _ in 0..partitions_len {
                let partition_index = cursor.get_i32();
                let committed_offset = cursor.get_i64();

                // Leader epoch (v2+)
                let committed_leader_epoch = if header.api_version >= 2 {
                    cursor.get_i32()
                } else {
                    -1
                };

                // Metadata (nullable)
                let committed_metadata = if is_flexible {
                    Self::decode_compact_string(cursor)?
                } else {
                    Self::decode_nullable_string(cursor)?
                };

                if is_flexible {
                    let _ = Self::decode_tagged_fields(cursor);
                }

                partitions.push(TxnOffsetCommitRequestPartition {
                    partition_index,
                    committed_offset,
                    committed_leader_epoch,
                    committed_metadata,
                });
            }

            if is_flexible {
                let _ = Self::decode_tagged_fields(cursor);
            }

            topics.push(TxnOffsetCommitRequestTopic { name, partitions });
        }

        if is_flexible {
            let _ = Self::decode_tagged_fields(cursor);
        }

        Ok(TxnOffsetCommitRequest {
            header,
            transactional_id,
            group_id,
            producer_id,
            producer_epoch,
            generation_id,
            member_id,
            group_instance_id,
            topics,
        })
    }
}

/// Frame codec that handles Kafka message framing (length-prefixed messages)
pub struct KafkaFrameCodec;

impl Decoder for KafkaFrameCodec {
    type Item = Bytes;
    type Error = KafkaCodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        if src.len() < 4 {
            // Need at least 4 bytes for message length
            return Ok(None);
        }

        // Peek at message length without consuming bytes
        let message_length = {
            let mut cursor = Cursor::new(src.as_ref());
            cursor.get_i32()
        };

        if message_length < 0 || message_length > 100_000_000 {
            return Err(KafkaCodecError::InvalidFormat(format!(
                "Invalid message length: {}",
                message_length
            )));
        }

        let total_length = 4 + message_length as usize;

        if src.len() < total_length {
            // Don't have the full message yet
            return Ok(None);
        }

        // Extract the complete message including length prefix
        let mut full_message = src.split_to(total_length);
        // Skip the 4-byte length prefix to get just the Kafka message
        full_message.advance(4); // Remove first 4 bytes (length prefix)
        let message = full_message.freeze();
        #[cfg(debug_assertions)]
        debug!("KafkaFrameCodec: Decoded request without length prefix: {} bytes", message.len());
        Ok(Some(message))
    }
}

impl Encoder<Bytes> for KafkaFrameCodec {
    type Error = KafkaCodecError;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<()> {
        // CRITICAL FIX: Kafka protocol requires 4-byte length prefix before response body
        // Java client expects: [LENGTH][RESPONSE_BODY] format
        // Without this prefix, Java misreads the response structure
        let message_len = item.len() as u32;
        dst.put_u32(message_len);  // Big-endian 4-byte length prefix
        dst.extend_from_slice(&item);
        #[cfg(debug_assertions)]
        debug!("KafkaFrameCodec: Encoded response with length prefix: {} bytes total", message_len + 4);
        
        // DEBUG: Show exact hex bytes after framing (first 12 bytes)
        if dst.len() >= 12 {
            let first_12_bytes = &dst[0..12];
            #[cfg(debug_assertions)]
            debug!("  - FRAMED RESPONSE BYTES: [{:02x} {:02x} {:02x} {:02x}] [{:02x} {:02x} {:02x} {:02x}] [{:02x} {:02x} {:02x} {:02x}]",
                  first_12_bytes[0], first_12_bytes[1], first_12_bytes[2], first_12_bytes[3],
                  first_12_bytes[4], first_12_bytes[5], first_12_bytes[6], first_12_bytes[7], 
                  first_12_bytes[8], first_12_bytes[9], first_12_bytes[10], first_12_bytes[11]);
            #[cfg(debug_assertions)]
            debug!("  - LENGTH_PREFIX: {} = 0x{:08x}", message_len, message_len);
            #[cfg(debug_assertions)]
            debug!("  - ACTUAL_CORRELATION_ID_AT_BYTES_4-7: {} (this should be correlation_id=1)", 
                  i32::from_be_bytes([first_12_bytes[4], first_12_bytes[5], first_12_bytes[6], first_12_bytes[7]]));
        }
        Ok(())
    }
}

// ============================================================================
// Additional Admin API Encode Functions
// ============================================================================

impl KafkaCodec {
    // ========================================================================
    // DELETE RECORDS API (ApiKey = 21)
    // ========================================================================

    fn encode_delete_records_response(
        resp: &super::messages::KafkaDeleteRecordsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let is_flexible = resp.api_version >= 2;

        // Response header
        buf.put_i32(resp.header.correlation_id);
        if is_flexible {
            buf.put_u8(0); // Header tagged fields
        }

        // Throttle time
        buf.put_i32(resp.throttle_time_ms);

        // Topics array
        if is_flexible {
            Self::encode_compact_array_len(resp.topics.len(), buf);
            for topic in &resp.topics {
                Self::encode_compact_string(&topic.name, buf);

                // Partitions array (compact)
                Self::encode_compact_array_len(topic.partitions.len(), buf);
                for partition in &topic.partitions {
                    buf.put_i32(partition.partition_index);
                    buf.put_i64(partition.low_watermark);
                    buf.put_i16(partition.error_code);
                    // Partition tagged fields
                    buf.put_u8(0);
                }
                // Topic tagged fields
                buf.put_u8(0);
            }
            // Response-level tagged fields
            buf.put_u8(0);
        } else {
            buf.put_i32(resp.topics.len() as i32);
            for topic in &resp.topics {
                Self::encode_string(&topic.name, buf);

                // Partitions array
                buf.put_i32(topic.partitions.len() as i32);
                for partition in &topic.partitions {
                    buf.put_i32(partition.partition_index);
                    buf.put_i64(partition.low_watermark);
                    buf.put_i16(partition.error_code);
                }
            }
        }

        Ok(())
    }

    // ========================================================================
    // CREATE PARTITIONS API (ApiKey = 37)
    // ========================================================================

    fn encode_create_partitions_response(
        resp: &super::messages::KafkaCreatePartitionsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let is_flexible = resp.api_version >= 2;

        // Response header
        buf.put_i32(resp.header.correlation_id);
        if is_flexible {
            // Flexible header: tagged fields (empty = 0x00)
            buf.put_u8(0);
        }

        // Throttle time
        buf.put_i32(resp.throttle_time_ms);

        // Results array
        if is_flexible {
            Self::encode_compact_array_len(resp.results.len(), buf);
            for result in &resp.results {
                Self::encode_compact_string(&result.name, buf);
                buf.put_i16(result.error_code);
                Self::encode_compact_nullable_string(&result.error_message, buf);
                // Per-element tagged fields
                buf.put_u8(0);
            }
            // Response-level tagged fields
            buf.put_u8(0);
        } else {
            buf.put_i32(resp.results.len() as i32);
            for result in &resp.results {
                Self::encode_string(&result.name, buf);
                buf.put_i16(result.error_code);
                Self::encode_nullable_string(&result.error_message, buf);
            }
        }

        Ok(())
    }

    // ========================================================================
    // DELETE GROUPS API (ApiKey = 42)
    // ========================================================================

    fn encode_delete_groups_response(
        resp: &super::messages::KafkaDeleteGroupsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let is_flexible = resp.api_version >= 2;

        // Response header
        buf.put_i32(resp.header.correlation_id);
        if is_flexible {
            // Flexible header: tagged fields (empty = 0x00)
            buf.put_u8(0);
        }

        // Throttle time
        buf.put_i32(resp.throttle_time_ms);

        // Results array
        if is_flexible {
            Self::encode_compact_array_len(resp.results.len(), buf);
            for result in &resp.results {
                Self::encode_compact_string(&result.group_id, buf);
                buf.put_i16(result.error_code);
                // Per-element tagged fields
                buf.put_u8(0);
            }
            // Response-level tagged fields
            buf.put_u8(0);
        } else {
            buf.put_i32(resp.results.len() as i32);
            for result in &resp.results {
                Self::encode_string(&result.group_id, buf);
                buf.put_i16(result.error_code);
            }
        }

        Ok(())
    }

    // ========================================================================
    // ALTER PARTITION REASSIGNMENTS API (ApiKey = 45)
    // ========================================================================

    fn encode_alter_partition_reassignments_response(
        resp: &super::messages::KafkaAlterPartitionReassignmentsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // AlterPartitionReassignments is always flexible (v0+)

        // Response header with tagged fields
        buf.put_i32(resp.header.correlation_id);
        buf.put_u8(0); // Header tagged fields

        // Throttle time
        buf.put_i32(resp.throttle_time_ms);

        // Top-level error
        buf.put_i16(resp.error_code);
        Self::encode_compact_nullable_string(&resp.error_message, buf);

        // Responses array (compact)
        Self::encode_compact_array_len(resp.responses.len(), buf);
        for topic_resp in &resp.responses {
            Self::encode_compact_string(&topic_resp.name, buf);

            // Partitions array (compact)
            Self::encode_compact_array_len(topic_resp.partitions.len(), buf);
            for partition in &topic_resp.partitions {
                buf.put_i32(partition.partition_index);
                buf.put_i16(partition.error_code);
                Self::encode_compact_nullable_string(&partition.error_message, buf);
                // Partition tagged fields
                buf.put_u8(0);
            }
            // Topic tagged fields
            buf.put_u8(0);
        }
        // Response-level tagged fields
        buf.put_u8(0);

        Ok(())
    }

    // ========================================================================
    // LEADER_AND_ISR API (ApiKey = 4)
    // ========================================================================

    fn encode_leader_and_isr_response(
        resp: &super::messages::KafkaLeaderAndIsrResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // Response header
        buf.put_i32(resp.header.correlation_id);

        // Error code
        buf.put_i16(resp.error_code);

        // Partitions array
        buf.put_i32(resp.partitions.len() as i32);
        for partition in &resp.partitions {
            Self::encode_string(&partition.topic, buf);
            buf.put_i32(partition.partition);
            buf.put_i16(partition.error_code);
        }

        Ok(())
    }

    // ========================================================================
    // STOP_REPLICA API (ApiKey = 5)
    // ========================================================================

    fn encode_stop_replica_response(
        resp: &super::messages::KafkaStopReplicaResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // Response header
        buf.put_i32(resp.header.correlation_id);

        // Error code
        buf.put_i16(resp.error_code);

        // Partition errors array
        buf.put_i32(resp.partition_errors.len() as i32);
        for partition_error in &resp.partition_errors {
            Self::encode_string(&partition_error.topic, buf);
            buf.put_i32(partition_error.partition);
            buf.put_i16(partition_error.error_code);
        }

        Ok(())
    }

    // ========================================================================
    // UPDATE_METADATA API (ApiKey = 6)
    // ========================================================================

    fn encode_update_metadata_response(
        resp: &super::messages::KafkaUpdateMetadataResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // Response header
        buf.put_i32(resp.header.correlation_id);

        // Error code (only field in response)
        buf.put_i16(resp.error_code);

        Ok(())
    }

    // ========================================================================
    // CONTROLLED_SHUTDOWN API (ApiKey = 7)
    // ========================================================================

    fn encode_controlled_shutdown_response(
        resp: &super::messages::KafkaControlledShutdownResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // Response header
        buf.put_i32(resp.header.correlation_id);

        // Error code
        buf.put_i16(resp.error_code);

        // Remaining partitions array
        buf.put_i32(resp.partitions_remaining.len() as i32);
        for partition in &resp.partitions_remaining {
            Self::encode_string(&partition.topic, buf);
            buf.put_i32(partition.partition);
        }

        Ok(())
    }

    // ==================== DeleteRecords API (21) ====================

    fn decode_delete_records_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaDeleteRecordsRequest> {
        let api_version = header.api_version;

        // Topics array
        let topics = if api_version >= 2 {
            // Flexible version - compact array
            let num_topics = match Self::decode_compact_array_len(cursor)? {
                Some(n) => n,
                None => 0,
            };
            let mut topics = Vec::with_capacity(num_topics.min(1000));
            for _ in 0..num_topics {
                let topic_name = Self::decode_compact_non_nullable_string(cursor)?;
                let num_partitions = match Self::decode_compact_array_len(cursor)? {
                    Some(n) => n,
                    None => 0,
                };
                let mut partitions = Vec::with_capacity(num_partitions.min(1000));
                for _ in 0..num_partitions {
                    if cursor.remaining() < 12 {
                        return Err(KafkaCodecError::InvalidFormat(
                            "DeleteRecords: not enough bytes for partition data".to_string()
                        ));
                    }
                    let partition = cursor.get_i32();
                    let offset = cursor.get_i64();
                    // Skip partition-level tagged fields in v2
                    Self::skip_tagged_fields(cursor)?;
                    partitions.push(KafkaDeleteRecordsPartition { partition, offset });
                }
                // Skip topic-level tagged fields in v2
                Self::skip_tagged_fields(cursor)?;
                topics.push(KafkaDeleteRecordsTopic {
                    name: topic_name,
                    partitions,
                });
            }
            topics
        } else {
            // Non-flexible version
            if cursor.remaining() < 4 {
                return Err(KafkaCodecError::InvalidFormat(
                    "DeleteRecords: not enough bytes for topics count".to_string()
                ));
            }
            let num_topics = cursor.get_i32() as usize;
            let mut topics = Vec::with_capacity(num_topics.min(1000));
            for _ in 0..num_topics {
                let topic_name = Self::decode_string(cursor)?;
                if cursor.remaining() < 4 {
                    return Err(KafkaCodecError::InvalidFormat(
                        "DeleteRecords: not enough bytes for partitions count".to_string()
                    ));
                }
                let num_partitions = cursor.get_i32() as usize;
                let mut partitions = Vec::with_capacity(num_partitions.min(1000));
                for _ in 0..num_partitions {
                    if cursor.remaining() < 12 {
                        return Err(KafkaCodecError::InvalidFormat(
                            "DeleteRecords: not enough bytes for partition data".to_string()
                        ));
                    }
                    let partition = cursor.get_i32();
                    let offset = cursor.get_i64();
                    partitions.push(KafkaDeleteRecordsPartition { partition, offset });
                }
                topics.push(KafkaDeleteRecordsTopic {
                    name: topic_name,
                    partitions,
                });
            }
            topics
        };

        // Timeout
        if cursor.remaining() < 4 {
            return Err(KafkaCodecError::InvalidFormat(
                "DeleteRecords: not enough bytes for timeout".to_string()
            ));
        }
        let timeout_ms = cursor.get_i32();

        // Skip request-level tagged fields in v2
        if api_version >= 2 {
            Self::skip_tagged_fields(cursor)?;
        }

        Ok(KafkaDeleteRecordsRequest {
            header,
            topics,
            timeout_ms,
        })
    }

    // ==================== CreatePartitions API (37) ====================

    fn decode_create_partitions_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaCreatePartitionsRequest> {
        let api_version = header.api_version;

        // Topics array
        let topics = if api_version >= 2 {
            // Flexible version - compact array
            let num_topics = match Self::decode_compact_array_len(cursor)? {
                Some(n) => n,
                None => 0,
            };
            let mut topics = Vec::with_capacity(num_topics.min(1000));
            for _ in 0..num_topics {
                let name = Self::decode_compact_non_nullable_string(cursor)?;
                if cursor.remaining() < 4 {
                    return Err(KafkaCodecError::InvalidFormat(
                        "CreatePartitions: not enough bytes for count".to_string()
                    ));
                }
                let count = cursor.get_i32();
                // Assignment (nullable compact array)
                let assignments = match Self::decode_compact_array_len(cursor)? {
                    Some(num_assignments) if num_assignments > 0 => {
                        let mut assigns = Vec::with_capacity(num_assignments.min(1000));
                        for _ in 0..num_assignments {
                            let num_broker_ids = match Self::decode_compact_array_len(cursor)? {
                                Some(n) => n,
                                None => 0,
                            };
                            let mut broker_ids = Vec::with_capacity(num_broker_ids.min(100));
                            for _ in 0..num_broker_ids {
                                if cursor.remaining() < 4 {
                                    return Err(KafkaCodecError::InvalidFormat(
                                        "CreatePartitions: not enough bytes for broker_id".to_string()
                                    ));
                                }
                                broker_ids.push(cursor.get_i32());
                            }
                            Self::skip_tagged_fields(cursor)?;
                            assigns.push(broker_ids);
                        }
                        Some(assigns)
                    }
                    _ => None,
                };
                Self::skip_tagged_fields(cursor)?;
                topics.push(KafkaCreatePartitionsTopic {
                    name,
                    count,
                    assignments,
                });
            }
            topics
        } else {
            // Non-flexible version
            if cursor.remaining() < 4 {
                return Err(KafkaCodecError::InvalidFormat(
                    "CreatePartitions: not enough bytes for topics count".to_string()
                ));
            }
            let num_topics = cursor.get_i32() as usize;
            let mut topics = Vec::with_capacity(num_topics.min(1000));
            for _ in 0..num_topics {
                let name = Self::decode_string(cursor)?;
                if cursor.remaining() < 8 {
                    return Err(KafkaCodecError::InvalidFormat(
                        "CreatePartitions: not enough bytes for count and assignments".to_string()
                    ));
                }
                let count = cursor.get_i32();
                // Assignment (nullable array)
                let num_assignments = cursor.get_i32();
                let assignments = if num_assignments >= 0 {
                    let mut assigns = Vec::with_capacity((num_assignments as usize).min(1000));
                    for _ in 0..num_assignments {
                        if cursor.remaining() < 4 {
                            return Err(KafkaCodecError::InvalidFormat(
                                "CreatePartitions: not enough bytes for broker_ids count".to_string()
                            ));
                        }
                        let num_broker_ids = cursor.get_i32() as usize;
                        let mut broker_ids = Vec::with_capacity(num_broker_ids.min(100));
                        for _ in 0..num_broker_ids {
                            if cursor.remaining() < 4 {
                                return Err(KafkaCodecError::InvalidFormat(
                                    "CreatePartitions: not enough bytes for broker_id".to_string()
                                ));
                            }
                            broker_ids.push(cursor.get_i32());
                        }
                        assigns.push(broker_ids);
                    }
                    Some(assigns)
                } else {
                    None
                };
                topics.push(KafkaCreatePartitionsTopic {
                    name,
                    count,
                    assignments,
                });
            }
            topics
        };

        // Timeout and validate_only
        if cursor.remaining() < 5 {
            return Err(KafkaCodecError::InvalidFormat(
                "CreatePartitions: not enough bytes for timeout and validate_only".to_string()
            ));
        }
        let timeout_ms = cursor.get_i32();
        let validate_only = cursor.get_u8() != 0;

        // Skip request-level tagged fields in v2+
        if api_version >= 2 {
            Self::skip_tagged_fields(cursor)?;
        }

        Ok(KafkaCreatePartitionsRequest {
            header,
            topics,
            timeout_ms,
            validate_only,
        })
    }

    // ==================== DeleteGroups API (42) ====================

    fn decode_delete_groups_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaDeleteGroupsRequest> {
        let api_version = header.api_version;

        // Groups array
        let groups = if api_version >= 2 {
            // Flexible version - compact array
            let num_groups = match Self::decode_compact_array_len(cursor)? {
                Some(n) => n,
                None => 0,
            };
            let mut groups = Vec::with_capacity(num_groups.min(1000));
            for _ in 0..num_groups {
                groups.push(Self::decode_compact_non_nullable_string(cursor)?);
            }
            // Skip request-level tagged fields
            Self::skip_tagged_fields(cursor)?;
            groups
        } else {
            // Non-flexible version
            if cursor.remaining() < 4 {
                return Err(KafkaCodecError::InvalidFormat(
                    "DeleteGroups: not enough bytes for groups count".to_string()
                ));
            }
            let num_groups = cursor.get_i32() as usize;
            let mut groups = Vec::with_capacity(num_groups.min(1000));
            for _ in 0..num_groups {
                groups.push(Self::decode_string(cursor)?);
            }
            groups
        };

        Ok(KafkaDeleteGroupsRequest { header, groups })
    }

    // ==================== AlterPartitionReassignments API (45) ====================

    fn decode_alter_partition_reassignments_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaAlterPartitionReassignmentsRequest> {
        // Timeout
        if cursor.remaining() < 4 {
            return Err(KafkaCodecError::InvalidFormat(
                "AlterPartitionReassignments: not enough bytes for timeout".to_string()
            ));
        }
        let timeout_ms = cursor.get_i32();

        // Topics array (always flexible for v0)
        let num_topics = match Self::decode_compact_array_len(cursor)? {
            Some(n) => n,
            None => 0,
        };
        let mut topics = Vec::with_capacity(num_topics.min(1000));
        for _ in 0..num_topics {
            let name = Self::decode_compact_non_nullable_string(cursor)?;
            let num_partitions = match Self::decode_compact_array_len(cursor)? {
                Some(n) => n,
                None => 0,
            };
            let mut partitions = Vec::with_capacity(num_partitions.min(1000));
            for _ in 0..num_partitions {
                if cursor.remaining() < 4 {
                    return Err(KafkaCodecError::InvalidFormat(
                        "AlterPartitionReassignments: not enough bytes for partition_index".to_string()
                    ));
                }
                let partition_index = cursor.get_i32();
                // Replicas (nullable compact array) - use the same decode function
                let replicas = match Self::decode_compact_array_len(cursor)? {
                    Some(count) => {
                        let mut reps = Vec::with_capacity(count.min(100));
                        for _ in 0..count {
                            if cursor.remaining() < 4 {
                                return Err(KafkaCodecError::InvalidFormat(
                                    "AlterPartitionReassignments: not enough bytes for replica".to_string()
                                ));
                            }
                            reps.push(cursor.get_i32());
                        }
                        Some(reps)
                    }
                    None => None,
                };
                Self::skip_tagged_fields(cursor)?;
                partitions.push(KafkaReassignablePartition {
                    partition_index,
                    replicas,
                });
            }
            Self::skip_tagged_fields(cursor)?;
            topics.push(KafkaReassignableTopic { name, partitions });
        }

        // Skip request-level tagged fields
        Self::skip_tagged_fields(cursor)?;

        Ok(KafkaAlterPartitionReassignmentsRequest {
            header,
            timeout_ms,
            topics,
        })
    }

    // ========================================================================
    // LIST PARTITION REASSIGNMENTS API (Key = 46)
    // ========================================================================

    fn decode_list_partition_reassignments_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaListPartitionReassignmentsRequest> {
        // This API is flexible from v0+
        let timeout_ms = cursor.get_i32();

        // Topics (nullable compact array)
        let topics = match Self::decode_compact_array_len(cursor)? {
            Some(topic_count) => {
                let mut topics = Vec::with_capacity(topic_count.min(1000));
                for _ in 0..topic_count {
                    let name = Self::decode_compact_string(cursor)?.unwrap_or_default();
                    // Partition indexes (compact array)
                    let partition_count = Self::decode_compact_array_len(cursor)?
                        .unwrap_or(0);
                    let mut partition_indexes = Vec::with_capacity(partition_count.min(10000));
                    for _ in 0..partition_count {
                        partition_indexes.push(cursor.get_i32());
                    }
                    Self::skip_tagged_fields(cursor)?;
                    topics.push(KafkaListPartitionReassignmentsTopic {
                        name,
                        partition_indexes,
                    });
                }
                Some(topics)
            }
            None => None,
        };

        Self::skip_tagged_fields(cursor)?;

        Ok(KafkaListPartitionReassignmentsRequest {
            header,
            timeout_ms,
            topics,
        })
    }

    fn encode_list_partition_reassignments_response(
        resp: &KafkaListPartitionReassignmentsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // Response header (flexible version)
        buf.put_i32(resp.header.correlation_id);
        buf.put_u8(0); // Empty tagged fields in header

        // ThrottleTimeMs
        buf.put_i32(resp.throttle_time_ms);
        // ErrorCode
        buf.put_i16(resp.error_code);
        // ErrorMessage (nullable compact string)
        Self::encode_compact_nullable_string(&resp.error_message, buf);

        // Topics (compact array)
        Self::encode_compact_array_len(resp.topics.len(), buf);
        for topic in &resp.topics {
            Self::encode_compact_string(&topic.name, buf);
            // Partitions (compact array)
            Self::encode_compact_array_len(topic.partitions.len(), buf);
            for partition in &topic.partitions {
                buf.put_i32(partition.partition_index);
                // Replicas (compact array)
                Self::encode_compact_array_len(partition.replicas.len(), buf);
                for replica in &partition.replicas {
                    buf.put_i32(*replica);
                }
                // AddingReplicas (compact array)
                Self::encode_compact_array_len(partition.adding_replicas.len(), buf);
                for replica in &partition.adding_replicas {
                    buf.put_i32(*replica);
                }
                // RemovingReplicas (compact array)
                Self::encode_compact_array_len(partition.removing_replicas.len(), buf);
                for replica in &partition.removing_replicas {
                    buf.put_i32(*replica);
                }
                buf.put_u8(0); // Tagged fields
            }
            buf.put_u8(0); // Tagged fields
        }
        buf.put_u8(0); // Tagged fields

        Ok(())
    }

    // ========================================================================
    // OFFSET DELETE API (Key = 47)
    // ========================================================================

    fn decode_offset_delete_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaOffsetDeleteRequest> {
        // This API is NOT flexible (flexibleVersions: none)
        let group_id = Self::decode_string(cursor)?;

        let topic_count = cursor.get_i32() as usize;
        let mut topics = Vec::with_capacity(topic_count.min(1000));
        for _ in 0..topic_count {
            let name = Self::decode_string(cursor)?;
            let partition_count = cursor.get_i32() as usize;
            let mut partitions = Vec::with_capacity(partition_count.min(10000));
            for _ in 0..partition_count {
                partitions.push(KafkaOffsetDeleteRequestPartition {
                    partition_index: cursor.get_i32(),
                });
            }
            topics.push(KafkaOffsetDeleteRequestTopic { name, partitions });
        }

        Ok(KafkaOffsetDeleteRequest {
            header,
            group_id,
            topics,
        })
    }

    fn encode_offset_delete_response(
        resp: &KafkaOffsetDeleteResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // Response header (non-flexible)
        buf.put_i32(resp.header.correlation_id);

        // ErrorCode
        buf.put_i16(resp.error_code);
        // ThrottleTimeMs
        buf.put_i32(resp.throttle_time_ms);

        // Topics (standard array)
        buf.put_i32(resp.topics.len() as i32);
        for topic in &resp.topics {
            Self::encode_string(&topic.name, buf);
            buf.put_i32(topic.partitions.len() as i32);
            for partition in &topic.partitions {
                buf.put_i32(partition.partition_index);
                buf.put_i16(partition.error_code);
            }
        }

        Ok(())
    }

    // ========================================================================
    // DESCRIBE CLUSTER API (Key = 60)
    // ========================================================================

    fn decode_describe_cluster_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaDescribeClusterRequest> {
        // This API is flexible from v0+
        let include_cluster_authorized_operations = cursor.get_u8() != 0;

        // EndpointType (v1+)
        let endpoint_type = if header.api_version >= 1 && cursor.remaining() >= 1 {
            cursor.get_i8()
        } else {
            1 // Default to brokers
        };

        Self::skip_tagged_fields(cursor)?;

        Ok(KafkaDescribeClusterRequest {
            header,
            include_cluster_authorized_operations,
            endpoint_type,
        })
    }

    fn encode_describe_cluster_response(
        resp: &KafkaDescribeClusterResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // Response header (flexible version)
        buf.put_i32(resp.header.correlation_id);
        buf.put_u8(0); // Empty tagged fields in header

        // ThrottleTimeMs
        buf.put_i32(resp.throttle_time_ms);
        // ErrorCode
        buf.put_i16(resp.error_code);
        // ErrorMessage (nullable compact string)
        Self::encode_compact_nullable_string(&resp.error_message, buf);

        // EndpointType (v1+)
        if resp.api_version >= 1 {
            buf.put_i8(resp.endpoint_type);
        }

        // ClusterId (compact string)
        Self::encode_compact_string(&resp.cluster_id, buf);
        // ControllerId
        buf.put_i32(resp.controller_id);

        // Brokers (compact array)
        Self::encode_compact_array_len(resp.brokers.len(), buf);
        for broker in &resp.brokers {
            buf.put_i32(broker.broker_id);
            Self::encode_compact_string(&broker.host, buf);
            buf.put_i32(broker.port);
            Self::encode_compact_nullable_string(&broker.rack, buf);
            buf.put_u8(0); // Tagged fields
        }

        // ClusterAuthorizedOperations
        buf.put_i32(resp.cluster_authorized_operations);

        buf.put_u8(0); // Tagged fields

        Ok(())
    }

    // ========================================================================
    // DESCRIBE PRODUCERS API (Key = 61)
    // ========================================================================

    fn decode_describe_producers_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaDescribeProducersRequest> {
        // This API is flexible from v0+
        let topic_count = Self::decode_compact_array_len(cursor)?.unwrap_or(0);
        let mut topics = Vec::with_capacity(topic_count.min(1000));

        for _ in 0..topic_count {
            let name = Self::decode_compact_string(cursor)?.unwrap_or_default();
            let partition_count = Self::decode_compact_array_len(cursor)?.unwrap_or(0);
            let mut partition_indexes = Vec::with_capacity(partition_count.min(10000));
            for _ in 0..partition_count {
                partition_indexes.push(cursor.get_i32());
            }
            Self::skip_tagged_fields(cursor)?;
            topics.push(KafkaDescribeProducersTopicRequest {
                name,
                partition_indexes,
            });
        }

        Self::skip_tagged_fields(cursor)?;

        Ok(KafkaDescribeProducersRequest { header, topics })
    }

    fn encode_describe_producers_response(
        resp: &KafkaDescribeProducersResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // Response header (flexible version)
        buf.put_i32(resp.header.correlation_id);
        buf.put_u8(0); // Empty tagged fields in header

        // ThrottleTimeMs
        buf.put_i32(resp.throttle_time_ms);

        // Topics (compact array)
        Self::encode_compact_array_len(resp.topics.len(), buf);
        for topic in &resp.topics {
            Self::encode_compact_string(&topic.name, buf);
            // Partitions (compact array)
            Self::encode_compact_array_len(topic.partitions.len(), buf);
            for partition in &topic.partitions {
                buf.put_i32(partition.partition_index);
                buf.put_i16(partition.error_code);
                Self::encode_compact_nullable_string(&partition.error_message, buf);
                // ActiveProducers (compact array)
                Self::encode_compact_array_len(partition.active_producers.len(), buf);
                for producer in &partition.active_producers {
                    buf.put_i64(producer.producer_id);
                    buf.put_i32(producer.producer_epoch);
                    buf.put_i32(producer.last_sequence);
                    buf.put_i64(producer.last_timestamp);
                    buf.put_i32(producer.coordinator_epoch);
                    buf.put_i64(producer.current_txn_start_offset);
                    buf.put_u8(0); // Tagged fields
                }
                buf.put_u8(0); // Tagged fields
            }
            buf.put_u8(0); // Tagged fields
        }
        buf.put_u8(0); // Tagged fields

        Ok(())
    }

    // ========================================================================
    // PUSH TELEMETRY API (Key = 72)
    // ========================================================================

    fn decode_push_telemetry_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaPushTelemetryRequest> {
        // This API is flexible from v0+
        // ClientInstanceId (UUID = 16 bytes)
        let mut client_instance_id = [0u8; 16];
        if cursor.remaining() < 16 {
            return Err(KafkaCodecError::InvalidFormat(
                "PushTelemetry: not enough bytes for client_instance_id".to_string()
            ));
        }
        cursor.copy_to_slice(&mut client_instance_id);

        // SubscriptionId
        let subscription_id = cursor.get_i32();
        // Terminating
        let terminating = cursor.get_u8() != 0;
        // CompressionType
        let compression_type = cursor.get_i8();

        // Metrics (compact bytes)
        let metrics_len = Self::decode_varint(cursor)? as usize;
        let actual_len = if metrics_len > 0 { metrics_len - 1 } else { 0 };
        let metrics = if actual_len > 0 && cursor.remaining() >= actual_len {
            let mut data = vec![0u8; actual_len];
            cursor.copy_to_slice(&mut data);
            Bytes::from(data)
        } else {
            Bytes::new()
        };

        Self::skip_tagged_fields(cursor)?;

        Ok(KafkaPushTelemetryRequest {
            header,
            client_instance_id,
            subscription_id,
            terminating,
            compression_type,
            metrics,
        })
    }

    fn encode_push_telemetry_response(
        resp: &KafkaPushTelemetryResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // Response header (flexible version)
        buf.put_i32(resp.header.correlation_id);
        buf.put_u8(0); // Empty tagged fields in header

        // ThrottleTimeMs
        buf.put_i32(resp.throttle_time_ms);
        // ErrorCode
        buf.put_i16(resp.error_code);

        buf.put_u8(0); // Tagged fields

        Ok(())
    }

    // ============================================================================
    // OFFSET FOR LEADER EPOCH API (ApiKey = 23)
    // ============================================================================

    fn decode_offset_for_leader_epoch_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaOffsetForLeaderEpochRequest> {
        let api_version = header.api_version;
        let flexible = api_version >= 4;

        // ReplicaId (v3+)
        let replica_id = if api_version >= 3 {
            cursor.get_i32()
        } else {
            -1
        };

        // Topics array
        let topics_count = if flexible {
            Self::decode_varint(cursor)? as i32 - 1
        } else {
            cursor.get_i32()
        };

        let mut topics = Vec::with_capacity(topics_count.max(0) as usize);
        for _ in 0..topics_count {
            let topic = if flexible {
                Self::decode_compact_string(cursor)?.unwrap_or_default()
            } else {
                Self::decode_string(cursor)?
            };

            let partitions_count = if flexible {
                Self::decode_varint(cursor)? as i32 - 1
            } else {
                cursor.get_i32()
            };

            let mut partitions = Vec::with_capacity(partitions_count.max(0) as usize);
            for _ in 0..partitions_count {
                let partition = cursor.get_i32();
                let current_leader_epoch = if api_version >= 2 {
                    cursor.get_i32()
                } else {
                    -1
                };
                let leader_epoch = cursor.get_i32();

                if flexible {
                    Self::skip_tagged_fields(cursor)?;
                }

                partitions.push(KafkaOffsetForLeaderEpochPartition {
                    partition,
                    current_leader_epoch,
                    leader_epoch,
                });
            }

            if flexible {
                Self::skip_tagged_fields(cursor)?;
            }

            topics.push(KafkaOffsetForLeaderEpochTopic { topic, partitions });
        }

        if flexible {
            Self::skip_tagged_fields(cursor)?;
        }

        Ok(KafkaOffsetForLeaderEpochRequest {
            header,
            replica_id,
            topics,
        })
    }

    fn encode_offset_for_leader_epoch_response(
        resp: &KafkaOffsetForLeaderEpochResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let api_version = resp.api_version;
        let flexible = api_version >= 4;

        // Response header
        buf.put_i32(resp.header.correlation_id);
        if flexible {
            buf.put_u8(0); // Empty tagged fields in header
        }

        // ThrottleTimeMs (v2+)
        if api_version >= 2 {
            buf.put_i32(resp.throttle_time_ms);
        }

        // Topics array
        if flexible {
            Self::encode_varint(buf, (resp.topics.len() + 1) as u64);
        } else {
            buf.put_i32(resp.topics.len() as i32);
        }

        for topic in &resp.topics {
            if flexible {
                Self::encode_compact_string(&topic.topic, buf);
            } else {
                Self::encode_string(&topic.topic, buf);
            }

            if flexible {
                Self::encode_varint(buf, (topic.partitions.len() + 1) as u64);
            } else {
                buf.put_i32(topic.partitions.len() as i32);
            }

            for partition in &topic.partitions {
                buf.put_i16(partition.error_code);
                buf.put_i32(partition.partition);
                if api_version >= 1 {
                    buf.put_i32(partition.leader_epoch);
                }
                buf.put_i64(partition.end_offset);

                if flexible {
                    buf.put_u8(0); // Tagged fields
                }
            }

            if flexible {
                buf.put_u8(0); // Tagged fields
            }
        }

        if flexible {
            buf.put_u8(0); // Tagged fields
        }

        Ok(())
    }

    // ============================================================================
    // DESCRIBE ACLS API (ApiKey = 29)
    // ============================================================================

    fn decode_describe_acls_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaDescribeAclsRequest> {
        let api_version = header.api_version;
        let flexible = api_version >= 2;

        let resource_type_filter = cursor.get_i8();

        let resource_name_filter = if flexible {
            Self::decode_compact_string(cursor)?
        } else {
            Self::decode_nullable_string(cursor)?
        };

        let pattern_type_filter = if api_version >= 1 {
            cursor.get_i8()
        } else {
            3 // MATCH by default
        };

        let principal_filter = if flexible {
            Self::decode_compact_string(cursor)?
        } else {
            Self::decode_nullable_string(cursor)?
        };

        let host_filter = if flexible {
            Self::decode_compact_string(cursor)?
        } else {
            Self::decode_nullable_string(cursor)?
        };

        let operation = cursor.get_i8();
        let permission_type = cursor.get_i8();

        if flexible {
            Self::skip_tagged_fields(cursor)?;
        }

        Ok(KafkaDescribeAclsRequest {
            header,
            resource_type_filter,
            resource_name_filter,
            pattern_type_filter,
            principal_filter,
            host_filter,
            operation,
            permission_type,
        })
    }

    fn encode_describe_acls_response(
        resp: &KafkaDescribeAclsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let api_version = resp.api_version;
        let flexible = api_version >= 2;

        // Response header
        buf.put_i32(resp.header.correlation_id);
        if flexible {
            buf.put_u8(0); // Empty tagged fields in header
        }

        buf.put_i32(resp.throttle_time_ms);
        buf.put_i16(resp.error_code);

        if flexible {
            Self::encode_compact_nullable_string(&resp.error_message, buf);
        } else {
            Self::encode_nullable_string(&resp.error_message, buf);
        }

        // Resources array
        if flexible {
            Self::encode_varint(buf, (resp.resources.len() + 1) as u64);
        } else {
            buf.put_i32(resp.resources.len() as i32);
        }

        for resource in &resp.resources {
            buf.put_i8(resource.resource_type);

            if flexible {
                Self::encode_compact_string(&resource.resource_name, buf);
            } else {
                Self::encode_string(&resource.resource_name, buf);
            }

            if api_version >= 1 {
                buf.put_i8(resource.pattern_type);
            }

            // ACLs array
            if flexible {
                Self::encode_varint(buf, (resource.acls.len() + 1) as u64);
            } else {
                buf.put_i32(resource.acls.len() as i32);
            }

            for acl in &resource.acls {
                if flexible {
                    Self::encode_compact_string(&acl.principal, buf);
                    Self::encode_compact_string(&acl.host, buf);
                } else {
                    Self::encode_string(&acl.principal, buf);
                    Self::encode_string(&acl.host, buf);
                }
                buf.put_i8(acl.operation);
                buf.put_i8(acl.permission_type);

                if flexible {
                    buf.put_u8(0); // Tagged fields
                }
            }

            if flexible {
                buf.put_u8(0); // Tagged fields
            }
        }

        if flexible {
            buf.put_u8(0); // Tagged fields
        }

        Ok(())
    }

    // ============================================================================
    // CREATE ACLS API (ApiKey = 30)
    // ============================================================================

    fn decode_create_acls_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaCreateAclsRequest> {
        let api_version = header.api_version;
        let flexible = api_version >= 2;

        let creations_count = if flexible {
            Self::decode_varint(cursor)? as i32 - 1
        } else {
            cursor.get_i32()
        };

        let mut creations = Vec::with_capacity(creations_count.max(0) as usize);
        for _ in 0..creations_count {
            let resource_type = cursor.get_i8();

            let resource_name = if flexible {
                Self::decode_compact_string(cursor)?.unwrap_or_default()
            } else {
                Self::decode_string(cursor)?
            };

            let pattern_type = if api_version >= 1 {
                cursor.get_i8()
            } else {
                3 // LITERAL by default
            };

            let principal = if flexible {
                Self::decode_compact_string(cursor)?.unwrap_or_default()
            } else {
                Self::decode_string(cursor)?
            };

            let host = if flexible {
                Self::decode_compact_string(cursor)?.unwrap_or_default()
            } else {
                Self::decode_string(cursor)?
            };

            let operation = cursor.get_i8();
            let permission_type = cursor.get_i8();

            if flexible {
                Self::skip_tagged_fields(cursor)?;
            }

            creations.push(KafkaAclCreation {
                resource_type,
                resource_name,
                pattern_type,
                principal,
                host,
                operation,
                permission_type,
            });
        }

        if flexible {
            Self::skip_tagged_fields(cursor)?;
        }

        Ok(KafkaCreateAclsRequest { header, creations })
    }

    fn encode_create_acls_response(
        resp: &KafkaCreateAclsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let api_version = resp.api_version;
        let flexible = api_version >= 2;

        // Response header
        buf.put_i32(resp.header.correlation_id);
        if flexible {
            buf.put_u8(0); // Empty tagged fields in header
        }

        buf.put_i32(resp.throttle_time_ms);

        // Results array
        if flexible {
            Self::encode_varint(buf, (resp.results.len() + 1) as u64);
        } else {
            buf.put_i32(resp.results.len() as i32);
        }

        for result in &resp.results {
            buf.put_i16(result.error_code);

            if flexible {
                Self::encode_compact_nullable_string(&result.error_message, buf);
            } else {
                Self::encode_nullable_string(&result.error_message, buf);
            }

            if flexible {
                buf.put_u8(0); // Tagged fields
            }
        }

        if flexible {
            buf.put_u8(0); // Tagged fields
        }

        Ok(())
    }

    // ============================================================================
    // DELETE ACLS API (ApiKey = 31)
    // ============================================================================

    fn decode_delete_acls_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaDeleteAclsRequest> {
        let api_version = header.api_version;
        let flexible = api_version >= 2;

        let filters_count = if flexible {
            Self::decode_varint(cursor)? as i32 - 1
        } else {
            cursor.get_i32()
        };

        let mut filters = Vec::with_capacity(filters_count.max(0) as usize);
        for _ in 0..filters_count {
            let resource_type_filter = cursor.get_i8();

            let resource_name_filter = if flexible {
                Self::decode_compact_string(cursor)?
            } else {
                Self::decode_nullable_string(cursor)?
            };

            let pattern_type_filter = if api_version >= 1 {
                cursor.get_i8()
            } else {
                3 // MATCH by default
            };

            let principal_filter = if flexible {
                Self::decode_compact_string(cursor)?
            } else {
                Self::decode_nullable_string(cursor)?
            };

            let host_filter = if flexible {
                Self::decode_compact_string(cursor)?
            } else {
                Self::decode_nullable_string(cursor)?
            };

            let operation = cursor.get_i8();
            let permission_type = cursor.get_i8();

            if flexible {
                Self::skip_tagged_fields(cursor)?;
            }

            filters.push(KafkaDeleteAclsFilter {
                resource_type_filter,
                resource_name_filter,
                pattern_type_filter,
                principal_filter,
                host_filter,
                operation,
                permission_type,
            });
        }

        if flexible {
            Self::skip_tagged_fields(cursor)?;
        }

        Ok(KafkaDeleteAclsRequest { header, filters })
    }

    fn encode_delete_acls_response(
        resp: &KafkaDeleteAclsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let api_version = resp.api_version;
        let flexible = api_version >= 2;

        // Response header
        buf.put_i32(resp.header.correlation_id);
        if flexible {
            buf.put_u8(0); // Empty tagged fields in header
        }

        buf.put_i32(resp.throttle_time_ms);

        // Filter results array
        if flexible {
            Self::encode_varint(buf, (resp.filter_results.len() + 1) as u64);
        } else {
            buf.put_i32(resp.filter_results.len() as i32);
        }

        for filter_result in &resp.filter_results {
            buf.put_i16(filter_result.error_code);

            if flexible {
                Self::encode_compact_nullable_string(&filter_result.error_message, buf);
            } else {
                Self::encode_nullable_string(&filter_result.error_message, buf);
            }

            // Matching ACLs array
            if flexible {
                Self::encode_varint(buf, (filter_result.matching_acls.len() + 1) as u64);
            } else {
                buf.put_i32(filter_result.matching_acls.len() as i32);
            }

            for acl in &filter_result.matching_acls {
                buf.put_i16(acl.error_code);

                if flexible {
                    Self::encode_compact_nullable_string(&acl.error_message, buf);
                } else {
                    Self::encode_nullable_string(&acl.error_message, buf);
                }

                buf.put_i8(acl.resource_type);

                if flexible {
                    Self::encode_compact_string(&acl.resource_name, buf);
                } else {
                    Self::encode_string(&acl.resource_name, buf);
                }

                if api_version >= 1 {
                    buf.put_i8(acl.pattern_type);
                }

                if flexible {
                    Self::encode_compact_string(&acl.principal, buf);
                    Self::encode_compact_string(&acl.host, buf);
                } else {
                    Self::encode_string(&acl.principal, buf);
                    Self::encode_string(&acl.host, buf);
                }

                buf.put_i8(acl.operation);
                buf.put_i8(acl.permission_type);

                if flexible {
                    buf.put_u8(0); // Tagged fields
                }
            }

            if flexible {
                buf.put_u8(0); // Tagged fields
            }
        }

        if flexible {
            buf.put_u8(0); // Tagged fields
        }

        Ok(())
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_string_encoding_decoding() {
        let mut buf = BytesMut::new();
        let test_string = "hello world";

        KafkaCodec::encode_string(test_string, &mut buf);

        let mut cursor = Cursor::new(buf.as_ref());
        let decoded = KafkaCodec::decode_string(&mut cursor).unwrap();

        assert_eq!(decoded, test_string);
    }

    #[test]
    fn test_nullable_string_encoding_decoding() {
        let mut buf = BytesMut::new();

        // Test non-null string
        KafkaCodec::encode_nullable_string(&Some("test".to_string()), &mut buf);
        // Test null string
        KafkaCodec::encode_nullable_string(&None, &mut buf);

        let mut cursor = Cursor::new(buf.as_ref());

        let decoded1 = KafkaCodec::decode_nullable_string(&mut cursor).unwrap();
        let decoded2 = KafkaCodec::decode_nullable_string(&mut cursor).unwrap();

        assert_eq!(decoded1, Some("test".to_string()));
        assert_eq!(decoded2, None);
    }

    #[test]
    fn test_bytes_encoding_decoding() {
        let mut buf = BytesMut::new();
        let test_bytes = Bytes::from("test data");

        KafkaCodec::encode_bytes(&test_bytes, &mut buf);

        let mut cursor = Cursor::new(buf.as_ref());
        let decoded = KafkaCodec::decode_bytes(&mut cursor).unwrap();

        assert_eq!(decoded, test_bytes);
    }
}
