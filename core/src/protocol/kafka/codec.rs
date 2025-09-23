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
use tracing::{debug, warn, info, error};

use super::messages::*;
use crate::protocol::kafka::{
    API_KEY_API_VERSIONS, API_KEY_CREATE_TOPICS, API_KEY_DELETE_TOPICS,
    API_KEY_DESCRIBE_GROUPS, API_KEY_FETCH, API_KEY_FIND_COORDINATOR,
    API_KEY_HEARTBEAT, API_KEY_JOIN_GROUP, API_KEY_LEAVE_GROUP, API_KEY_LIST_GROUPS,
    API_KEY_LIST_OFFSETS, API_KEY_METADATA, API_KEY_OFFSET_COMMIT, API_KEY_OFFSET_FETCH,
    API_KEY_PRODUCE, API_KEY_SYNC_GROUP,
    API_KEY_GET_TELEMETRY_SUBSCRIPTIONS,
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
        let api_version = cursor.get_i16();
        let correlation_id = cursor.get_i32();
        
        // JAVA CLIENT FIX: Handle flexible version headers for ApiVersions v3+
        // Java Kafka 4.1 clients use headerVersion=2 for ApiVersions v4
        let client_id = if api_key == API_KEY_API_VERSIONS && api_version >= 3 {
            // ApiVersions v3+ uses flexible version header (headerVersion=2)
            // This has compact strings (varint length encoding)
            debug!("Decoding flexible header for ApiVersions v{}", api_version);
            
            // Decode compact string for client_id
            match Self::decode_varint(&mut cursor) {
                Ok(varint_len) => {
                    if varint_len == 0 {
                        None
                    } else {
                        let actual_len = (varint_len - 1) as usize;
                        if cursor.remaining() >= actual_len {
                            let mut buf = vec![0u8; actual_len];
                            cursor.copy_to_slice(&mut buf);
                            String::from_utf8(buf).ok()
                        } else {
                            None
                        }
                    }
                }
                Err(_) => None,
            }
        } else {
            // Standard header with nullable string
            Self::decode_nullable_string(&mut cursor)?
        };
        
        // Skip tagged fields for flexible versions
        if api_key == API_KEY_API_VERSIONS && api_version >= 3 {
            // Read and skip tagged fields (usually 0)
            if cursor.remaining() > 0 {
                match Self::decode_varint(&mut cursor) {
                    Ok(num_tags) => {
                        debug!("Skipping {} tagged fields in header", num_tags);
                        // Usually 0, but if present, skip them
                        for _ in 0..num_tags {
                            // Skip tag and length
                            let _ = Self::decode_varint(&mut cursor);
                            if let Ok(tag_len) = Self::decode_varint(&mut cursor) {
                                // Defensive bounds checking before advancing
                                let advance_len = tag_len as usize;
                                if advance_len <= cursor.remaining() {
                                    cursor.advance(advance_len);
                                } else {
                                    debug!("Tagged field length {} exceeds remaining buffer {}, skipping to end", 
                                           advance_len, cursor.remaining());
                                    // Skip to end of buffer safely
                                    let remaining = cursor.remaining();
                                    if remaining > 0 {
                                        cursor.advance(remaining);
                                    }
                                    break; // Exit the loop as buffer is exhausted
                                }
                            }
                        }
                    }
                    Err(_) => {}
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
            _ => Err(KafkaCodecError::UnsupportedVersion(api_key, api_version)),
        }
    }

    /// Encode a Kafka response to bytes
    pub fn encode_response(response: &KafkaResponse) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        
        info!("🎯 encode_response: Starting with empty buffer");

        match response {
            KafkaResponse::Produce(resp) => Self::encode_produce_response(resp, &mut buf)?,
            KafkaResponse::Fetch(resp) => Self::encode_fetch_response(resp, &mut buf)?,
            KafkaResponse::Metadata(resp) => {
                info!("📝 encode_response: About to encode Metadata response");
                Self::encode_metadata_response(resp, &mut buf)?;
                info!("✅ encode_response: Metadata response encoded, buffer size: {}", buf.len());
                
                // DEBUG: Check first 12 bytes of encoded response
                if buf.len() >= 12 {
                    let first_12_bytes = &buf[0..12];
                    info!("📊 encode_response: First 12 bytes after Metadata encoding: [{:02x} {:02x} {:02x} {:02x}] [{:02x} {:02x} {:02x} {:02x}] [{:02x} {:02x} {:02x} {:02x}]",
                          first_12_bytes[0], first_12_bytes[1], first_12_bytes[2], first_12_bytes[3],
                          first_12_bytes[4], first_12_bytes[5], first_12_bytes[6], first_12_bytes[7], 
                          first_12_bytes[8], first_12_bytes[9], first_12_bytes[10], first_12_bytes[11]);
                }
            },
            KafkaResponse::OffsetCommit(resp) => {
                Self::encode_offset_commit_response(resp, &mut buf)?
            }
            KafkaResponse::OffsetFetch(resp) => Self::encode_offset_fetch_response(resp, &mut buf)?,
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
            KafkaResponse::SaslHandshake(resp) => {
                Self::encode_sasl_handshake_response(resp, &mut buf)?
            }
            KafkaResponse::SaslAuthenticate(resp) => {
                Self::encode_sasl_authenticate_response(resp, &mut buf)?
            }
            KafkaResponse::GetTelemetrySubscriptions(resp) => {
                // Simple telemetry response encoding
                // Header
                buf.put_i32(resp.header.correlation_id);

                // Body
                buf.put_i32(resp.throttle_time_ms);
                buf.put_i16(resp.error_code);
                buf.extend_from_slice(&resp.client_instance_id);
                buf.put_i32(resp.subscription_id);

                // Accepted compression types array
                buf.put_i32(resp.accepted_compression_types.len() as i32);
                for compression_type in &resp.accepted_compression_types {
                    buf.put_i8(*compression_type);
                }

                buf.put_i32(resp.push_interval_ms);
                buf.put_i32(resp.telemetry_max_bytes);
                buf.put_u8(if resp.delta_temporality { 1 } else { 0 });

                // Requested metrics array
                buf.put_i32(resp.requested_metrics.len() as i32);
                for metric in &resp.requested_metrics {
                    let metric_bytes = metric.as_bytes();
                    buf.put_i16(metric_bytes.len() as i16);
                    buf.extend_from_slice(metric_bytes);
                }
            }
            // Obsolete cluster management APIs (removed in Kafka 4.0)
            KafkaResponse::LeaderAndIsr(_) => {
                return Err(KafkaCodecError::UnsupportedApiKey(4)); // API key 4
            }
            KafkaResponse::StopReplica(_) => {
                return Err(KafkaCodecError::UnsupportedApiKey(5)); // API key 5
            }
            KafkaResponse::UpdateMetadata(_) => {
                return Err(KafkaCodecError::UnsupportedApiKey(6)); // API key 6
            }
            KafkaResponse::ControlledShutdown(_) => {
                return Err(KafkaCodecError::UnsupportedApiKey(7)); // API key 7
            }
            // Transaction APIs - placeholder error responses
            KafkaResponse::InitProducerId(_) => {
                return Err(KafkaCodecError::UnsupportedApiKey(22)); // API key 22
            }
            KafkaResponse::AddPartitionsToTxn(_) => {
                return Err(KafkaCodecError::UnsupportedApiKey(24)); // API key 24
            }
            KafkaResponse::AddOffsetsToTxn(_) => {
                return Err(KafkaCodecError::UnsupportedApiKey(25)); // API key 25
            }
            KafkaResponse::EndTxn(_) => {
                return Err(KafkaCodecError::UnsupportedApiKey(26)); // API key 26
            }
            KafkaResponse::WriteTxnMarkers(_) => {
                return Err(KafkaCodecError::UnsupportedApiKey(27)); // API key 27
            }
            KafkaResponse::TxnOffsetCommit(_) => {
                return Err(KafkaCodecError::UnsupportedApiKey(28)); // API key 28
            }
        }

        // CRITICAL FIX: Remove double encoding! 
        // KafkaFrameCodec already adds length prefix, so we don't need it here
        // This was causing [LENGTH_PREFIX][BUFFER_SIZE][CORRELATION_ID] instead of [LENGTH_PREFIX][CORRELATION_ID]
        
        info!("🔧 FIXED: Removed double length encoding, returning buffer directly");
        info!("🔧 Buffer size: {}, first 8 bytes should be correlation_id + throttle_time", buf.len());
        if buf.len() >= 8 {
            let first_8_bytes = &buf[0..8];
            info!("🔧 First 8 bytes: [{:02x} {:02x} {:02x} {:02x}] [{:02x} {:02x} {:02x} {:02x}]", 
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
        debug!("Decoding produce request: topic_count={}", topic_count);
        let mut topic_data = Vec::with_capacity(topic_count as usize);

        for i in 0..topic_count {
            let topic = Self::decode_string(cursor)?;
            debug!("Decoded topic {}: '{}'", i, topic);
            let partition_count = cursor.get_i32();
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
        buf.put_i32(response.header.correlation_id);
        buf.put_i32(response.responses.len() as i32);

        for topic_response in &response.responses {
            Self::encode_string(&topic_response.topic, buf);
            buf.put_i32(topic_response.partition_responses.len() as i32);

            for partition_response in &topic_response.partition_responses {
                buf.put_i32(partition_response.partition);
                buf.put_i16(partition_response.error_code);
                buf.put_i64(partition_response.base_offset);
                buf.put_i64(partition_response.log_append_time_ms);
                buf.put_i64(partition_response.log_start_offset);
            }
        }

        buf.put_i32(response.throttle_time_ms);
        Ok(())
    }

    // ========================================================================
    // FETCH REQUEST/RESPONSE
    // ========================================================================

    fn decode_fetch_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaFetchRequest> {
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
        // TODO: Use the actual API version from the request. For now, try using a higher version
        let api_version = 10i16; // Use v10 to match consumer's expected API version
        
        buf.put_i32(response.header.correlation_id);
        
        // throttle_time_ms is the first field of the response body in v1+
        if api_version >= 1 {
            buf.put_i32(response.throttle_time_ms);
        }
        
        // No global error_code in v1, and no session_id in v1
        // In v1, the structure is: ThrottleTime [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
        
        // session_id only in v7+, not present in v1
        if api_version >= 7 {
            buf.put_i32(response.session_id);
        }
        
        // Topics array
        buf.put_i32(response.responses.len() as i32);

        for topic_response in &response.responses {
            Self::encode_string(&topic_response.topic, buf);
            buf.put_i32(topic_response.partitions.len() as i32);

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

                // aborted_transactions only in v4+
                if api_version >= 4 {
                    buf.put_i32(0); // Empty aborted transactions
                }

                // preferred_read_replica only in v11+
                if api_version >= 11 {
                    buf.put_i32(partition_response.preferred_read_replica);
                }
                
                if let Some(records) = &partition_response.records {
                    tracing::debug!("Encoding partition {} records: {} bytes (first 32 bytes: {:?}) for API v{}", 
                                  partition_response.partition, records.len(),
                                  &records[..std::cmp::min(32, records.len())], api_version);
                } else {
                    tracing::debug!("Encoding partition {} records: None for API v{}", partition_response.partition, api_version);
                }
                Self::encode_nullable_bytes(&partition_response.records, buf);
            }
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
        let replica_id = cursor.get_i32();
        let isolation_level = cursor.get_i8();
        let topic_count = cursor.get_i32();

        let mut topics = Vec::with_capacity(topic_count as usize);
        for _ in 0..topic_count {
            let topic = Self::decode_string(cursor)?;
            let partition_count = cursor.get_i32();
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
            topics.push(KafkaListOffsetsTopic { topic, partitions });
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
        debug!("Decoding Metadata request: api_version={}", header.api_version);
        debug!("  Cursor position: {}, remaining: {}", cursor.position(), cursor.remaining());
        
        // Debug: Show raw bytes around current cursor position
        let start_pos = cursor.position() as usize;
        let slice = cursor.get_ref();
        let debug_bytes = &slice[start_pos..std::cmp::min(start_pos + 20, slice.len())];
        debug!("  Raw bytes at cursor: {:02x?}", debug_bytes);
        
        // CRITICAL FIX: Handle flexible versions (v9+) vs non-flexible versions
        // CRITICAL FIX: Request parsing must match Java client's flexible format
        let is_flexible = header.api_version >= 9; // Java client sends flexible v9 requests
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
        
        debug!("  Topic count read: {} (flexible={})", topic_count, is_flexible);
        debug!("  Cursor after topic_count: pos={}, remaining={}", cursor.position(), cursor.remaining());
        let topics = if topic_count == -1 {
            debug!("  Topics: null (topic_count = -1)");
            None
        } else if topic_count == 0 {
            debug!("  Topics: empty array (topic_count = 0)");
            Some(Vec::new())
        } else {
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
                    debug!("    Topic {} (flexible): '{}'", i, topic_name);
                    topics.push(topic_name);
                    
                    // Skip tagged fields for this topic (just read the count)
                    let _tagged_fields_count = Self::decode_varint(cursor)?;
                } else {
                    // Non-flexible versions just have topic name as regular string
                    let topic_name = Self::decode_string(cursor)?;
                    debug!("    Topic {} (non-flexible): '{}'", i, topic_name);
                    topics.push(topic_name);
                }
            }
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
        info!("🔧 MetadataResponse encoding: version={}, flexible={}", response.api_version, is_flexible);
        
        debug!("Encoding Metadata response v{}, flexible={}", response.api_version, is_flexible);
        debug!("  - correlation_id: {}", response.header.correlation_id);
        debug!("  - throttle_time_ms: {}", response.throttle_time_ms);
        debug!("  - brokers count: {}", response.brokers.len());
        debug!("  - topics count: {}", response.topics.len());
        
        // CRITICAL FIX: Java Kafka client expects correlation_id at the FIRST position after length prefix
        // All Kafka responses must start with correlation_id as the first field
        // This is consistent across all Kafka APIs including Metadata v0-v11
        
        info!("🔍 BEFORE correlation_id encoding: buffer is empty, size={}", buf.len());
        buf.put_i32(response.header.correlation_id);
        info!("✅ AFTER correlation_id encoding: buffer size={}, correlation_id={}", buf.len(), response.header.correlation_id);
        // DEBUG: Show exact hex bytes at start of response
        if buf.len() >= 4 {
            let first_4_bytes = &buf[0..4];
            info!("  - CORRELATION_ID BYTES: [{:02x}, {:02x}, {:02x}, {:02x}] = {}", 
                  first_4_bytes[0], first_4_bytes[1], first_4_bytes[2], first_4_bytes[3], 
                  i32::from_be_bytes([first_4_bytes[0], first_4_bytes[1], first_4_bytes[2], first_4_bytes[3]]));
        }
        
        // throttle_time_ms position depends on version
        // v0-v2: NO throttle_time_ms field
        // v3+: throttle_time_ms after correlation_id (2nd field)
        if response.api_version >= 3 {
            buf.put_i32(response.throttle_time_ms);
            debug!("  - Wrote throttle_time_ms after correlation_id (v3+), buffer size: {}", buf.len());
        } else {
            debug!("  - Skipped throttle_time_ms (v0-v2 don't have it), buffer size: {}", buf.len());
        }

        // Brokers array - CRITICAL for Java Kafka 4.1 compatibility
        info!("  - CRITICAL: Encoding brokers array, count: {}", response.brokers.len());
        if response.brokers.is_empty() {
            error!("  - ERROR: brokers array is empty! This will cause Java client parsing failure");
        }
        // CRITICAL FIX: Kafka v9+ uses compact array encoding (unsigned varint)
        // Based on Kafka 4.1.0 source: _writable.writeUnsignedVarint(brokers.size() + 1)
        if is_flexible {
            Self::encode_varint(buf, (response.brokers.len() + 1) as u64);
            info!("  - KAFKA v9+ FIX: Using compact array encoding for brokers, varint length: {}", response.brokers.len() + 1);
        } else {
            buf.put_i32(response.brokers.len() as i32);
            info!("  - KAFKA v0-8: Using standard i32 array encoding for brokers, length: {}", response.brokers.len());
        }
        info!("  - Wrote brokers array length, buffer size: {}", buf.len());

        for (i, broker) in response.brokers.iter().enumerate() {
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
            debug!("    broker[{}] encoded, buffer size: {}", i, buf.len());
            
            // Tagged fields for broker (v9+)
            if is_flexible {
                Self::encode_empty_tagged_fields(buf);
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
        debug!("  - Encoding topics array, actual count: {}", response.topics.len());
        debug!("  - Topics in response: {:?}", response.topics.iter().map(|t| &t.topic).collect::<Vec<_>>());
        if is_flexible {
            // CRITICAL FIX: Use correct compact array encoding for topics  
            let array_len = response.topics.len();
            let encoded_len = array_len + 1;
            Self::encode_varint(buf, encoded_len as u64);
            info!("  - FIXED topics compact array: length={}, encoded as varint {} (Java will read {})", 
                  array_len, encoded_len, array_len);
        } else {
            buf.put_i32(response.topics.len() as i32);
        }
        debug!("  - Wrote topics array length, buffer size: {}", buf.len());

        for (i, topic) in response.topics.iter().enumerate() {
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
                debug!("    FIXED partitions compact array: length={}, encoded as varint {} (Java will read {})", 
                       array_len, encoded_len, array_len);
            } else {
                buf.put_i32(topic.partitions.len() as i32);
            }

            for (partition_idx, partition) in topic.partitions.iter().enumerate() {
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
            info!("  - Added response-level tagged fields, final buffer size: {}", buf.len());
        }
        
        // v2 does NOT have throttle_time_ms - removing incorrect implementation
        
        info!("Completed Metadata response encoding, total bytes: {}", buf.len());
        
        // DEBUG: Log first 100 bytes of response for Java client debugging
        if buf.len() >= 10 {
            let preview: Vec<u8> = buf[0..std::cmp::min(100, buf.len())].to_vec();
            info!("Response bytes (first 100): {:?}", preview);
        }
        
        // CRITICAL DEBUG: Show exact structure for Java client correlation_id analysis
        if buf.len() >= 20 {
            let first_20_bytes = &buf[0..20];
            info!("  - FULL METADATA STRUCTURE (20 bytes):");
            info!("    Bytes 00-03: [{:02x} {:02x} {:02x} {:02x}] = correlation_id: {}", 
                  first_20_bytes[0], first_20_bytes[1], first_20_bytes[2], first_20_bytes[3],
                  i32::from_be_bytes([first_20_bytes[0], first_20_bytes[1], first_20_bytes[2], first_20_bytes[3]]));
            info!("    Bytes 04-07: [{:02x} {:02x} {:02x} {:02x}] = throttle_time: {}", 
                  first_20_bytes[4], first_20_bytes[5], first_20_bytes[6], first_20_bytes[7],
                  i32::from_be_bytes([first_20_bytes[4], first_20_bytes[5], first_20_bytes[6], first_20_bytes[7]]));
            info!("    Byte 08: [{:02x}] = broker_count_varint: {}", first_20_bytes[8], first_20_bytes[8]);
            info!("    Bytes 09-12: [{:02x} {:02x} {:02x} {:02x}] = broker_node_id: {}", 
                  first_20_bytes[9], first_20_bytes[10], first_20_bytes[11], first_20_bytes[12],
                  i32::from_be_bytes([first_20_bytes[9], first_20_bytes[10], first_20_bytes[11], first_20_bytes[12]]));
            info!("    Byte 13: [{:02x}] = host_length_varint: {}", first_20_bytes[13], first_20_bytes[13]);
            info!("    Bytes 14-19: [{:02x} {:02x} {:02x} {:02x} {:02x} {:02x}] = host_prefix: '{}'", 
                  first_20_bytes[14], first_20_bytes[15], first_20_bytes[16], first_20_bytes[17], first_20_bytes[18], first_20_bytes[19],
                  String::from_utf8_lossy(&first_20_bytes[14..20]));
            
            // CRITICAL: Try to understand where Java reads correlation_id=680 (0x02A8)
            info!("  - CHECKING FOR 680 (0x02A8) IN RESPONSE:");
            for i in 0..=(buf.len().saturating_sub(4)) {
                if i + 3 < buf.len() {
                    let value = i32::from_be_bytes([buf[i], buf[i+1], buf[i+2], buf[i+3]]);
                    if value == 680 {
                        info!("    Found 680 at byte offset {}: [{:02x} {:02x} {:02x} {:02x}]", i, buf[i], buf[i+1], buf[i+2], buf[i+3]);
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
        buf.put_i32(response.header.correlation_id);
        buf.put_i32(response.throttle_time_ms);
        buf.put_i32(response.topics.len() as i32);

        for topic in &response.topics {
            Self::encode_string(&topic.topic, buf);
            buf.put_i32(topic.partitions.len() as i32);

            for partition in &topic.partitions {
                buf.put_i32(partition.partition);
                buf.put_i16(partition.error_code);
                buf.put_i64(partition.timestamp);
                buf.put_i64(partition.offset);
                buf.put_i32(partition.leader_epoch);
            }
        }

        Ok(())
    }

    // ========================================================================
    // OFFSET COMMIT REQUEST/RESPONSE
    // ========================================================================

    fn decode_offset_commit_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaOffsetCommitRequest> {
        let group_id = Self::decode_string(cursor)?;
        let generation_id = cursor.get_i32();
        let consumer_id = Self::decode_string(cursor)?;
        let retention_time_ms = cursor.get_i64();

        let topic_count = cursor.get_i32();
        let mut topics = Vec::with_capacity(topic_count as usize);

        for _ in 0..topic_count {
            let topic = Self::decode_string(cursor)?;
            let partition_count = cursor.get_i32();
            let mut partitions = Vec::with_capacity(partition_count as usize);

            for _ in 0..partition_count {
                let partition = cursor.get_i32();
                let offset = cursor.get_i64();
                let timestamp = cursor.get_i64();
                let metadata = Self::decode_nullable_string(cursor)?;

                partitions.push(KafkaOffsetCommitPartition {
                    partition,
                    offset,
                    timestamp,
                    metadata,
                });
            }

            topics.push(KafkaOffsetCommitTopic { topic, partitions });
        }

        Ok(KafkaOffsetCommitRequest {
            header,
            group_id,
            generation_id,
            consumer_id,
            retention_time_ms,
            topics,
        })
    }

    fn encode_offset_commit_response(
        response: &KafkaOffsetCommitResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        buf.put_i32(response.header.correlation_id);
        buf.put_i32(response.throttle_time_ms);
        buf.put_i32(response.topics.len() as i32);

        for topic in &response.topics {
            Self::encode_string(&topic.topic, buf);
            buf.put_i32(topic.partitions.len() as i32);

            for partition in &topic.partitions {
                buf.put_i32(partition.partition);
                buf.put_i16(partition.error_code);
            }
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
        buf: &mut BytesMut,
    ) -> Result<()> {
        buf.put_i32(response.header.correlation_id);
        buf.put_i32(response.throttle_time_ms);
        buf.put_i32(response.topics.len() as i32);

        for topic in &response.topics {
            Self::encode_string(&topic.topic, buf);
            buf.put_i32(topic.partitions.len() as i32);

            for partition in &topic.partitions {
                buf.put_i32(partition.partition);
                buf.put_i64(partition.offset);
                buf.put_i32(partition.leader_epoch);
                Self::encode_nullable_string(&partition.metadata, buf);
                buf.put_i16(partition.error_code);
            }
        }

        buf.put_i16(response.error_code);
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
        buf.put_i32(response.header.correlation_id);
        buf.put_i32(response.throttle_time_ms);
        buf.put_i16(response.error_code);
        Self::encode_nullable_string(&response.error_message, buf);
        buf.put_i32(response.node_id);
        Self::encode_string(&response.host, buf);
        buf.put_i32(response.port);
        Ok(())
    }

    // ========================================================================
    // LIST GROUPS REQUEST/RESPONSE
    // ========================================================================

    fn decode_list_groups_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaListGroupsRequest> {
        let states_filter_count = cursor.get_i32();
        let mut states_filter = Vec::with_capacity(states_filter_count as usize);

        for _ in 0..states_filter_count {
            states_filter.push(Self::decode_string(cursor)?);
        }

        Ok(KafkaListGroupsRequest {
            header,
            states_filter,
        })
    }

    fn encode_list_groups_response(
        response: &KafkaListGroupsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        buf.put_i32(response.header.correlation_id);
        buf.put_i32(response.throttle_time_ms);
        buf.put_i16(response.error_code);
        buf.put_i32(response.groups.len() as i32);

        for group in &response.groups {
            Self::encode_string(&group.group_id, buf);
            Self::encode_string(&group.protocol_type, buf);
            Self::encode_string(&group.group_state, buf);
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
        buf.put_i32(response.header.correlation_id);
        buf.put_i32(response.throttle_time_ms);
        buf.put_i16(response.error_code);
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
        buf.put_i32(response.header.correlation_id);
        buf.put_i32(response.throttle_time_ms);
        buf.put_i16(response.error_code);
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
        let generation_id = cursor.get_i32();
        let consumer_id = Self::decode_string(cursor)?;

        // group_instance_id is only present in API version 3 and above
        let group_instance_id = if header.api_version >= 3 {
            Self::decode_nullable_string(cursor)?
        } else {
            None
        };

        // protocol_type and protocol_name were added in later versions
        // In versions 0-2, these fields are not present
        let protocol_type = if header.api_version >= 3 {
            Self::decode_string(cursor)?
        } else {
            String::new()
        };

        let protocol_name = if header.api_version >= 3 {
            Self::decode_string(cursor)?
        } else {
            String::new()
        };

        let assignment_count = cursor.get_i32();
        let mut assignments = Vec::with_capacity(assignment_count as usize);

        for _ in 0..assignment_count {
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
        buf.put_i32(response.header.correlation_id);

        // throttle_time_ms is only present in version 1 and above
        if response.api_version >= 1 {
            buf.put_i32(response.throttle_time_ms);
        }

        buf.put_i16(response.error_code);

        // protocol_type and protocol_name were added in later versions
        // In versions 0-2, these fields are not present
        if response.api_version >= 3 {
            Self::encode_string(&response.protocol_type, buf);
            Self::encode_string(&response.protocol_name, buf);
        }

        Self::encode_bytes(&response.assignment, buf);
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
        buf.put_i32(response.header.correlation_id);
        buf.put_i32(response.throttle_time_ms);
        buf.put_i16(response.error_code);
        buf.put_i32(response.generation_id);
        Self::encode_string(&response.protocol_name, buf);
        Self::encode_string(&response.protocol_type, buf);
        Self::encode_string(&response.leader, buf);
        Self::encode_string(&response.member_id, buf);
        buf.put_i32(response.members.len() as i32);

        for member in &response.members {
            Self::encode_string(&member.member_id, buf);
            Self::encode_nullable_string(&member.group_instance_id, buf);
            Self::encode_bytes(&member.metadata, buf);
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
        let group_count = cursor.get_i32();
        let mut groups = Vec::with_capacity(group_count as usize);

        for _ in 0..group_count {
            groups.push(Self::decode_string(cursor)?);
        }

        let include_authorized_operations = cursor.get_i8() != 0;

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
        buf.put_i32(response.header.correlation_id);
        buf.put_i32(response.throttle_time_ms);
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

        Ok(())
    }

    // ========================================================================
    // API VERSIONS REQUEST/RESPONSE
    // ========================================================================

    fn decode_api_versions_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaApiVersionsRequest> {
        debug!(
            "Decoding ApiVersions request: api_version={}, remaining_bytes={}, correlation_id={}",
            header.api_version,
            cursor.remaining(),
            header.correlation_id
        );

        // Version-aware decoding for ApiVersions  
        let (client_software_name, client_software_version) = if header.api_version >= 3 {
            // v3+: Use KIP-482 flexible versions with compact strings
            debug!("Decoding ApiVersions v3+ with KIP-482 flexible versions");
            debug!("Remaining bytes before field parsing: {}", cursor.remaining());
            
            // Show hex bytes for debugging
            let remaining_bytes = cursor.remaining();
            if remaining_bytes > 0 {
                let pos = cursor.position() as usize;
                let slice = cursor.get_ref();
                let hex_bytes = &slice[pos..pos+std::cmp::min(remaining_bytes, 32)];
                debug!("Next {} hex bytes: {}", hex_bytes.len(), hex_bytes.iter().map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(""));
            }
            
            // FIXED: Use proper varint decoding for Java 4.1 compatibility
            // Java 4.1 uses: readUnsignedVarint() - 1 for compact string lengths
            
            // First compact string: client_software_name
            let name = if cursor.remaining() > 0 {
                match Self::decode_varint(cursor) {
                    Ok(varint_len) => {
                        debug!("First varint length: {}", varint_len);
                        if varint_len == 0 {
                            debug!("Null string for client_software_name");
                            None
                        } else {
                            let actual_len = (varint_len - 1) as usize; // Java: readUnsignedVarint() - 1
                            if cursor.remaining() >= actual_len {
                                let mut buf = vec![0u8; actual_len];
                                cursor.copy_to_slice(&mut buf);
                                match String::from_utf8(buf) {
                                    Ok(name_str) => {
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
                        debug!("Second varint length: {}", varint_len);
                        if varint_len == 0 {
                            debug!("Null string for client_software_version");
                            None
                        } else {
                            let actual_len = (varint_len - 1) as usize; // Java: readUnsignedVarint() - 1
                            if cursor.remaining() >= actual_len {
                                let mut buf = vec![0u8; actual_len];
                                cursor.copy_to_slice(&mut buf);
                                match String::from_utf8(buf) {
                                    Ok(version_str) => {
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
                        debug!("Tagged fields count: {}", num_tagged_fields);
                        // Skip tagged fields for now (TODO: implement proper tagged field parsing)
                        for i in 0..num_tagged_fields {
                            // Each tagged field has: tag (varint) + size (varint) + data
                            if let (Ok(tag), Ok(size)) = (Self::decode_varint(cursor), Self::decode_varint(cursor)) {
                                debug!("  Tagged field {}: tag={}, size={}", i, tag, size);
                                // CRITICAL FIX: Check bounds before advancing
                                let advance_size = size as usize;
                                if cursor.remaining() >= advance_size {
                                    cursor.advance(advance_size);
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
                            debug!("Consuming {} remaining bytes", remaining);
                            cursor.advance(remaining);
                        }
                    }
                }
            }
            
            debug!("Successfully parsed Java 4.1 ApiVersions format");
            
            (name, version)
        } else {
            // v0-v2: No client software fields
            debug!("Decoding ApiVersions v0-v2 (no client software fields)");
            (None, None)
        };

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
            debug!("Encoding ApiVersions v{} with flexible versions (FIXED ORDER)", response.api_version);
            
            // DEBUG: Log response structure for investigation
            debug!("ApiVersions v{} response structure:", response.api_version);
            debug!("  correlation_id: {}", response.header.correlation_id);  
            debug!("  error_code: {}", response.error_code);
            debug!("  api_keys count: {}", response.api_keys.len());
            debug!("  throttle_time_ms: {}", response.throttle_time_ms);
            
            // 1. ErrorCode (i16) - FIRST field after correlation_id
            debug!("  Adding error_code: {}", response.error_code);
            buf.put_i16(response.error_code);
            
            // 2. API Keys compact array - SECOND field
            let compact_length = (response.api_keys.len() as u64) + 1;
            debug!("  compact_length (raw): {}", response.api_keys.len());
            debug!("  compact_length (encoded): {}", compact_length);
            
            Self::encode_varint(buf, compact_length);
            
            for (i, api_key) in response.api_keys.iter().enumerate() {
                if i < 5 {  // Only log first 5 for brevity
                    debug!("  api_key[{}]: key={}, min={}, max={}", i, api_key.api_key, api_key.min_version, api_key.max_version);
                }
                buf.put_i16(api_key.api_key);
                buf.put_i16(api_key.min_version);
                buf.put_i16(api_key.max_version);
                
                // Tagged fields for each API key (empty for now)
                buf.put_u8(0); // empty tagged fields
            }
            
            // 3. ThrottleTimeMs - THIRD field (correct Kafka specification position)
            debug!("  Adding throttle_time_ms: {} at CORRECT position", response.throttle_time_ms);
            buf.put_i32(response.throttle_time_ms);

            // Add new fields for v3+ Java client compatibility
            if response.api_version >= 3 {
                // cluster_id (compact nullable string)
                debug!("  Adding cluster_id: {:?}", response.cluster_id);
                Self::encode_compact_nullable_string(&response.cluster_id, buf);
                
                // controller_id (i32) - Added for v3+
                if let Some(controller_id) = response.controller_id {
                    debug!("  Adding controller_id: {}", controller_id);
                    buf.put_i32(controller_id);
                } else {
                    debug!("  Adding controller_id: -1 (no controller)");
                    buf.put_i32(-1); // -1 indicates no controller
                }
                
                // supported_features (compact array) - Added for v3+
                debug!("  Adding supported_features count: {}", response.supported_features.len());
                Self::encode_compact_array_len(response.supported_features.len(), buf);
                for feature in &response.supported_features {
                    Self::encode_compact_string(feature, buf);
                }
            }

            // Top-level tagged fields (empty for now)  
            debug!("  Adding final tagged fields marker: 0x00");
            buf.put_u8(0); // empty tagged fields
        } else {
            // v0-v2 uses traditional fixed arrays
            debug!("Encoding ApiVersions v{} with standard format", response.api_version);
            buf.put_i32(response.api_keys.len() as i32);
            
            for api_key in &response.api_keys {
                buf.put_i16(api_key.api_key);
                buf.put_i16(api_key.min_version);
                buf.put_i16(api_key.max_version);
            }
            
            // 3. ThrottleTimeMs (for v1-v2, after api_keys)
            if response.api_version >= 1 {
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
        debug!("decode_string: length = {}", len);
        if len == -1 {
            return Err(KafkaCodecError::InvalidFormat(
                "Expected non-null string".to_string(),
            ));
        }

        if len == 0 {
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
        buf.put_i32(response.header.correlation_id);
        buf.put_i32(response.throttle_time_ms);
        buf.put_i32(response.topics.len() as i32);

        for topic in &response.topics {
            Self::encode_string(&topic.name, buf);
            buf.put_i16(topic.error_code);
            Self::encode_nullable_string(&topic.error_message, buf);

            // Topic configs (empty for basic response)
            buf.put_i32(0);
        }

        Ok(())
    }

    /// Decode DeleteTopics request
    fn decode_delete_topics_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaDeleteTopicsRequest> {
        let topic_count = cursor.get_i32();
        let mut topic_names = Vec::with_capacity(topic_count as usize);
        
        for _ in 0..topic_count {
            let topic_name = Self::decode_string(cursor)?;
            topic_names.push(topic_name);
        }
        
        // API version dependent fields - for now we support v0
        let timeout_ms = if header.api_version >= 1 {
            cursor.get_i32()
        } else {
            5000 // Default timeout
        };

        Ok(KafkaDeleteTopicsRequest {
            correlation_id: header.correlation_id,
            client_id: header.client_id,
            topic_names,
            timeout_ms,
        })
    }

    /// Encode DeleteTopics response
    fn encode_delete_topics_response(
        response: &KafkaDeleteTopicsResponse,
        buf: &mut BytesMut,
    ) -> Result<()> {
        // Correlation ID
        buf.put_i32(response.correlation_id);
        
        // Throttle time (API v1+)
        buf.put_i32(response.throttle_time_ms);
        
        // Response array
        buf.put_i32(response.responses.len() as i32);
        for topic_response in &response.responses {
            Self::encode_string(&topic_response.name, buf);
            buf.put_i16(topic_response.error_code);
            Self::encode_nullable_string(&topic_response.error_message, buf);
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
        // Resources array length
        let resources_length = cursor.get_i32();
        let mut resources = Vec::with_capacity(resources_length as usize);

        for _ in 0..resources_length {
            // Resource type
            let resource_type = cursor.get_i8();
            
            // Resource name
            let resource_name = Self::decode_string(cursor)?;
            
            // Configuration keys array
            let config_keys_length = cursor.get_i32();
            let configuration_keys = if config_keys_length == -1 {
                None
            } else {
                let mut keys = Vec::with_capacity(config_keys_length as usize);
                for _ in 0..config_keys_length {
                    let key = Self::decode_string(cursor)?;
                    keys.push(key);
                }
                Some(keys)
            };
            
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
        // Correlation ID
        buf.put_i32(response.correlation_id);

        // Throttle time (API v0+)
        buf.put_i32(response.throttle_time_ms);

        // Results array
        buf.put_i32(response.results.len() as i32);
        
        for result in &response.results {
            // Error code
            buf.put_i16(result.error_code);
            
            // Error message (nullable string)
            Self::encode_nullable_string(&result.error_message, buf);
            
            // Resource type
            buf.put_i8(result.resource_type);
            
            // Resource name
            Self::encode_string(&result.resource_name, buf);
            
            // Configs array
            buf.put_i32(result.configs.len() as i32);
            
            for config in &result.configs {
                // Config name
                Self::encode_string(&config.name, buf);
                
                // Config value (nullable string)
                Self::encode_nullable_string(&config.value, buf);
                
                // Read only
                buf.put_u8(if config.read_only { 1 } else { 0 });
                
                // Is default (API v0+)
                buf.put_u8(if config.is_default { 1 } else { 0 });
                
                // Config source (API v1+)
                buf.put_i8(config.config_source);
                
                // Is sensitive
                buf.put_u8(if config.is_sensitive { 1 } else { 0 });
                
                // Synonyms array (API v1+)
                buf.put_i32(config.synonyms.len() as i32);
                for synonym in &config.synonyms {
                    Self::encode_string(&synonym.name, buf);
                    Self::encode_nullable_string(&synonym.value, buf);
                    buf.put_i8(synonym.source);
                }
                
                // Config type (API v3+)
                buf.put_i8(config.config_type);
                
                // Documentation (API v3+)
                Self::encode_nullable_string(&config.documentation, buf);
            }
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
        // Resources array length
        let resources_length = cursor.get_i32();
        let mut resources = Vec::with_capacity(resources_length as usize);
        
        for _ in 0..resources_length {
            // Resource type
            let resource_type = cursor.get_i8();
            
            // Resource name
            let resource_name = Self::decode_string(cursor)?;
            
            // Configs array length
            let configs_length = cursor.get_i32();
            let mut configs = Vec::with_capacity(configs_length as usize);
            
            for _ in 0..configs_length {
                // Config name
                let name = Self::decode_string(cursor)?;
                
                // Config value (nullable string)
                let value = Self::decode_nullable_string(cursor)?;
                
                configs.push(KafkaAlterableConfig { name, value });
            }
            
            resources.push(KafkaAlterConfigsResource {
                resource_type,
                resource_name,
                configs,
            });
        }

        // Validate only (API v0+)
        let validate_only = cursor.get_u8() != 0;

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
        // Correlation ID
        buf.put_i32(response.correlation_id);
        
        // Throttle time (API v0+)
        buf.put_i32(response.throttle_time_ms);
        
        // Responses array
        buf.put_i32(response.responses.len() as i32);
        
        for resource_response in &response.responses {
            // Error code
            buf.put_i16(resource_response.error_code);
            
            // Error message (nullable string)
            Self::encode_nullable_string(&resource_response.error_message, buf);
            
            // Resource type
            buf.put_i8(resource_response.resource_type);
            
            // Resource name
            Self::encode_string(&resource_response.resource_name, buf);
        }

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
            debug!("No remaining bytes for tagged fields, treating as empty");
            return Ok(());
        }
        
        let num_fields = match Self::decode_varint(cursor) {
            Ok(n) => n,
            Err(e) => {
                debug!("Failed to decode tagged field count, treating as empty: {}", e);
                // If we can't even read the count, treat as empty tagged fields
                return Ok(());
            }
        };
        
        debug!("Decoding {} tagged fields", num_fields);
        
        // For each tagged field, skip tag and data
        for i in 0..num_fields {
            debug!("Processing tagged field {}/{}", i + 1, num_fields);
            
            // Skip tag
            let tag = match Self::decode_varint(cursor) {
                Ok(tag) => tag,
                Err(e) => {
                    debug!("Failed to decode tag {}: {}, stopping tagged field parsing", i, e);
                    break;
                }
            };
            
            // Skip data length
            let data_len = match Self::decode_varint(cursor) {
                Ok(len) => len,
                Err(e) => {
                    debug!("Failed to decode data length for tag {}: {}, stopping tagged field parsing", tag, e);
                    break;
                }
            };
            
            debug!("Tagged field: tag={}, data_len={}", tag, data_len);
            
            // Skip data bytes with bounds checking
            if cursor.remaining() < data_len as usize {
                debug!(
                    "Not enough bytes for tagged field data: needed={}, available={}, treating as end of fields",
                    data_len, cursor.remaining()
                );
                break;
            }
            cursor.advance(data_len as usize);
            debug!("Successfully skipped tagged field data ({} bytes)", data_len);
        }
        
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
        info!("  - FIXED compact string '{}': length={}, encoded as varint {} (Java will read {})", 
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
                info!("  - FIXED compact nullable string '{}': length={}, encoded as varint {} (Java will read {})", 
                      s, len, varint_len, len);
                buf.put_slice(s.as_bytes());
            }
            None => {
                // CRITICAL FIX: Null string MUST be encoded as single byte 0, not varint
                buf.put_u8(0);
                info!("  - FIXED compact nullable string: null, encoded as 0x00");
            }
        }
    }


    /// Encodes a compact array (array with varint length) for flexible versions
    #[allow(dead_code)]
    fn encode_compact_array_len(len: usize, buf: &mut BytesMut) {
        // Compact arrays use length + 1 encoding (0 = null, 1 = empty, 2+ = length + 1)
        Self::encode_varint(buf, len as u64 + 1);
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
        info!("KafkaFrameCodec: Decoded request without length prefix: {} bytes", message.len());
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
        info!("KafkaFrameCodec: Encoded response with length prefix: {} bytes total", message_len + 4);
        
        // DEBUG: Show exact hex bytes after framing (first 12 bytes)
        if dst.len() >= 12 {
            let first_12_bytes = &dst[0..12];
            info!("  - FRAMED RESPONSE BYTES: [{:02x} {:02x} {:02x} {:02x}] [{:02x} {:02x} {:02x} {:02x}] [{:02x} {:02x} {:02x} {:02x}]",
                  first_12_bytes[0], first_12_bytes[1], first_12_bytes[2], first_12_bytes[3],
                  first_12_bytes[4], first_12_bytes[5], first_12_bytes[6], first_12_bytes[7], 
                  first_12_bytes[8], first_12_bytes[9], first_12_bytes[10], first_12_bytes[11]);
            info!("  - LENGTH_PREFIX: {} = 0x{:08x}", message_len, message_len);
            info!("  - ACTUAL_CORRELATION_ID_AT_BYTES_4-7: {} (this should be correlation_id=1)", 
                  i32::from_be_bytes([first_12_bytes[4], first_12_bytes[5], first_12_bytes[6], first_12_bytes[7]]));
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
