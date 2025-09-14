//! Protocol Adapter
//!
//! This module provides conversion between Kafka protocol messages
//! and FluxMQ's internal message format, enabling protocol compatibility
//! while maintaining FluxMQ's optimized internal structures.

use bytes::Bytes;
use crc32fast::Hasher;
use std::collections::HashMap;
use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;

use super::errors::KafkaErrorCode;
use super::messages::*;
use crate::consumer::ConsumerGroupMessage;
use crate::protocol::{
    DeleteTopicsRequest, DeleteTopicsResponse, FetchRequest, FetchResponse, ListOffsetsRequest,
    ListOffsetsResponse, Message, MetadataRequest, MetadataResponse, MultiFetchRequest,
    MultiFetchResponse, ProduceRequest, ProduceResponse, Request, Response,
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};

/// Protocol adapter for converting between Kafka and FluxMQ message formats
pub struct ProtocolAdapter;

#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("Unsupported Kafka API: key={0}, version={1}")]
    UnsupportedApi(i16, i16),
    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),
    #[error("Invalid message format: {0}")]
    InvalidFormat(String),
    #[error("Invalid message: {0}")]
    InvalidMessage(String),
    #[error("FluxMQ error: {0}")]
    FluxMq(String),
    #[error("Missing required field: {0}")]
    MissingField(String),
}

pub type Result<T> = std::result::Result<T, AdapterError>;

impl ProtocolAdapter {
    /// Convert a Kafka request to FluxMQ internal format
    pub fn kafka_to_fluxmq(kafka_request: KafkaRequest) -> Result<Request> {
        match kafka_request {
            KafkaRequest::Produce(req) => {
                let fluxmq_req = Self::convert_produce_request(req)?;
                Ok(Request::Produce(fluxmq_req))
            }
            KafkaRequest::Fetch(req) => {
                // Check if this is a multi-topic fetch request
                if req.topics.len() > 1
                    || req.topics.get(0).map_or(false, |t| t.partitions.len() > 1)
                {
                    let multi_fetch_req = Self::convert_multi_fetch_request(req)?;
                    Ok(Request::MultiFetch(multi_fetch_req))
                } else {
                    let fluxmq_req = Self::convert_fetch_request(req)?;
                    Ok(Request::Fetch(fluxmq_req))
                }
            }
            KafkaRequest::ListOffsets(req) => {
                let fluxmq_req = Self::convert_list_offsets_request(req)?;
                Ok(Request::ListOffsets(fluxmq_req))
            }
            KafkaRequest::Metadata(req) => {
                let fluxmq_req = Self::convert_metadata_request(req)?;
                Ok(Request::Metadata(fluxmq_req))
            }
            KafkaRequest::JoinGroup(_req) => {
                // Consumer group requests are handled separately
                Err(AdapterError::UnsupportedApi(11, 0))
            }
            KafkaRequest::OffsetCommit(_req) => {
                // OffsetCommit is handled separately as consumer group request
                Err(AdapterError::UnsupportedApi(8, 0))
            }
            KafkaRequest::OffsetFetch(_req) => {
                // OffsetFetch is handled separately as consumer group request
                Err(AdapterError::UnsupportedApi(9, 0))
            }
            KafkaRequest::FindCoordinator(_req) => {
                // FindCoordinator is handled specially - return error to trigger special handling
                Err(AdapterError::UnsupportedApi(10, 0))
            }
            KafkaRequest::ListGroups(_req) => {
                // ListGroups is handled separately as consumer group request
                Err(AdapterError::UnsupportedApi(16, 0))
            }
            KafkaRequest::Heartbeat(_req) => {
                // Consumer group APIs not yet supported
                Err(AdapterError::UnsupportedApi(12, 0))
            }
            KafkaRequest::LeaveGroup(_req) => {
                // Consumer group APIs not yet supported
                Err(AdapterError::UnsupportedApi(13, 0))
            }
            KafkaRequest::SyncGroup(_req) => {
                // Consumer group APIs not yet supported
                Err(AdapterError::UnsupportedApi(14, 0))
            }
            KafkaRequest::DescribeGroups(_req) => {
                // DescribeGroups is handled separately as consumer group request
                Err(AdapterError::UnsupportedApi(15, 0))
            }
            KafkaRequest::ApiVersions(_req) => {
                // ApiVersions is handled specially - return error to trigger special handling
                Err(AdapterError::UnsupportedApi(18, 0))
            }
            KafkaRequest::CreateTopics(req) => {
                let fluxmq_req = Self::convert_create_topics_request(req)?;
                Ok(Request::CreateTopics(fluxmq_req))
            }
            KafkaRequest::DeleteTopics(req) => {
                let fluxmq_req = Self::convert_delete_topics_request(req)?;
                Ok(Request::DeleteTopics(fluxmq_req))
            }
            KafkaRequest::DescribeConfigs(req) => {
                let fluxmq_req = Self::convert_describe_configs_request(req)?;
                Ok(Request::DescribeConfigs(fluxmq_req))
            }
            KafkaRequest::AlterConfigs(req) => {
                let fluxmq_req = Self::convert_alter_configs_request(req)?;
                Ok(Request::AlterConfigs(fluxmq_req))
            }
            KafkaRequest::SaslHandshake(req) => {
                let fluxmq_req = Self::convert_sasl_handshake_request(req)?;
                Ok(Request::SaslHandshake(fluxmq_req))
            }
            KafkaRequest::SaslAuthenticate(req) => {
                let fluxmq_req = Self::convert_sasl_authenticate_request(req)?;
                Ok(Request::SaslAuthenticate(fluxmq_req))
            }
            KafkaRequest::LeaderAndIsr(_req) => {
                // Not implemented yet - return error
                Err(AdapterError::UnsupportedOperation(
                    "LEADER_AND_ISR not implemented".to_string(),
                ))
            }
            KafkaRequest::StopReplica(_req) => {
                // Not implemented yet - return error
                Err(AdapterError::UnsupportedOperation(
                    "STOP_REPLICA not implemented".to_string(),
                ))
            }
            KafkaRequest::UpdateMetadata(_req) => {
                // Not implemented yet - return error
                Err(AdapterError::UnsupportedOperation(
                    "UPDATE_METADATA not implemented".to_string(),
                ))
            }
            KafkaRequest::ControlledShutdown(_req) => {
                // Not implemented yet - return error
                Err(AdapterError::UnsupportedOperation(
                    "CONTROLLED_SHUTDOWN not implemented".to_string(),
                ))
            }
        }
    }

    /// Convert a FluxMQ response to Kafka format
    pub fn fluxmq_to_kafka(
        fluxmq_response: Response,
        correlation_id: i32,
    ) -> Result<KafkaResponse> {
        match fluxmq_response {
            Response::Produce(resp) => {
                let kafka_resp = Self::convert_produce_response(resp, correlation_id)?;
                Ok(KafkaResponse::Produce(kafka_resp))
            }
            Response::Fetch(resp) => {
                let kafka_resp = Self::convert_fetch_response(resp, correlation_id)?;
                Ok(KafkaResponse::Fetch(kafka_resp))
            }
            Response::MultiFetch(resp) => {
                let kafka_resp = Self::convert_multi_fetch_response(resp, correlation_id)?;
                Ok(KafkaResponse::Fetch(kafka_resp))
            }
            Response::ListOffsets(resp) => {
                let kafka_resp = Self::convert_list_offsets_response(resp, correlation_id)?;
                Ok(KafkaResponse::ListOffsets(kafka_resp))
            }
            Response::Metadata(resp) => {
                let kafka_resp = Self::convert_metadata_response(resp, correlation_id)?;
                Ok(KafkaResponse::Metadata(kafka_resp))
            }
            Response::CreateTopics(resp) => {
                let kafka_resp = Self::convert_create_topics_response(resp, correlation_id)?;
                Ok(KafkaResponse::CreateTopics(kafka_resp))
            }
            Response::DeleteTopics(resp) => {
                let kafka_resp = Self::convert_delete_topics_response(resp, correlation_id)?;
                Ok(KafkaResponse::DeleteTopics(kafka_resp))
            }
            Response::DescribeConfigs(resp) => {
                let kafka_resp = Self::convert_describe_configs_response(resp, correlation_id)?;
                Ok(KafkaResponse::DescribeConfigs(kafka_resp))
            }
            Response::AlterConfigs(resp) => {
                let kafka_resp = Self::convert_alter_configs_response(resp, correlation_id)?;
                Ok(KafkaResponse::AlterConfigs(kafka_resp))
            }
            Response::SaslHandshake(resp) => {
                let kafka_resp = Self::convert_sasl_handshake_response(resp, correlation_id)?;
                Ok(KafkaResponse::SaslHandshake(kafka_resp))
            }
            Response::SaslAuthenticate(resp) => {
                let kafka_resp = Self::convert_sasl_authenticate_response(resp, correlation_id)?;
                Ok(KafkaResponse::SaslAuthenticate(kafka_resp))
            }
            Response::NoResponse => {
                // For fire-and-forget requests (acks=0), return error to signal no response
                Err(AdapterError::FluxMq(
                    "Fire-and-forget request - no response should be sent".to_string(),
                ))
            }
        }
    }

    /// Handle Kafka consumer group request (separate from regular message flow)
    pub fn handle_consumer_group_request(
        kafka_request: KafkaRequest,
    ) -> Result<Option<ConsumerGroupMessage>> {
        match kafka_request {
            KafkaRequest::JoinGroup(req) => {
                let fluxmq_msg = Self::convert_join_group_request(req)?;
                Ok(Some(fluxmq_msg))
            }
            KafkaRequest::OffsetCommit(req) => {
                let fluxmq_msg = Self::convert_offset_commit_request(req)?;
                Ok(Some(fluxmq_msg))
            }
            KafkaRequest::OffsetFetch(req) => {
                let fluxmq_msg = Self::convert_offset_fetch_request(req)?;
                Ok(Some(fluxmq_msg))
            }
            KafkaRequest::Heartbeat(req) => {
                let fluxmq_msg = Self::convert_heartbeat_request(req)?;
                Ok(Some(fluxmq_msg))
            }
            KafkaRequest::LeaveGroup(req) => {
                let fluxmq_msg = Self::convert_leave_group_request(req)?;
                Ok(Some(fluxmq_msg))
            }
            KafkaRequest::SyncGroup(req) => {
                let fluxmq_msg = Self::convert_sync_group_request(req)?;
                Ok(Some(fluxmq_msg))
            }
            KafkaRequest::ListGroups(req) => {
                let fluxmq_msg = Self::convert_list_groups_request(req)?;
                Ok(Some(fluxmq_msg))
            }
            KafkaRequest::DescribeGroups(req) => {
                let fluxmq_msg = Self::convert_describe_groups_request(req)?;
                Ok(Some(fluxmq_msg))
            }
            _ => Ok(None), // Not a consumer group request
        }
    }

    /// Convert consumer group response from FluxMQ to Kafka format
    pub fn consumer_group_response_to_kafka(
        fluxmq_response: ConsumerGroupMessage,
        correlation_id: i32,
        api_version: i16,
    ) -> Result<KafkaResponse> {
        match fluxmq_response {
            ConsumerGroupMessage::JoinGroupResponse {
                error_code,
                generation_id,
                group_protocol,
                leader_id,
                consumer_id,
                members,
            } => {
                // Convert member list to Kafka format
                let kafka_members = members
                    .into_iter()
                    .map(|member| KafkaJoinGroupMember {
                        member_id: member.consumer_id,
                        group_instance_id: None,
                        metadata: Self::serialize_consumer_metadata(&member.subscribed_topics),
                    })
                    .collect();

                let resp = KafkaJoinGroupResponse {
                    header: KafkaResponseHeader { correlation_id },
                    throttle_time_ms: 0,
                    error_code: Self::map_error_code(error_code),
                    generation_id,
                    protocol_type: "consumer".to_string(),
                    protocol_name: group_protocol.clone(),
                    leader: leader_id,
                    member_id: consumer_id,
                    members: kafka_members,
                };
                debug!("JoinGroup response: protocol_type='consumer', protocol_name='{}', generation={}", 
                               group_protocol, generation_id);
                Ok(KafkaResponse::JoinGroup(resp))
            }
            ConsumerGroupMessage::OffsetCommitResponse {
                error_code,
                topic_partition_errors,
            } => {
                let resp = Self::convert_offset_commit_response(
                    error_code,
                    topic_partition_errors,
                    correlation_id,
                )?;
                Ok(KafkaResponse::OffsetCommit(resp))
            }
            ConsumerGroupMessage::OffsetFetchResponse {
                error_code,
                offsets,
            } => {
                let resp =
                    Self::convert_offset_fetch_response(error_code, offsets, correlation_id)?;
                Ok(KafkaResponse::OffsetFetch(resp))
            }
            ConsumerGroupMessage::ListGroupsResponse { .. } => {
                Self::convert_list_groups_response(fluxmq_response, correlation_id)
            }
            ConsumerGroupMessage::DescribeGroupsResponse { .. } => {
                Self::convert_describe_groups_response(fluxmq_response, correlation_id)
            }
            ConsumerGroupMessage::HeartbeatResponse { error_code } => {
                let resp = KafkaHeartbeatResponse {
                    header: KafkaResponseHeader { correlation_id },
                    throttle_time_ms: 0,
                    error_code,
                };
                Ok(KafkaResponse::Heartbeat(resp))
            }
            ConsumerGroupMessage::LeaveGroupResponse { error_code } => {
                let resp = KafkaLeaveGroupResponse {
                    header: KafkaResponseHeader { correlation_id },
                    throttle_time_ms: 0,
                    error_code,
                };
                Ok(KafkaResponse::LeaveGroup(resp))
            }
            ConsumerGroupMessage::SyncGroupResponse { error_code, .. } => {
                // TODO: Implement proper sync group response conversion
                let resp = KafkaSyncGroupResponse {
                    header: KafkaResponseHeader { correlation_id },
                    api_version,
                    throttle_time_ms: 0,
                    error_code,
                    protocol_type: "consumer".to_string(),
                    protocol_name: "range".to_string(),
                    assignment: bytes::Bytes::new(),
                };
                Ok(KafkaResponse::SyncGroup(resp))
            }
            _ => Err(AdapterError::InvalidFormat(
                "Unexpected consumer group message type".to_string(),
            )),
        }
    }

    // ========================================================================
    // PRIVATE CONVERSION METHODS
    // ========================================================================

    fn convert_produce_request(kafka_req: KafkaProduceRequest) -> Result<ProduceRequest> {
        // For simplicity, handle only the first topic/partition
        // In a full implementation, we'd need to handle multiple topics/partitions
        let topic_data = kafka_req
            .topic_data
            .into_iter()
            .next()
            .ok_or_else(|| AdapterError::MissingField("topic_data".to_string()))?;

        let partition_data = topic_data
            .partition_data
            .into_iter()
            .next()
            .ok_or_else(|| AdapterError::MissingField("partition_data".to_string()))?;

        // Convert Kafka record batch to FluxMQ messages
        let messages = if let Some(records_bytes) = partition_data.records {
            Self::parse_kafka_record_batch(&records_bytes)?
        } else {
            vec![]
        };

        Ok(ProduceRequest {
            correlation_id: kafka_req.header.correlation_id,
            topic: topic_data.topic,
            partition: partition_data.partition as u32,
            messages,
            acks: kafka_req.acks,
            timeout_ms: kafka_req.timeout_ms as u32,
        })
    }

    fn convert_fetch_request(kafka_req: KafkaFetchRequest) -> Result<FetchRequest> {
        // Handle only the first topic/partition for simplicity
        let topic_data = kafka_req
            .topics
            .into_iter()
            .next()
            .ok_or_else(|| AdapterError::MissingField("topics".to_string()))?;

        let partition_data = topic_data
            .partitions
            .into_iter()
            .next()
            .ok_or_else(|| AdapterError::MissingField("partitions".to_string()))?;

        Ok(FetchRequest {
            correlation_id: kafka_req.header.correlation_id,
            topic: topic_data.topic,
            partition: partition_data.partition as u32,
            offset: partition_data.fetch_offset as u64,
            max_bytes: partition_data.max_bytes as u32,
            timeout_ms: kafka_req.max_wait_ms as u32,
        })
    }

    fn convert_list_offsets_request(
        kafka_req: KafkaListOffsetsRequest,
    ) -> Result<ListOffsetsRequest> {
        // Handle only the first topic/partition for simplicity
        let topic_data = kafka_req
            .topics
            .into_iter()
            .next()
            .ok_or_else(|| AdapterError::MissingField("topics".to_string()))?;

        let partition_data = topic_data
            .partitions
            .into_iter()
            .next()
            .ok_or_else(|| AdapterError::MissingField("partitions".to_string()))?;

        Ok(ListOffsetsRequest {
            correlation_id: kafka_req.header.correlation_id,
            topic: topic_data.topic,
            partition: partition_data.partition as u32,
            timestamp: partition_data.timestamp,
        })
    }

    fn convert_metadata_request(kafka_req: KafkaMetadataRequest) -> Result<MetadataRequest> {
        Ok(MetadataRequest {
            correlation_id: kafka_req.header.correlation_id,
            topics: kafka_req.topics.unwrap_or_default(),
            api_version: kafka_req.header.api_version, // Preserve API version for proper response encoding
            allow_auto_topic_creation: kafka_req.allow_auto_topic_creation,
        })
    }

    fn convert_produce_response(
        fluxmq_resp: ProduceResponse,
        correlation_id: i32,
    ) -> Result<KafkaProduceResponse> {
        let partition_response = KafkaPartitionProduceResponse {
            partition: fluxmq_resp.partition as i32,
            error_code: Self::map_error_code(fluxmq_resp.error_code),
            base_offset: fluxmq_resp.base_offset as i64,
            log_append_time_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            log_start_offset: 0, // Simplified
        };

        let topic_response = KafkaTopicProduceResponse {
            topic: fluxmq_resp.topic,
            partition_responses: vec![partition_response],
        };

        Ok(KafkaProduceResponse {
            header: KafkaResponseHeader { correlation_id },
            responses: vec![topic_response],
            throttle_time_ms: 0,
        })
    }

    fn convert_fetch_response(
        fluxmq_resp: FetchResponse,
        correlation_id: i32,
    ) -> Result<KafkaFetchResponse> {
        // Convert FluxMQ messages to Kafka record batch format using proper KIP-98 format
        let records = if fluxmq_resp.messages.is_empty() {
            tracing::debug!("No messages in fetch response, returning empty bytes");
            Some(bytes::Bytes::new())
        } else {
            let record_batch = Self::encode_messages_as_record_batch(&fluxmq_resp.messages);
            tracing::debug!(
                "Generated record batch for {} messages: {} bytes",
                fluxmq_resp.messages.len(),
                record_batch.len()
            );
            Some(record_batch)
        };

        let partition_response = KafkaPartitionFetchResponse {
            partition: fluxmq_resp.partition as i32,
            error_code: Self::map_error_code(fluxmq_resp.error_code),
            high_watermark: fluxmq_resp.messages.len() as i64, // Simplified
            last_stable_offset: fluxmq_resp.messages.len() as i64,
            log_start_offset: 0,
            aborted_transactions: vec![],
            preferred_read_replica: -1,
            records,
        };

        let topic_response = KafkaTopicFetchResponse {
            topic: fluxmq_resp.topic,
            partitions: vec![partition_response],
        };

        Ok(KafkaFetchResponse {
            header: KafkaResponseHeader { correlation_id },
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::NoError.as_i16(),
            session_id: 0,
            responses: vec![topic_response],
        })
    }

    fn convert_list_offsets_response(
        fluxmq_resp: ListOffsetsResponse,
        correlation_id: i32,
    ) -> Result<KafkaListOffsetsResponse> {
        let partition_response = KafkaListOffsetsPartitionResponse {
            partition: fluxmq_resp.partition as i32,
            error_code: Self::map_error_code(fluxmq_resp.error_code),
            timestamp: fluxmq_resp.timestamp,
            offset: fluxmq_resp.offset,
            leader_epoch: -1, // Not tracked yet
        };

        let topic_response = KafkaListOffsetsTopicResponse {
            topic: fluxmq_resp.topic,
            partitions: vec![partition_response],
        };

        Ok(KafkaListOffsetsResponse {
            header: KafkaResponseHeader { correlation_id },
            throttle_time_ms: 0,
            topics: vec![topic_response],
        })
    }

    fn convert_metadata_response(
        fluxmq_resp: MetadataResponse,
        correlation_id: i32,
    ) -> Result<KafkaMetadataResponse> {
        let brokers = fluxmq_resp
            .brokers
            .into_iter()
            .map(|b| KafkaBrokerMetadata {
                node_id: b.node_id,
                host: b.host,
                port: b.port,
                rack: None,
            })
            .collect();

        let topics = fluxmq_resp
            .topics
            .into_iter()
            .map(|t| {
                let partitions = t
                    .partitions
                    .into_iter()
                    .map(|p| KafkaPartitionMetadata {
                        error_code: KafkaErrorCode::NoError.as_i16(),
                        partition: p.id as i32,
                        leader: p.leader.unwrap_or(-1),
                        leader_epoch: 0,
                        replica_nodes: p.replicas,
                        isr_nodes: p.isr,
                        offline_replicas: vec![],
                    })
                    .collect();

                KafkaTopicMetadata {
                    error_code: t.error_code,
                    topic: t.name,
                    is_internal: false,
                    partitions,
                    topic_authorized_operations: -2147483648, // No authorization operations
                }
            })
            .collect();

        Ok(KafkaMetadataResponse {
            header: KafkaResponseHeader { correlation_id },
            api_version: fluxmq_resp.api_version, // Use actual requested API version
            throttle_time_ms: 0,
            brokers,
            cluster_id: Some("fluxmq-cluster".to_string()),
            controller_id: 0,
            topics,
            cluster_authorized_operations: -2147483648,
        })
    }

    fn convert_join_group_request(
        kafka_req: KafkaJoinGroupRequest,
    ) -> Result<ConsumerGroupMessage> {
        use crate::consumer::{ConsumerGroupMessage, GroupProtocol};

        // Enhanced protocol metadata handling
        let mut group_protocols = Vec::new();
        let mut subscribed_topics = Vec::new();

        for protocol in kafka_req.protocols {
            // Parse consumer protocol metadata to extract subscribed topics
            if let Ok(topics) = Self::parse_consumer_protocol_metadata(&protocol.metadata) {
                subscribed_topics.extend(topics);
            }

            group_protocols.push(GroupProtocol {
                name: protocol.name,
                metadata: protocol.metadata.to_vec(),
            });
        }

        // Deduplicate subscribed topics
        subscribed_topics.sort();
        subscribed_topics.dedup();

        debug!(
            "JoinGroup: consumer {} subscribing to topics: {:?}",
            kafka_req.member_id, subscribed_topics
        );

        Ok(ConsumerGroupMessage::JoinGroup {
            group_id: kafka_req.group_id,
            consumer_id: kafka_req.member_id,
            client_id: kafka_req.header.client_id.unwrap_or("unknown".to_string()),
            client_host: "unknown".to_string(), // Not available in Kafka request
            session_timeout_ms: kafka_req.session_timeout_ms as u64,
            rebalance_timeout_ms: kafka_req.rebalance_timeout_ms as u64,
            protocol_type: kafka_req.protocol_type,
            group_protocols,
        })
    }

    fn convert_offset_commit_request(
        kafka_req: KafkaOffsetCommitRequest,
    ) -> Result<ConsumerGroupMessage> {
        use crate::consumer::{ConsumerGroupMessage, TopicPartitionOffset};

        let offsets = kafka_req
            .topics
            .into_iter()
            .flat_map(|topic_data| {
                topic_data
                    .partitions
                    .into_iter()
                    .map(move |partition_data| TopicPartitionOffset {
                        topic: topic_data.topic.clone(),
                        partition: partition_data.partition as u32,
                        offset: partition_data.offset,
                        metadata: partition_data.metadata,
                    })
            })
            .collect();

        Ok(ConsumerGroupMessage::OffsetCommit {
            group_id: kafka_req.group_id,
            consumer_id: kafka_req.consumer_id,
            generation_id: kafka_req.generation_id,
            retention_time_ms: kafka_req.retention_time_ms,
            offsets,
        })
    }

    fn convert_offset_fetch_request(
        kafka_req: KafkaOffsetFetchRequest,
    ) -> Result<ConsumerGroupMessage> {
        use crate::consumer::{ConsumerGroupMessage, TopicPartition};

        let topic_partitions = kafka_req.topics.map(|topics| {
            topics
                .into_iter()
                .flat_map(|topic_data| {
                    topic_data.partitions.into_iter().map(move |partition| {
                        TopicPartition::new(topic_data.topic.clone(), partition as u32)
                    })
                })
                .collect()
        });

        Ok(ConsumerGroupMessage::OffsetFetch {
            group_id: kafka_req.group_id,
            topic_partitions,
        })
    }

    fn convert_heartbeat_request(kafka_req: KafkaHeartbeatRequest) -> Result<ConsumerGroupMessage> {
        Ok(ConsumerGroupMessage::Heartbeat {
            group_id: kafka_req.group_id,
            consumer_id: kafka_req.consumer_id,
            generation_id: kafka_req.generation_id,
        })
    }

    fn convert_leave_group_request(
        kafka_req: KafkaLeaveGroupRequest,
    ) -> Result<ConsumerGroupMessage> {
        Ok(ConsumerGroupMessage::LeaveGroup {
            group_id: kafka_req.group_id,
            consumer_id: kafka_req.consumer_id,
        })
    }

    fn convert_sync_group_request(
        kafka_req: KafkaSyncGroupRequest,
    ) -> Result<ConsumerGroupMessage> {
        // TODO: Parse assignments from Kafka format
        // For now, return empty assignments
        Ok(ConsumerGroupMessage::SyncGroup {
            group_id: kafka_req.group_id,
            consumer_id: kafka_req.consumer_id,
            generation_id: kafka_req.generation_id,
            group_assignments: std::collections::HashMap::new(),
        })
    }

    fn convert_offset_commit_response(
        _error_code: i16,
        topic_partition_errors: Vec<crate::consumer::TopicPartitionError>,
        correlation_id: i32,
    ) -> Result<KafkaOffsetCommitResponse> {
        let mut topic_responses = std::collections::HashMap::new();

        for error in topic_partition_errors {
            let topic_response = topic_responses
                .entry(error.topic.clone())
                .or_insert_with(|| KafkaOffsetCommitTopicResponse {
                    topic: error.topic.clone(),
                    partitions: Vec::new(),
                });

            topic_response
                .partitions
                .push(KafkaOffsetCommitPartitionResponse {
                    partition: error.partition as i32,
                    error_code: error.error_code,
                });
        }

        Ok(KafkaOffsetCommitResponse {
            header: KafkaResponseHeader { correlation_id },
            throttle_time_ms: 0,
            topics: topic_responses.into_values().collect(),
        })
    }

    fn convert_offset_fetch_response(
        global_error_code: i16,
        offsets: Vec<crate::consumer::TopicPartitionOffsetResult>,
        correlation_id: i32,
    ) -> Result<KafkaOffsetFetchResponse> {
        let mut topic_responses = std::collections::HashMap::new();

        for offset in offsets {
            let topic_response = topic_responses
                .entry(offset.topic.clone())
                .or_insert_with(|| KafkaOffsetFetchTopicResponse {
                    topic: offset.topic.clone(),
                    partitions: Vec::new(),
                });

            topic_response
                .partitions
                .push(KafkaOffsetFetchPartitionResponse {
                    partition: offset.partition as i32,
                    offset: offset.offset,
                    leader_epoch: offset.leader_epoch,
                    metadata: offset.metadata,
                    error_code: offset.error_code,
                });
        }

        Ok(KafkaOffsetFetchResponse {
            header: KafkaResponseHeader { correlation_id },
            throttle_time_ms: 0,
            topics: topic_responses.into_values().collect(),
            error_code: global_error_code,
        })
    }

    fn convert_create_topics_response(
        fluxmq_resp: crate::protocol::CreateTopicsResponse,
        correlation_id: i32,
    ) -> Result<KafkaCreateTopicsResponse> {
        let kafka_topics = fluxmq_resp
            .topics
            .into_iter()
            .map(|topic| {
                KafkaCreatableTopicResult {
                    name: topic.name,
                    topic_id: None, // UUID not used in basic version
                    error_code: topic.error_code,
                    error_message: topic.error_message,
                    topic_config_error_code: None,
                    num_partitions: Some(topic.num_partitions),
                    replication_factor: Some(topic.replication_factor),
                    configs: Some(
                        topic
                            .configs
                            .into_iter()
                            .map(|config| KafkaCreatableTopicConfigs {
                                name: config.name,
                                value: config.value,
                                read_only: false,
                                config_source: 0,
                                is_sensitive: false,
                            })
                            .collect(),
                    ),
                }
            })
            .collect();

        Ok(KafkaCreateTopicsResponse {
            header: KafkaResponseHeader { correlation_id },
            throttle_time_ms: fluxmq_resp.throttle_time_ms,
            topics: kafka_topics,
        })
    }

    /// Convert Kafka DeleteTopics request to FluxMQ request
    fn convert_delete_topics_request(req: KafkaDeleteTopicsRequest) -> Result<DeleteTopicsRequest> {
        Ok(DeleteTopicsRequest {
            correlation_id: req.correlation_id,
            topic_names: req.topic_names,
            timeout_ms: req.timeout_ms,
        })
    }

    /// Convert FluxMQ DeleteTopics response to Kafka response
    fn convert_delete_topics_response(
        resp: DeleteTopicsResponse,
        correlation_id: i32,
    ) -> Result<KafkaDeleteTopicsResponse> {
        let kafka_topics = resp
            .responses
            .into_iter()
            .map(|result| super::messages::DeletableTopicResult {
                name: result.name,
                topic_id: result.topic_id,
                error_code: result.error_code,
                error_message: result.error_message,
            })
            .collect();

        Ok(KafkaDeleteTopicsResponse {
            correlation_id,
            throttle_time_ms: resp.throttle_time_ms,
            responses: kafka_topics,
        })
    }

    /// Convert Kafka SASL Handshake request to FluxMQ request
    fn convert_sasl_handshake_request(
        req: KafkaSaslHandshakeRequest,
    ) -> Result<SaslHandshakeRequest> {
        Ok(SaslHandshakeRequest {
            correlation_id: req.correlation_id,
            mechanism: req.mechanism,
        })
    }

    /// Convert FluxMQ SASL Handshake response to Kafka response
    fn convert_sasl_handshake_response(
        resp: SaslHandshakeResponse,
        correlation_id: i32,
    ) -> Result<KafkaSaslHandshakeResponse> {
        Ok(KafkaSaslHandshakeResponse {
            correlation_id,
            error_code: resp.error_code,
            mechanisms: resp.mechanisms,
        })
    }

    /// Convert Kafka SASL Authenticate request to FluxMQ request
    fn convert_sasl_authenticate_request(
        req: KafkaSaslAuthenticateRequest,
    ) -> Result<SaslAuthenticateRequest> {
        Ok(SaslAuthenticateRequest {
            correlation_id: req.correlation_id,
            auth_bytes: req.auth_bytes,
        })
    }

    /// Convert FluxMQ SASL Authenticate response to Kafka response
    fn convert_sasl_authenticate_response(
        resp: SaslAuthenticateResponse,
        correlation_id: i32,
    ) -> Result<KafkaSaslAuthenticateResponse> {
        Ok(KafkaSaslAuthenticateResponse {
            correlation_id,
            error_code: resp.error_code,
            error_message: resp.error_message,
            auth_bytes: resp.auth_bytes,
            session_lifetime_ms: resp.session_lifetime_ms,
        })
    }

    // ========================================================================
    // HELPER METHODS
    // ========================================================================

    /// Parse Kafka record batch into FluxMQ messages
    fn parse_kafka_record_batch(records_bytes: &Bytes) -> Result<Vec<Message>> {
        // This is a simplified implementation
        // In a real implementation, we'd need to properly parse the Kafka record batch format
        // For now, we'll create a single message from the raw bytes

        let message = Message {
            key: None,
            value: records_bytes.clone(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            headers: HashMap::new(),
        };

        Ok(vec![message])
    }

    /// Convert Kafka CreateTopics request to FluxMQ format
    fn convert_create_topics_request(
        kafka_req: KafkaCreateTopicsRequest,
    ) -> Result<crate::protocol::CreateTopicsRequest> {
        use crate::protocol::{CreatableTopic, CreateTopicsRequest, CreateableTopicConfig};

        let fluxmq_topics: Vec<CreatableTopic> = kafka_req
            .topics
            .into_iter()
            .map(|kafka_topic| CreatableTopic {
                name: kafka_topic.name,
                num_partitions: kafka_topic.num_partitions,
                replication_factor: kafka_topic.replication_factor,
                assignments: vec![], // Auto-assignment for now
                configs: kafka_topic
                    .configs
                    .unwrap_or_default()
                    .into_iter()
                    .map(|config| CreateableTopicConfig {
                        name: config.name,
                        value: config.value,
                    })
                    .collect(),
            })
            .collect();

        Ok(CreateTopicsRequest {
            correlation_id: kafka_req.header.correlation_id,
            topics: fluxmq_topics,
            timeout_ms: kafka_req.timeout_ms,
            validate_only: kafka_req.validate_only,
        })
    }

    /// Map FluxMQ error codes to Kafka error codes
    fn map_error_code(fluxmq_error_code: i16) -> i16 {
        match fluxmq_error_code {
            0 => KafkaErrorCode::NoError.as_i16(),
            1 => KafkaErrorCode::OffsetOutOfRange.as_i16(),
            2 => KafkaErrorCode::CorruptMessage.as_i16(),
            3 => KafkaErrorCode::UnknownTopicOrPartition.as_i16(),
            4 => KafkaErrorCode::LeaderNotAvailable.as_i16(),
            5 => KafkaErrorCode::NotLeaderForPartition.as_i16(),
            _ => KafkaErrorCode::Unknown.as_i16(),
        }
    }

    /// Convert Kafka ListGroups request to FluxMQ format
    fn convert_list_groups_request(
        _kafka_req: KafkaListGroupsRequest,
    ) -> Result<ConsumerGroupMessage> {
        Ok(ConsumerGroupMessage::ListGroups)
    }

    /// Convert Kafka DescribeGroups request to FluxMQ format
    fn convert_describe_groups_request(
        kafka_req: KafkaDescribeGroupsRequest,
    ) -> Result<ConsumerGroupMessage> {
        let group_ids = kafka_req.groups;
        Ok(ConsumerGroupMessage::DescribeGroups { group_ids })
    }

    /// Convert FluxMQ ListGroups response to Kafka format
    pub fn convert_list_groups_response(
        response: ConsumerGroupMessage,
        correlation_id: i32,
    ) -> Result<KafkaResponse> {
        match response {
            ConsumerGroupMessage::ListGroupsResponse { error_code, groups } => {
                let kafka_groups = groups
                    .into_iter()
                    .map(|group| KafkaListedGroup {
                        group_id: group.group_id,
                        protocol_type: group.protocol_type,
                        group_state: "Stable".to_string(), // Default state
                    })
                    .collect();

                Ok(KafkaResponse::ListGroups(KafkaListGroupsResponse {
                    header: KafkaResponseHeader { correlation_id },
                    throttle_time_ms: 0,
                    error_code: Self::map_error_code(error_code),
                    groups: kafka_groups,
                }))
            }
            _ => Err(AdapterError::InvalidMessage(
                "Expected ListGroups response".to_string(),
            )),
        }
    }

    /// Convert FluxMQ DescribeGroups response to Kafka format
    pub fn convert_describe_groups_response(
        response: ConsumerGroupMessage,
        correlation_id: i32,
    ) -> Result<KafkaResponse> {
        match response {
            ConsumerGroupMessage::DescribeGroupsResponse { groups } => {
                let kafka_groups = groups
                    .into_iter()
                    .map(|group| {
                        let kafka_members = group
                            .members
                            .into_iter()
                            .map(|member| KafkaDescribedGroupMember {
                                member_id: member.consumer_id,
                                group_instance_id: None,
                                client_id: member.client_id,
                                client_host: member.client_host,
                                member_metadata: Bytes::from(member.member_metadata),
                                member_assignment: Bytes::from(member.member_assignment),
                            })
                            .collect();

                        KafkaDescribedGroup {
                            error_code: Self::map_error_code(group.error_code),
                            group_id: group.group_id,
                            group_state: format!("{:?}", group.state),
                            protocol_type: group.protocol_type,
                            protocol_data: group.protocol_data,
                            members: kafka_members,
                            authorized_operations: 0,
                        }
                    })
                    .collect();

                Ok(KafkaResponse::DescribeGroups(KafkaDescribeGroupsResponse {
                    header: KafkaResponseHeader { correlation_id },
                    throttle_time_ms: 0,
                    groups: kafka_groups,
                }))
            }
            _ => Err(AdapterError::InvalidMessage(
                "Expected DescribeGroups response".to_string(),
            )),
        }
    }

    /// Parse Kafka consumer protocol metadata to extract subscribed topics
    fn parse_consumer_protocol_metadata(metadata: &bytes::Bytes) -> Result<Vec<String>> {
        // Kafka consumer protocol metadata format:
        // version (2 bytes) + subscription (array of topic names)
        // This is a simplified parser - a full implementation would handle all versions

        if metadata.len() < 6 {
            // Too short to contain valid metadata
            return Ok(Vec::new());
        }

        let mut cursor = std::io::Cursor::new(metadata);

        // Read version (2 bytes, big-endian)
        let mut version_bytes = [0u8; 2];
        if cursor.read_exact(&mut version_bytes).is_err() {
            return Ok(Vec::new());
        }
        let _version = u16::from_be_bytes(version_bytes);

        // Read topic count (4 bytes, big-endian)
        let mut count_bytes = [0u8; 4];
        if cursor.read_exact(&mut count_bytes).is_err() {
            return Ok(Vec::new());
        }
        let topic_count = i32::from_be_bytes(count_bytes);

        if topic_count < 0 || topic_count > 1000 {
            // Sanity check: reasonable limit on topic count
            return Ok(Vec::new());
        }

        let mut topics = Vec::new();

        for _ in 0..topic_count {
            // Read topic name length (2 bytes, big-endian)
            let mut len_bytes = [0u8; 2];
            if cursor.read_exact(&mut len_bytes).is_err() {
                break;
            }
            let topic_len = u16::from_be_bytes(len_bytes) as usize;

            if topic_len > 249 {
                // Kafka topic name limit
                break;
            }

            // Read topic name
            let mut topic_bytes = vec![0u8; topic_len];
            if cursor.read_exact(&mut topic_bytes).is_err() {
                break;
            }

            if let Ok(topic_name) = String::from_utf8(topic_bytes) {
                topics.push(topic_name);
            }
        }

        Ok(topics)
    }

    /// Serialize consumer metadata for JoinGroup response
    fn serialize_consumer_metadata(subscribed_topics: &[String]) -> Bytes {
        // Kafka consumer protocol metadata format:
        // version (2 bytes) + topic_count (4 bytes) + topics (length-prefixed strings)

        let mut metadata = Vec::new();

        // Version 0
        metadata.extend_from_slice(&0u16.to_be_bytes());

        // Topic count
        metadata.extend_from_slice(&(subscribed_topics.len() as i32).to_be_bytes());

        // Topics
        for topic in subscribed_topics {
            // Topic name length (2 bytes)
            metadata.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            // Topic name
            metadata.extend_from_slice(topic.as_bytes());
        }

        // User data (empty for now)
        metadata.extend_from_slice(&0i32.to_be_bytes());

        Bytes::from(metadata)
    }

    /// Convert Kafka multi-topic Fetch request to FluxMQ format
    fn convert_multi_fetch_request(kafka_req: KafkaFetchRequest) -> Result<MultiFetchRequest> {
        use crate::protocol::{PartitionFetchRequest, TopicFetchRequest};

        let topics = kafka_req
            .topics
            .into_iter()
            .map(|topic_data| {
                let partitions = topic_data
                    .partitions
                    .into_iter()
                    .map(|partition_data| PartitionFetchRequest {
                        partition: partition_data.partition as u32,
                        offset: partition_data.fetch_offset as u64,
                        max_bytes: partition_data.max_bytes as u32,
                    })
                    .collect();

                TopicFetchRequest {
                    topic: topic_data.topic,
                    partitions,
                }
            })
            .collect();

        Ok(MultiFetchRequest {
            correlation_id: kafka_req.header.correlation_id,
            topics,
            max_wait_ms: kafka_req.max_wait_ms as u32,
            min_bytes: kafka_req.min_bytes as u32,
            max_bytes: kafka_req.max_bytes as u32,
        })
    }

    /// Convert FluxMQ MultiFetch response to Kafka format
    fn convert_multi_fetch_response(
        fluxmq_resp: MultiFetchResponse,
        correlation_id: i32,
    ) -> Result<KafkaFetchResponse> {
        let topic_responses = fluxmq_resp
            .topics
            .into_iter()
            .map(|topic_resp| {
                let partition_responses = topic_resp
                    .partitions
                    .into_iter()
                    .map(|partition_resp| {
                        // Convert messages to Kafka record batch format
                        let records =
                            Self::encode_messages_as_record_batch(&partition_resp.messages);

                        KafkaPartitionFetchResponse {
                            partition: partition_resp.partition as i32,
                            error_code: partition_resp.error_code,
                            high_watermark: partition_resp
                                .messages
                                .last()
                                .map(|(offset, _)| *offset as i64 + 1)
                                .unwrap_or(0),
                            last_stable_offset: -1,
                            log_start_offset: 0,
                            aborted_transactions: vec![],
                            preferred_read_replica: -1,
                            records: Some(records),
                        }
                    })
                    .collect();

                KafkaTopicFetchResponse {
                    topic: topic_resp.topic,
                    partitions: partition_responses,
                }
            })
            .collect();

        Ok(KafkaFetchResponse {
            header: KafkaResponseHeader { correlation_id },
            throttle_time_ms: 0,
            error_code: Self::map_error_code(fluxmq_resp.error_code),
            session_id: 0,
            responses: topic_responses,
        })
    }

    /// Encode FluxMQ messages as proper Kafka record batch format (KIP-98)
    /// Using a simplified approach that focuses on compatibility
    fn encode_messages_as_record_batch(messages: &[(u64, Message)]) -> Bytes {
        if messages.is_empty() {
            return Bytes::new();
        }

        // Try the simplest possible legacy format that kafka-python should understand
        let mut records = Vec::new();

        for (offset, message) in messages {
            let value_bytes = &message.value;

            // Message format for magic byte 0/1 (legacy format):
            // offset (8 bytes) + message_size (4 bytes) + crc (4 bytes) + magic (1 byte) + attributes (1 byte) + key_length (4 bytes) + value_length (4 bytes) + value
            records.extend_from_slice(&(*offset as i64).to_be_bytes());

            // Message size = 4 (crc) + 1 (magic) + 1 (attributes) + 4 (key_length) + 4 (value_length) + value.len()
            let message_size = 4 + 1 + 1 + 4 + 4 + value_bytes.len();
            records.extend_from_slice(&(message_size as i32).to_be_bytes());

            // Calculate CRC32 for the message payload (magic + attributes + key_length + value_length + value)
            let mut crc_hasher = Hasher::new();
            crc_hasher.update(&[0]); // magic byte
            crc_hasher.update(&[0]); // attributes
            crc_hasher.update(&(-1i32).to_be_bytes()); // key length (-1 for null)
            crc_hasher.update(&(value_bytes.len() as i32).to_be_bytes()); // value length
            crc_hasher.update(value_bytes); // value data
            let crc = crc_hasher.finalize();
            records.extend_from_slice(&crc.to_be_bytes());

            // Magic byte = 0 (oldest format for maximum compatibility)
            records.push(0);

            // Attributes (no compression, no timestamp)
            records.push(0);

            // Key length = -1 (null key)
            records.extend_from_slice(&(-1i32).to_be_bytes());

            // Value length and value
            records.extend_from_slice(&(value_bytes.len() as i32).to_be_bytes());
            records.extend_from_slice(value_bytes);
        }

        tracing::debug!(
            "Created simple legacy records with {} bytes for {} messages",
            records.len(),
            messages.len()
        );

        Bytes::from(records)
    }

    // ========================================================================
    // DESCRIBE CONFIGS CONVERSION FUNCTIONS
    // ========================================================================

    /// Convert Kafka DescribeConfigs request to FluxMQ format
    fn convert_describe_configs_request(
        kafka_req: KafkaDescribeConfigsRequest,
    ) -> Result<crate::protocol::DescribeConfigsRequest> {
        use crate::protocol::{ConfigResource, DescribeConfigsRequest};

        let fluxmq_resources: Vec<ConfigResource> = kafka_req
            .resources
            .into_iter()
            .map(|kafka_resource| ConfigResource {
                resource_type: kafka_resource.resource_type,
                resource_name: kafka_resource.resource_name,
                configuration_keys: kafka_resource.configuration_keys.unwrap_or_default(),
            })
            .collect();

        Ok(DescribeConfigsRequest {
            correlation_id: kafka_req.correlation_id,
            resources: fluxmq_resources,
            include_synonyms: kafka_req.include_synonyms,
            include_documentation: kafka_req.include_documentation,
        })
    }

    /// Convert FluxMQ DescribeConfigs response to Kafka format
    fn convert_describe_configs_response(
        fluxmq_resp: crate::protocol::DescribeConfigsResponse,
        correlation_id: i32,
    ) -> Result<KafkaDescribeConfigsResponse> {
        let kafka_results: Vec<KafkaConfigResourceResult> = fluxmq_resp
            .results
            .into_iter()
            .map(|fluxmq_result| {
                let kafka_configs: Vec<KafkaConfigEntry> = fluxmq_result
                    .configs
                    .into_iter()
                    .map(|fluxmq_config| KafkaConfigEntry {
                        name: fluxmq_config.name,
                        value: Some(fluxmq_config.value),
                        read_only: fluxmq_config.read_only,
                        is_default: fluxmq_config.is_default,
                        config_source: fluxmq_config.config_source,
                        is_sensitive: fluxmq_config.is_sensitive,
                        synonyms: fluxmq_config
                            .synonyms
                            .into_iter()
                            .map(|synonym| KafkaConfigSynonym {
                                name: synonym.name,
                                value: synonym.value,
                                source: synonym.source,
                            })
                            .collect(),
                        config_type: fluxmq_config.config_type,
                        documentation: fluxmq_config.documentation,
                    })
                    .collect();

                KafkaConfigResourceResult {
                    error_code: fluxmq_result.error_code,
                    error_message: fluxmq_result.error_message,
                    resource_type: fluxmq_result.resource_type,
                    resource_name: fluxmq_result.resource_name,
                    configs: kafka_configs,
                }
            })
            .collect();

        Ok(KafkaDescribeConfigsResponse {
            correlation_id,
            throttle_time_ms: fluxmq_resp.throttle_time_ms,
            results: kafka_results,
        })
    }

    // ========================================================================
    // ALTER CONFIGS REQUEST/RESPONSE CONVERSION
    // ========================================================================

    /// Convert Kafka AlterConfigs request to FluxMQ format
    fn convert_alter_configs_request(
        kafka_req: KafkaAlterConfigsRequest,
    ) -> Result<crate::protocol::AlterConfigsRequest> {
        use crate::protocol::{AlterConfigsRequest, AlterConfigsResource, AlterableConfig};

        let fluxmq_resources: Vec<AlterConfigsResource> = kafka_req
            .resources
            .into_iter()
            .map(|kafka_resource| {
                let fluxmq_configs: Vec<AlterableConfig> = kafka_resource
                    .configs
                    .into_iter()
                    .map(|kafka_config| AlterableConfig {
                        name: kafka_config.name,
                        value: kafka_config.value,
                    })
                    .collect();

                AlterConfigsResource {
                    resource_type: kafka_resource.resource_type,
                    resource_name: kafka_resource.resource_name,
                    configs: fluxmq_configs,
                }
            })
            .collect();

        Ok(AlterConfigsRequest {
            correlation_id: kafka_req.correlation_id,
            resources: fluxmq_resources,
            validate_only: kafka_req.validate_only,
        })
    }

    /// Convert FluxMQ AlterConfigs response to Kafka format
    fn convert_alter_configs_response(
        fluxmq_resp: crate::protocol::AlterConfigsResponse,
        correlation_id: i32,
    ) -> Result<KafkaAlterConfigsResponse> {
        let kafka_responses: Vec<KafkaAlterConfigsResourceResponse> = fluxmq_resp
            .responses
            .into_iter()
            .map(|fluxmq_response| KafkaAlterConfigsResourceResponse {
                error_code: fluxmq_response.error_code,
                error_message: fluxmq_response.error_message,
                resource_type: fluxmq_response.resource_type,
                resource_name: fluxmq_response.resource_name,
            })
            .collect();

        Ok(KafkaAlterConfigsResponse {
            correlation_id,
            throttle_time_ms: fluxmq_resp.throttle_time_ms,
            responses: kafka_responses,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{BrokerMetadata, PartitionMetadata, TopicMetadata};

    #[test]
    fn test_produce_request_conversion() {
        let kafka_req = KafkaProduceRequest {
            header: KafkaRequestHeader {
                api_key: 0,
                api_version: 1,
                correlation_id: 123,
                client_id: Some("test-client".to_string()),
            },
            transactional_id: None,
            acks: 1,
            timeout_ms: 1000,
            topic_data: vec![KafkaTopicProduceData {
                topic: "test-topic".to_string(),
                partition_data: vec![KafkaPartitionProduceData {
                    partition: 0,
                    records: Some(Bytes::from("test-data")),
                }],
            }],
        };

        let fluxmq_req = ProtocolAdapter::convert_produce_request(kafka_req).unwrap();

        assert_eq!(fluxmq_req.correlation_id, 123);
        assert_eq!(fluxmq_req.topic, "test-topic");
        assert_eq!(fluxmq_req.partition, 0);
        assert_eq!(fluxmq_req.acks, 1);
        assert_eq!(fluxmq_req.timeout_ms, 1000);
        assert!(!fluxmq_req.messages.is_empty());
    }

    #[test]
    fn test_metadata_response_conversion() {
        let fluxmq_resp = MetadataResponse {
            correlation_id: 456,
            brokers: vec![BrokerMetadata {
                node_id: 0,
                host: "localhost".to_string(),
                port: 9092,
            }],
            topics: vec![TopicMetadata {
                name: "test-topic".to_string(),
                error_code: 0,
                partitions: vec![PartitionMetadata {
                    id: 0,
                    leader: Some(0),
                    replicas: vec![0],
                    isr: vec![0],
                    leader_epoch: 0,
                }],
            }],
            api_version: 7,
        };

        let kafka_resp = ProtocolAdapter::convert_metadata_response(fluxmq_resp, 456).unwrap();

        assert_eq!(kafka_resp.header.correlation_id, 456);
        assert_eq!(kafka_resp.brokers.len(), 1);
        assert_eq!(kafka_resp.topics.len(), 1);
        assert_eq!(kafka_resp.topics[0].topic, "test-topic");
        assert_eq!(kafka_resp.topics[0].partitions.len(), 1);
    }

    #[test]
    fn test_error_code_mapping() {
        assert_eq!(
            ProtocolAdapter::map_error_code(0),
            KafkaErrorCode::NoError.as_i16()
        );
        assert_eq!(
            ProtocolAdapter::map_error_code(1),
            KafkaErrorCode::OffsetOutOfRange.as_i16()
        );
        assert_eq!(
            ProtocolAdapter::map_error_code(3),
            KafkaErrorCode::UnknownTopicOrPartition.as_i16()
        );
        assert_eq!(
            ProtocolAdapter::map_error_code(999),
            KafkaErrorCode::Unknown.as_i16()
        );
    }
}
