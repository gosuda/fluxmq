//! Protocol Adapter
//!
//! This module provides conversion between Kafka protocol messages
//! and FluxMQ's internal message format, enabling protocol compatibility
//! while maintaining FluxMQ's optimized internal structures.

use bytes::Bytes;
use std::collections::HashMap;
use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

use super::errors::KafkaErrorCode;
use super::messages::*;
use crate::consumer::{ConsumerGroupMessage, TopicPartition};
use crate::protocol::{
    DeleteTopicsRequest, DeleteTopicsResponse, FetchRequest, FetchResponse, ListOffsetsRequest,
    ListOffsetsResponse, Message, MetadataRequest, MetadataResponse, MultiFetchRequest,
    MultiFetchResponse, ProduceRequest, ProduceResponse, Request, Response,
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};
use bytes::BufMut;

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
            KafkaRequest::GetTelemetrySubscriptions(_req) => {
                // GET_TELEMETRY_SUBSCRIPTIONS is handled internally, no FluxMQ conversion needed
                Err(AdapterError::UnsupportedOperation(
                    "GET_TELEMETRY_SUBSCRIPTIONS handled internally".to_string(),
                ))
            }
            KafkaRequest::LeaderAndIsr(_req) => {
                // LEADER_AND_ISR: Cluster management API handled separately
                // This API is used for leader election and replica assignment
                Err(AdapterError::UnsupportedApi(6, 0))
            }
            KafkaRequest::StopReplica(_req) => {
                // STOP_REPLICA: Cluster management API handled separately
                // This API is used for stopping and removing replicas
                Err(AdapterError::UnsupportedApi(5, 0))
            }
            KafkaRequest::UpdateMetadata(_req) => {
                // UPDATE_METADATA: Cluster management API handled separately
                // This API is used for cluster metadata synchronization
                Err(AdapterError::UnsupportedApi(6, 0))
            }
            KafkaRequest::ControlledShutdown(_req) => {
                // CONTROLLED_SHUTDOWN: Cluster management API handled separately
                // This API is used for graceful broker shutdown coordination
                Err(AdapterError::UnsupportedApi(7, 0))
            }
            // Transaction APIs - handled by transaction coordinator
            KafkaRequest::InitProducerId(_req) => {
                // InitProducerId: Transaction coordinator API
                Err(AdapterError::UnsupportedApi(22, 0))
            }
            KafkaRequest::AddPartitionsToTxn(_req) => {
                // AddPartitionsToTxn: Transaction coordinator API
                Err(AdapterError::UnsupportedApi(24, 0))
            }
            KafkaRequest::AddOffsetsToTxn(_req) => {
                // AddOffsetsToTxn: Transaction coordinator API
                Err(AdapterError::UnsupportedApi(25, 0))
            }
            KafkaRequest::EndTxn(_req) => {
                // EndTxn: Transaction coordinator API
                Err(AdapterError::UnsupportedApi(26, 0))
            }
            KafkaRequest::WriteTxnMarkers(_req) => {
                // WriteTxnMarkers: Transaction coordinator API
                Err(AdapterError::UnsupportedApi(27, 0))
            }
            KafkaRequest::TxnOffsetCommit(_req) => {
                // TxnOffsetCommit: Transaction coordinator API
                Err(AdapterError::UnsupportedApi(28, 0))
            }
            // Admin APIs
            KafkaRequest::DeleteRecords(_req) => {
                // DeleteRecords: Admin API handled separately
                Err(AdapterError::UnsupportedApi(21, 0))
            }
            KafkaRequest::CreatePartitions(_req) => {
                // CreatePartitions: Admin API handled separately
                Err(AdapterError::UnsupportedApi(37, 0))
            }
            KafkaRequest::DeleteGroups(_req) => {
                // DeleteGroups: Admin API handled separately
                Err(AdapterError::UnsupportedApi(42, 0))
            }
            KafkaRequest::AlterPartitionReassignments(_req) => {
                // AlterPartitionReassignments: Admin API handled separately
                Err(AdapterError::UnsupportedApi(45, 0))
            }
            KafkaRequest::IncrementalAlterConfigs(_req) => {
                // IncrementalAlterConfigs: Admin API handled separately
                Err(AdapterError::UnsupportedApi(44, 0))
            }
            // New Admin APIs (Kafka 4.1.0 compatibility)
            KafkaRequest::ListPartitionReassignments(_req) => {
                // ListPartitionReassignments: Admin API handled separately
                Err(AdapterError::UnsupportedApi(46, 0))
            }
            KafkaRequest::OffsetDelete(_req) => {
                // OffsetDelete: Admin API handled separately
                Err(AdapterError::UnsupportedApi(47, 0))
            }
            KafkaRequest::DescribeCluster(_req) => {
                // DescribeCluster: Admin API handled separately
                Err(AdapterError::UnsupportedApi(60, 0))
            }
            KafkaRequest::DescribeProducers(_req) => {
                // DescribeProducers: Admin API handled separately
                Err(AdapterError::UnsupportedApi(61, 0))
            }
            KafkaRequest::PushTelemetry(_req) => {
                // PushTelemetry: Telemetry API handled separately
                Err(AdapterError::UnsupportedApi(72, 0))
            }
            // Phase 2 APIs
            KafkaRequest::OffsetForLeaderEpoch(_req) => {
                // OffsetForLeaderEpoch: Replication API handled separately
                Err(AdapterError::UnsupportedApi(23, 0))
            }
            KafkaRequest::DescribeAcls(_req) => {
                // DescribeAcls: ACL API handled separately
                Err(AdapterError::UnsupportedApi(29, 0))
            }
            KafkaRequest::CreateAcls(_req) => {
                // CreateAcls: ACL API handled separately
                Err(AdapterError::UnsupportedApi(30, 0))
            }
            KafkaRequest::DeleteAcls(_req) => {
                // DeleteAcls: ACL API handled separately
                Err(AdapterError::UnsupportedApi(31, 0))
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
                let kafka_resp = Self::convert_produce_response(resp, correlation_id, 0)?; // v0 for legacy adapter path
                Ok(KafkaResponse::Produce(kafka_resp))
            }
            Response::Fetch(resp) => {
                let kafka_resp = Self::convert_fetch_response(resp, correlation_id, 11)?; // v11 for preferred_read_replica
                Ok(KafkaResponse::Fetch(kafka_resp))
            }
            Response::MultiFetch(resp) => {
                let kafka_resp = Self::convert_multi_fetch_response(resp, correlation_id, 11)?; // v11 for preferred_read_replica
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
                let kafka_resp = Self::convert_create_topics_response(resp, correlation_id, 0)?; // v0 for legacy adapter path
                Ok(KafkaResponse::CreateTopics(kafka_resp))
            }
            Response::DeleteTopics(resp) => {
                let kafka_resp = Self::convert_delete_topics_response(resp, correlation_id, 0)?; // v0 for legacy adapter path
                Ok(KafkaResponse::DeleteTopics(kafka_resp))
            }
            Response::DescribeConfigs(resp) => {
                let kafka_resp = Self::convert_describe_configs_response(resp, correlation_id, 0)?; // v0 for legacy adapter path
                Ok(KafkaResponse::DescribeConfigs(kafka_resp))
            }
            Response::AlterConfigs(resp) => {
                let kafka_resp = Self::convert_alter_configs_response(resp, correlation_id, 0)?; // v0 for legacy adapter path
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
            // Consumer Group APIs - Direct conversion since structures match
            Response::FindCoordinator(resp) => {
                let kafka_resp = KafkaFindCoordinatorResponse {
                    header: KafkaResponseHeader {
                        correlation_id: resp.correlation_id,
                    },
                    api_version: 0, // Default to non-flexible, will be overridden if needed
                    throttle_time_ms: resp.throttle_time_ms,
                    error_code: resp.error_code,
                    error_message: resp.error_message.clone(),
                    node_id: resp.node_id,
                    host: resp.host.clone(),
                    port: resp.port,
                };
                Ok(KafkaResponse::FindCoordinator(kafka_resp))
            }
            Response::JoinGroup(resp) => {
                let kafka_resp = KafkaJoinGroupResponse {
                    header: KafkaResponseHeader {
                        correlation_id: resp.correlation_id,
                    },
                    api_version: 5, // Default to v5 (protocol_type not included)
                    throttle_time_ms: resp.throttle_time_ms,
                    error_code: resp.error_code,
                    generation_id: resp.generation_id,
                    protocol_type: resp.protocol_type.clone().unwrap_or_default(),
                    protocol_name: resp.protocol_name.clone(),
                    leader: resp.leader.clone(),
                    member_id: resp.member_id.clone(),
                    members: resp
                        .members
                        .iter()
                        .map(|m| KafkaJoinGroupMember {
                            member_id: m.member_id.clone(),
                            group_instance_id: m.group_instance_id.clone(),
                            metadata: m.metadata.clone(),
                        })
                        .collect(),
                };
                Ok(KafkaResponse::JoinGroup(kafka_resp))
            }
            Response::SyncGroup(resp) => {
                let kafka_resp = KafkaSyncGroupResponse {
                    header: KafkaResponseHeader {
                        correlation_id: resp.correlation_id,
                    },
                    api_version: 0, // Default to v0 for compatibility
                    throttle_time_ms: resp.throttle_time_ms,
                    error_code: resp.error_code,
                    protocol_type: resp.protocol_type.clone().unwrap_or_default(),
                    protocol_name: resp.protocol_name.clone().unwrap_or_default(),
                    assignment: resp.assignment.clone(),
                };
                Ok(KafkaResponse::SyncGroup(kafka_resp))
            }
            Response::Heartbeat(resp) => {
                let kafka_resp = KafkaHeartbeatResponse {
                    header: KafkaResponseHeader {
                        correlation_id: resp.correlation_id,
                    },
                    api_version: resp.api_version,
                    throttle_time_ms: resp.throttle_time_ms,
                    error_code: resp.error_code,
                };
                Ok(KafkaResponse::Heartbeat(kafka_resp))
            }
            Response::LeaveGroup(resp) => {
                // KafkaLeaveGroupResponse doesn't have members field in v0-v2
                // Only return error code (stub implementation returns error anyway)
                let kafka_resp = KafkaLeaveGroupResponse {
                    header: KafkaResponseHeader {
                        correlation_id: resp.correlation_id,
                    },
                    api_version: resp.api_version,
                    throttle_time_ms: resp.throttle_time_ms,
                    error_code: resp.error_code,
                };
                Ok(KafkaResponse::LeaveGroup(kafka_resp))
            }
            Response::OffsetCommit(resp) => {
                let kafka_resp = KafkaOffsetCommitResponse {
                    header: KafkaResponseHeader {
                        correlation_id: resp.correlation_id,
                    },
                    api_version: resp.api_version,
                    throttle_time_ms: resp.throttle_time_ms,
                    topics: resp
                        .topics
                        .iter()
                        .map(|t| KafkaOffsetCommitTopicResponse {
                            topic: t.name.clone(),
                            partitions: t
                                .partitions
                                .iter()
                                .map(|p| KafkaOffsetCommitPartitionResponse {
                                    partition: p.partition_index,
                                    error_code: p.error_code,
                                })
                                .collect(),
                        })
                        .collect(),
                };
                Ok(KafkaResponse::OffsetCommit(kafka_resp))
            }
            Response::OffsetFetch(resp) => {
                let kafka_resp = KafkaOffsetFetchResponse {
                    header: KafkaResponseHeader {
                        correlation_id: resp.correlation_id,
                    },
                    api_version: resp.api_version,
                    throttle_time_ms: resp.throttle_time_ms,
                    topics: resp
                        .topics
                        .iter()
                        .map(|t| KafkaOffsetFetchTopicResponse {
                            topic: t.name.clone(),
                            partitions: t
                                .partitions
                                .iter()
                                .map(|p| KafkaOffsetFetchPartitionResponse {
                                    partition: p.partition_index,
                                    offset: p.committed_offset,
                                    leader_epoch: p.committed_leader_epoch,
                                    metadata: p.metadata.clone(),
                                    error_code: p.error_code,
                                })
                                .collect(),
                        })
                        .collect(),
                    error_code: resp.error_code,
                };
                Ok(KafkaResponse::OffsetFetch(kafka_resp))
            }
            Response::DescribeGroups(resp) => {
                let kafka_resp = KafkaDescribeGroupsResponse {
                    header: KafkaResponseHeader {
                        correlation_id: resp.correlation_id,
                    },
                    api_version: resp.api_version,
                    throttle_time_ms: resp.throttle_time_ms,
                    groups: resp
                        .groups
                        .iter()
                        .map(|g| KafkaDescribedGroup {
                            error_code: g.error_code,
                            group_id: g.group_id.clone(),
                            group_state: g.group_state.clone(),
                            protocol_type: g.protocol_type.clone(),
                            protocol_data: g.protocol_data.clone(),
                            members: g
                                .members
                                .iter()
                                .map(|m| KafkaDescribedGroupMember {
                                    member_id: m.member_id.clone(),
                                    group_instance_id: m.group_instance_id.clone(),
                                    client_id: m.client_id.clone(),
                                    client_host: m.client_host.clone(),
                                    member_metadata: m.member_metadata.clone(),
                                    member_assignment: m.member_assignment.clone(),
                                })
                                .collect(),
                            authorized_operations: g.authorized_operations,
                        })
                        .collect(),
                };
                Ok(KafkaResponse::DescribeGroups(kafka_resp))
            }
            Response::ListGroups(resp) => {
                let kafka_resp = KafkaListGroupsResponse {
                    header: KafkaResponseHeader {
                        correlation_id: resp.correlation_id,
                    },
                    api_version: resp.api_version,
                    throttle_time_ms: resp.throttle_time_ms,
                    error_code: resp.error_code,
                    groups: resp
                        .groups
                        .iter()
                        .map(|g| KafkaListedGroup {
                            group_id: g.group_id.clone(),
                            protocol_type: g.protocol_type.clone(),
                            group_state: g.group_state.clone(),
                        })
                        .collect(),
                };
                Ok(KafkaResponse::ListGroups(kafka_resp))
            }
            // Transaction APIs - Direct pass-through since handler returns Kafka format
            Response::InitProducerId(resp) => {
                Ok(KafkaResponse::InitProducerId(KafkaInitProducerIdResponse {
                    correlation_id: resp.correlation_id,
                    error_code: resp.error_code,
                    producer_id: resp.producer_id,
                    producer_epoch: resp.producer_epoch,
                    throttle_time_ms: resp.throttle_time_ms,
                    api_version: resp.api_version,
                }))
            }
            Response::AddPartitionsToTxn(resp) => Ok(KafkaResponse::AddPartitionsToTxn(
                KafkaAddPartitionsToTxnResponse {
                    correlation_id: resp.correlation_id,
                    api_version: resp.api_version,
                    throttle_time_ms: resp.throttle_time_ms,
                    results: resp
                        .results
                        .iter()
                        .map(|t| KafkaAddPartitionsToTxnTopicResult {
                            name: t.name.clone(),
                            results: t
                                .results
                                .iter()
                                .map(|p| KafkaAddPartitionsToTxnPartitionResult {
                                    partition_index: p.partition_index,
                                    error_code: p.error_code,
                                })
                                .collect(),
                        })
                        .collect(),
                },
            )),
            Response::AddOffsetsToTxn(resp) => Ok(KafkaResponse::AddOffsetsToTxn(
                KafkaAddOffsetsToTxnResponse {
                    correlation_id: resp.correlation_id,
                    throttle_time_ms: resp.throttle_time_ms,
                    error_code: resp.error_code,
                },
            )),
            Response::EndTxn(resp) => Ok(KafkaResponse::EndTxn(KafkaEndTxnResponse {
                correlation_id: resp.correlation_id,
                api_version: resp.api_version,
                throttle_time_ms: resp.throttle_time_ms,
                error_code: resp.error_code,
            })),
            Response::WriteTxnMarkers(resp) => Ok(KafkaResponse::WriteTxnMarkers(
                KafkaWriteTxnMarkersResponse {
                    correlation_id: resp.correlation_id,
                    markers: resp
                        .markers
                        .iter()
                        .map(|m| KafkaWritableTxnMarkerResult {
                            producer_id: m.producer_id,
                            topics: m
                                .topics
                                .iter()
                                .map(|t| KafkaWritableTxnMarkerTopicResult {
                                    name: t.name.clone(),
                                    partitions: t
                                        .partitions
                                        .iter()
                                        .map(|p| KafkaWritableTxnMarkerPartitionResult {
                                            partition_index: p.partition_index,
                                            error_code: p.error_code,
                                        })
                                        .collect(),
                                })
                                .collect(),
                        })
                        .collect(),
                },
            )),
            Response::TxnOffsetCommit(resp) => Ok(KafkaResponse::TxnOffsetCommit(
                KafkaTxnOffsetCommitResponse {
                    correlation_id: resp.correlation_id,
                    throttle_time_ms: resp.throttle_time_ms,
                    topics: resp
                        .topics
                        .iter()
                        .map(|t| KafkaTxnOffsetCommitResponseTopic {
                            name: t.name.clone(),
                            partitions: t
                                .partitions
                                .iter()
                                .map(|p| KafkaTxnOffsetCommitResponsePartition {
                                    partition_index: p.partition_index,
                                    error_code: p.error_code,
                                })
                                .collect(),
                        })
                        .collect(),
                },
            )),
            Response::NoResponse => {
                // For fire-and-forget requests (acks=0), return error to signal no response
                Err(AdapterError::FluxMq(
                    "Fire-and-forget request - no response should be sent".to_string(),
                ))
            }
        }
    }

    /// Check if request is a consumer group request without taking ownership
    pub fn is_consumer_group_request(kafka_request: &KafkaRequest) -> bool {
        matches!(
            kafka_request,
            KafkaRequest::JoinGroup(_)
                | KafkaRequest::OffsetCommit(_)
                | KafkaRequest::OffsetFetch(_)
                | KafkaRequest::FindCoordinator(_)
                | KafkaRequest::Heartbeat(_)
                | KafkaRequest::LeaveGroup(_)
                | KafkaRequest::SyncGroup(_)
                | KafkaRequest::DescribeGroups(_)
                | KafkaRequest::ListGroups(_)
        )
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
                    api_version: 5, // Default to v5 (protocol_type not included)
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
                    api_version,
                )?;
                Ok(KafkaResponse::OffsetCommit(resp))
            }
            ConsumerGroupMessage::OffsetFetchResponse {
                error_code,
                offsets,
            } => {
                let resp = Self::convert_offset_fetch_response(
                    error_code,
                    offsets,
                    correlation_id,
                    api_version,
                )?;
                Ok(KafkaResponse::OffsetFetch(resp))
            }
            ConsumerGroupMessage::ListGroupsResponse { .. } => {
                Self::convert_list_groups_response(fluxmq_response, correlation_id, api_version)
            }
            ConsumerGroupMessage::DescribeGroupsResponse { .. } => {
                Self::convert_describe_groups_response(fluxmq_response, correlation_id, api_version)
            }
            ConsumerGroupMessage::HeartbeatResponse { error_code } => {
                let resp = KafkaHeartbeatResponse {
                    header: KafkaResponseHeader { correlation_id },
                    api_version,
                    throttle_time_ms: 0,
                    error_code,
                };
                Ok(KafkaResponse::Heartbeat(resp))
            }
            ConsumerGroupMessage::LeaveGroupResponse { error_code } => {
                let resp = KafkaLeaveGroupResponse {
                    header: KafkaResponseHeader { correlation_id },
                    api_version,
                    throttle_time_ms: 0,
                    error_code,
                };
                Ok(KafkaResponse::LeaveGroup(resp))
            }
            ConsumerGroupMessage::SyncGroupResponse {
                error_code,
                assignment,
            } => {
                // Serialize TopicPartitions to Kafka MemberAssignment format
                // MemberAssignment format:
                //   Version (int16): 0
                //   PartitionAssignment[] (array):
                //     topic (string)
                //     partitions (int32 array)
                //   UserData (bytes): null/empty

                let assignment_bytes = Self::serialize_member_assignment(&assignment);

                let resp = KafkaSyncGroupResponse {
                    header: KafkaResponseHeader { correlation_id },
                    api_version,
                    throttle_time_ms: 0,
                    error_code,
                    protocol_type: "consumer".to_string(),
                    protocol_name: "range".to_string(),
                    assignment: assignment_bytes,
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

    /// Deserialize Kafka MemberAssignment bytes to Vec<TopicPartition>
    /// MemberAssignment format (ConsumerProtocol):
    ///   Version (int16): 0
    ///   PartitionAssignment[] (array):
    ///     topic (string)
    ///     partitions (int32 array)
    ///   UserData (bytes): can be null (-1 length)
    pub fn deserialize_member_assignment(data: &Bytes) -> Vec<TopicPartition> {
        use bytes::Buf;

        info!(
            "📦 DESERIALIZE: Received assignment bytes: {:?} ({} bytes)",
            data.as_ref(),
            data.len()
        );

        if data.is_empty() {
            info!("📦 DESERIALIZE: Empty assignment data");
            return Vec::new();
        }

        let mut cursor = std::io::Cursor::new(data.as_ref());

        // Check minimum size (version + array length = 6 bytes)
        if cursor.remaining() < 6 {
            info!(
                "📦 DESERIALIZE: Assignment data too small: {} bytes",
                cursor.remaining()
            );
            return Vec::new();
        }

        // Version (int16) - ConsumerProtocol versions 0-3 have same format
        let version = cursor.get_i16();
        info!("📦 DESERIALIZE: Assignment version: {}", version);

        // Topics array length
        let topics_count = cursor.get_i32();
        info!("📦 DESERIALIZE: Topics count: {}", topics_count);
        if topics_count <= 0 {
            info!("📦 DESERIALIZE: No topics in assignment");
            return Vec::new();
        }

        let mut result = Vec::new();

        for i in 0..topics_count {
            // Topic string (i16 length prefix + bytes)
            if cursor.remaining() < 2 {
                info!("📦 DESERIALIZE: Not enough data for topic {} length", i);
                break;
            }
            let topic_len = cursor.get_i16() as usize;
            info!(
                "📦 DESERIALIZE: Topic {} name length: {}, remaining: {}",
                i,
                topic_len,
                cursor.remaining()
            );
            if cursor.remaining() < topic_len {
                info!("📦 DESERIALIZE: Not enough data for topic {} name", i);
                break;
            }
            let mut topic_bytes = vec![0u8; topic_len];
            cursor.copy_to_slice(&mut topic_bytes);
            let topic = String::from_utf8_lossy(&topic_bytes).into_owned();
            info!("📦 DESERIALIZE: Topic {}: '{}'", i, topic);

            // Partitions array
            if cursor.remaining() < 4 {
                info!("📦 DESERIALIZE: Not enough data for partitions array length");
                break;
            }
            let partitions_count = cursor.get_i32();
            info!(
                "📦 DESERIALIZE: Topic '{}' has {} partitions",
                topic, partitions_count
            );

            for p in 0..partitions_count {
                if cursor.remaining() < 4 {
                    info!(
                        "📦 DESERIALIZE: Not enough data for partition {} of topic '{}'",
                        p, topic
                    );
                    break;
                }
                let partition = cursor.get_i32();
                info!("📦 DESERIALIZE: Topic '{}' partition {}", topic, partition);
                result.push(TopicPartition {
                    topic: topic.clone(),
                    partition: partition as u32,
                });
            }
        }

        info!(
            "📦 DESERIALIZE: Final result: {} partitions from {} bytes",
            result.len(),
            data.len()
        );

        result
    }

    /// Serialize TopicPartitions to Kafka MemberAssignment format
    /// MemberAssignment format (ConsumerProtocol):
    ///   Version (int16): 0
    ///   PartitionAssignment[] (array):
    ///     topic (string)
    ///     partitions (int32 array)
    ///   UserData (bytes): null (-1 length)
    pub fn serialize_member_assignment(assignment: &[TopicPartition]) -> Bytes {
        use std::collections::HashMap;

        // If no assignment, return empty bytes (which is valid - consumer has no partitions)
        if assignment.is_empty() {
            // Return a minimal valid assignment: version=0, empty array, no userdata
            let mut buf = bytes::BytesMut::with_capacity(10);
            buf.put_i16(0); // Version
            buf.put_i32(0); // Empty topics array
            buf.put_i32(-1); // Null user data
            return buf.freeze();
        }

        // Group partitions by topic
        let mut topic_partitions: HashMap<&str, Vec<i32>> = HashMap::new();
        for tp in assignment {
            topic_partitions
                .entry(tp.topic.as_str())
                .or_default()
                .push(tp.partition as i32);
        }

        // Calculate buffer size:
        // - Version (2 bytes)
        // - Array length (4 bytes)
        // - Per topic: topic string (2 + len bytes) + partition array (4 + n*4 bytes)
        // - UserData (4 bytes for null marker)
        let mut size = 2 + 4 + 4; // version + array_len + userdata_null
        for (topic, partitions) in &topic_partitions {
            size += 2 + topic.len(); // topic string: length prefix + string bytes
            size += 4 + (partitions.len() * 4); // partitions array: length + n * 4
        }

        let mut buf = bytes::BytesMut::with_capacity(size);

        // Version
        buf.put_i16(0);

        // Topics array
        buf.put_i32(topic_partitions.len() as i32);

        for (topic, partitions) in &topic_partitions {
            // Topic string (Kafka standard string: i16 length prefix + bytes)
            buf.put_i16(topic.len() as i16);
            buf.put_slice(topic.as_bytes());

            // Partitions array
            buf.put_i32(partitions.len() as i32);
            for &partition in partitions {
                buf.put_i32(partition);
            }
        }

        // UserData (null = -1)
        buf.put_i32(-1);

        debug!(
            "Serialized member assignment: {} topics, {} partitions, {} bytes",
            topic_partitions.len(),
            assignment.len(),
            buf.len()
        );

        buf.freeze()
    }

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
            // PERFORMANCE: Skip debug logging in hot path
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
        api_version: i16,
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
            api_version,
            responses: vec![topic_response],
            throttle_time_ms: 0,
        })
    }

    fn convert_fetch_response(
        fluxmq_resp: FetchResponse,
        correlation_id: i32,
        api_version: i16,
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
            api_version,
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
            api_version: 0, // Default to non-flexible for legacy adapter path
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
        // Parse assignments from leader's SyncGroup request
        // The leader sends assignments for all members in the group
        let mut group_assignments = std::collections::HashMap::new();

        for assignment in kafka_req.assignments {
            let partitions = Self::deserialize_member_assignment(&assignment.assignment);
            debug!(
                "SyncGroup assignment for consumer '{}': {} partitions, {} raw bytes",
                assignment.consumer_id,
                partitions.len(),
                assignment.assignment.len()
            );
            group_assignments.insert(assignment.consumer_id, partitions);
        }

        debug!(
            "Converted SyncGroup request: group_id='{}', consumer_id='{}', generation={}, {} member assignments",
            kafka_req.group_id,
            kafka_req.consumer_id,
            kafka_req.generation_id,
            group_assignments.len()
        );

        Ok(ConsumerGroupMessage::SyncGroup {
            group_id: kafka_req.group_id,
            consumer_id: kafka_req.consumer_id,
            generation_id: kafka_req.generation_id,
            group_assignments,
        })
    }

    fn convert_offset_commit_response(
        _error_code: i16,
        topic_partition_errors: Vec<crate::consumer::TopicPartitionError>,
        correlation_id: i32,
        api_version: i16,
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
            api_version,
            throttle_time_ms: 0,
            topics: topic_responses.into_values().collect(),
        })
    }

    fn convert_offset_fetch_response(
        global_error_code: i16,
        offsets: Vec<crate::consumer::TopicPartitionOffsetResult>,
        correlation_id: i32,
        api_version: i16,
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
            api_version,
            throttle_time_ms: 0,
            topics: topic_responses.into_values().collect(),
            error_code: global_error_code,
        })
    }

    fn convert_create_topics_response(
        fluxmq_resp: crate::protocol::CreateTopicsResponse,
        correlation_id: i32,
        api_version: i16,
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
            api_version,
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
        api_version: i16,
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
            api_version,
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

    /// Parse Kafka record batch into FluxMQ messages with compression support
    pub fn parse_kafka_record_batch(records_bytes: &Bytes) -> Result<Vec<Message>> {
        if records_bytes.is_empty() {
            return Ok(vec![]);
        }

        use crate::compression::{decompress_fast, CompressionType};

        // First, try to detect the record batch format by checking the magic byte
        // Modern Kafka uses RecordBatch format (magic byte 2), older versions use legacy format (magic 0/1)

        // RecordBatch format starts with:
        // baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1) + ...
        if records_bytes.len() >= 17 {
            let magic_byte_offset = 16; // After baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4)
            let magic = records_bytes[magic_byte_offset];

            tracing::debug!(
                "Kafka record magic byte detected: {}, buffer_len: {}",
                magic,
                records_bytes.len()
            );

            if magic == 2 {
                // Modern RecordBatch format - delegate to new parser
                return Self::parse_record_batch_v2(records_bytes);
            }
        }

        // Fallback to legacy record format parsing (magic 0/1)
        tracing::debug!("Using legacy record format parser");
        let mut messages = Vec::new();
        let mut cursor = 0usize;

        while cursor + 14 < records_bytes.len() {
            // Minimum record size
            // Parse Kafka legacy record format:
            // offset (8) + message_size (4) + crc (4) + magic (1) + attributes (1) + key_length (4) + value_length (4) + value

            // Skip offset (8 bytes)
            cursor += 8;

            // Read message size (4 bytes)
            if cursor + 4 > records_bytes.len() {
                break;
            }
            let _message_size = i32::from_be_bytes([
                records_bytes[cursor],
                records_bytes[cursor + 1],
                records_bytes[cursor + 2],
                records_bytes[cursor + 3],
            ]) as usize;
            cursor += 4;

            // Skip CRC (4 bytes)
            cursor += 4;

            // Read magic byte (1 byte)
            if cursor >= records_bytes.len() {
                break;
            }
            let magic = records_bytes[cursor];
            tracing::debug!("Legacy record magic byte: {}", magic);
            cursor += 1;

            // Read attributes (1 byte) - contains compression info
            if cursor >= records_bytes.len() {
                break;
            }
            let attributes = records_bytes[cursor];
            cursor += 1;

            // Read key_length (4 bytes) - handle null keys properly
            if cursor + 4 > records_bytes.len() {
                tracing::warn!(
                    "Insufficient data for key_length: cursor={}, buffer_len={}",
                    cursor,
                    records_bytes.len()
                );
                break;
            }
            let key_length_i32 = i32::from_be_bytes([
                records_bytes[cursor],
                records_bytes[cursor + 1],
                records_bytes[cursor + 2],
                records_bytes[cursor + 3],
            ]);
            cursor += 4;

            // Handle null key (-1 in Kafka protocol)
            if key_length_i32 < 0 {
                tracing::debug!(
                    "Null key encountered (length={}), skipping key data",
                    key_length_i32
                );
                // Null key, continue to value processing
            } else {
                // Skip key data (we're not processing keys in this simplified parser)
                let key_length = key_length_i32 as usize;
                if cursor + key_length > records_bytes.len() {
                    tracing::warn!(
                        "Insufficient data for key: cursor={}, key_length={}, buffer_len={}",
                        cursor,
                        key_length,
                        records_bytes.len()
                    );
                    break;
                }
                cursor += key_length;
            }

            // Read value_length (4 bytes)
            if cursor + 4 > records_bytes.len() {
                tracing::warn!(
                    "Insufficient data for value_length: cursor={}, buffer_len={}",
                    cursor,
                    records_bytes.len()
                );
                break;
            }
            let value_length_i32 = i32::from_be_bytes([
                records_bytes[cursor],
                records_bytes[cursor + 1],
                records_bytes[cursor + 2],
                records_bytes[cursor + 3],
            ]);
            cursor += 4;

            // Handle null value (-1 in Kafka protocol)
            if value_length_i32 < 0 {
                tracing::debug!(
                    "Null value encountered (length={}), skipping",
                    value_length_i32
                );
                // For null values, create an empty message and continue
                let message = Message {
                    timestamp: 0,
                    key: None,
                    value: bytes::Bytes::new(),
                    headers: std::collections::HashMap::new(),
                };
                messages.push(message);
                continue;
            }

            let value_length = value_length_i32 as usize;

            // Read value data
            if cursor + value_length > records_bytes.len() {
                tracing::warn!(
                    "Insufficient data for value: cursor={}, value_length={}, buffer_len={}",
                    cursor,
                    value_length,
                    records_bytes.len()
                );
                break;
            }
            let value_data = &records_bytes[cursor..cursor + value_length];
            cursor += value_length;

            // Check compression type from attributes (lower 3 bits)
            let compression_type = match attributes & 0x07 {
                0 => CompressionType::None,   // No compression
                1 => CompressionType::Gzip,   // GZIP
                2 => CompressionType::Snappy, // Snappy
                3 => CompressionType::Lz4,    // LZ4
                4 => CompressionType::Zstd,   // ZSTD
                _ => {
                    tracing::warn!(
                        "Unknown compression type: {}, treating as uncompressed",
                        attributes & 0x07
                    );
                    CompressionType::None
                }
            };

            // Decompress if needed
            let final_value = match compression_type {
                CompressionType::None => Bytes::copy_from_slice(value_data),
                _ => match decompress_fast(value_data, compression_type, None) {
                    Ok(decompressed) => decompressed,
                    Err(e) => {
                        tracing::warn!("Decompression failed: {}, using compressed data as-is", e);
                        Bytes::copy_from_slice(value_data)
                    }
                },
            };

            // Create FluxMQ message
            let message = Message {
                key: None, // Assuming null key for simplicity
                value: final_value,
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                headers: HashMap::new(),
            };

            messages.push(message);
        }

        // Fallback: if parsing failed, create single message from raw data
        if messages.is_empty() && !records_bytes.is_empty() {
            tracing::warn!("Failed to parse Kafka records, creating single message from raw data");
            let message = Message {
                key: None,
                value: records_bytes.clone(),
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                headers: HashMap::new(),
            };
            messages.push(message);
        }

        tracing::debug!(
            "Parsed {} messages from Kafka record batch with compression support",
            messages.len()
        );
        Ok(messages)
    }

    /// Parse modern Kafka RecordBatch format (magic byte 2)
    /// RecordBatch format introduced in Kafka 0.11.0
    fn parse_record_batch_v2(records_bytes: &Bytes) -> Result<Vec<Message>> {
        tracing::debug!(
            "🔍 JAVA DEBUG: Parsing RecordBatch v2 format (magic=2), buffer_len: {}",
            records_bytes.len()
        );

        if records_bytes.len() < 61 {
            tracing::warn!(
                "RecordBatch buffer too small: {} bytes (minimum 61 required)",
                records_bytes.len()
            );
            return Ok(vec![]);
        }

        let mut messages = Vec::new();

        // RecordBatch format:
        // baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1) +
        // crc(4) + attributes(2) + lastOffsetDelta(4) + firstTimestamp(8) +
        // maxTimestamp(8) + producerId(8) + producerEpoch(2) + baseSequence(4) +
        // recordsCount(4) + records...

        // Skip to magic byte (already verified as 2)
        let mut cursor = 16usize;
        let magic = records_bytes[cursor];
        cursor += 1;

        if magic != 2 {
            return Err(AdapterError::InvalidFormat(format!(
                "Expected magic byte 2, got {}",
                magic
            )));
        }

        // Skip CRC (4 bytes)
        cursor += 4;

        // Read attributes (2 bytes) - contains compression info
        if cursor + 2 > records_bytes.len() {
            return Err(AdapterError::InvalidFormat(
                "Buffer too small for attributes".to_string(),
            ));
        }
        let attributes = u16::from_be_bytes([records_bytes[cursor], records_bytes[cursor + 1]]);
        cursor += 2;

        let compression_type = attributes & 0x7; // Lower 3 bits

        tracing::debug!(
            "RecordBatch attributes: {:#06x}, compression_type: {}",
            attributes,
            compression_type
        );

        // Skip: lastOffsetDelta(4) + firstTimestamp(8) + maxTimestamp(8) +
        //       producerId(8) + producerEpoch(2) + baseSequence(4)
        cursor += 34;

        // Read records count
        if cursor + 4 > records_bytes.len() {
            return Err(AdapterError::InvalidFormat(
                "Buffer too small for records count".to_string(),
            ));
        }
        let records_count = i32::from_be_bytes([
            records_bytes[cursor],
            records_bytes[cursor + 1],
            records_bytes[cursor + 2],
            records_bytes[cursor + 3],
        ]);
        cursor += 4;

        tracing::debug!(
            "🔍 JAVA DEBUG: RecordBatch contains {} records",
            records_count
        );

        if records_count <= 0 {
            tracing::warn!(
                "🔍 JAVA DEBUG: RecordBatch has no records: {}",
                records_count
            );
            return Ok(messages);
        }

        // Get the remaining records data - add defensive bounds check
        if cursor > records_bytes.len() {
            return Err(AdapterError::InvalidFormat(format!(
                "Cursor position {} exceeds buffer length {}",
                cursor,
                records_bytes.len()
            )));
        }
        let records_data = &records_bytes[cursor..];

        // Decompress records if necessary
        let decompressed_data = match compression_type {
            0 => {
                // No compression
                tracing::debug!("No compression, using records data directly");
                records_data.to_vec()
            }
            1 => {
                // Gzip compression
                tracing::debug!("Decompressing Gzip compressed records");
                Self::decompress_gzip(records_data)?
            }
            2 => {
                // Snappy compression
                tracing::debug!("Decompressing Snappy compressed records");
                Self::decompress_snappy(records_data)?
            }
            3 => {
                // LZ4 compression
                tracing::debug!("Decompressing LZ4 compressed records");
                Self::decompress_lz4(records_data)?
            }
            4 => {
                // ZSTD compression
                tracing::debug!("Decompressing ZSTD compressed records");
                Self::decompress_zstd(records_data)?
            }
            _ => {
                return Err(AdapterError::InvalidFormat(format!(
                    "Unsupported compression type: {}",
                    compression_type
                )));
            }
        };

        // Convert decompressed data to Bytes for parsing
        let decompressed_bytes = Bytes::from(decompressed_data);
        let mut decompressed_cursor = 0usize;

        tracing::trace!(
            "🔍 DECOMPRESSED DEBUG: {} bytes of records data (compression_type={})",
            decompressed_bytes.len(),
            compression_type
        );

        // PERFORMANCE: Only generate hex dump at trace level (disabled in production)
        #[cfg(debug_assertions)]
        if tracing::enabled!(tracing::Level::TRACE) {
            let debug_bytes = decompressed_bytes.len().min(32);
            let hex_data: String = decompressed_bytes[0..debug_bytes]
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");
            tracing::trace!("🔍 DECOMPRESSED HEX: {}", hex_data);
        }

        // PERFORMANCE: Pre-allocate messages vector based on expected count
        messages.reserve(records_count as usize);

        // Parse individual records from decompressed data
        for _i in 0..records_count {
            // PERFORMANCE: Removed per-record debug logging in hot path
            match Self::parse_single_record_v2(&decompressed_bytes, &mut decompressed_cursor) {
                Ok(Some(message)) => {
                    messages.push(message);
                }
                Ok(None) => {
                    // Control record - skip silently
                }
                Err(e) => {
                    tracing::debug!("❌ Failed to parse record: {}", e);
                    // Continue parsing remaining records
                    break;
                }
            }
        }

        tracing::trace!(
            "Successfully parsed RecordBatch v2: {} messages from {} records",
            messages.len(),
            records_count
        );

        Ok(messages)
    }

    /// Parse a single record from RecordBatch v2 format
    /// PERFORMANCE: Minimal logging in hot path - all debug logs moved to trace level
    fn parse_single_record_v2(
        records_bytes: &Bytes,
        cursor: &mut usize,
    ) -> Result<Option<Message>> {
        if *cursor >= records_bytes.len() {
            return Err(AdapterError::InvalidFormat(
                "Cursor beyond buffer end".to_string(),
            ));
        }

        // Record format:
        // length(varint) + attributes(1) + timestampDelta(varint) + offsetDelta(varint) +
        // keyLength(varint) + key + valueLength(varint) + value + headersCount(varint) + headers

        // Read record length (varint)
        let record_length = Self::read_varint_from_bytes(records_bytes, cursor)?;

        if record_length <= 0 {
            return Err(AdapterError::InvalidFormat(format!(
                "Invalid record length: {}",
                record_length
            )));
        }

        // Important fix: record_length is the length of the record data AFTER the length field
        // So the record ends at: current_cursor + record_length (not record_start + record_length)
        let record_data_start = *cursor;
        let record_data_end = record_data_start + record_length as usize;

        // Defensive check: ensure we don't read beyond buffer
        if record_data_end > records_bytes.len() {
            return Err(AdapterError::InvalidFormat(format!(
                "Record extends beyond buffer: record_end={}, buffer_len={}",
                record_data_end,
                records_bytes.len()
            )));
        }

        // Read attributes (1 byte)
        if *cursor >= records_bytes.len() {
            return Err(AdapterError::InvalidFormat(
                "Buffer too small for record attributes".to_string(),
            ));
        }
        let _attributes = records_bytes[*cursor];
        *cursor += 1;

        // Skip timestampDelta and offsetDelta (varints)
        Self::read_varint_from_bytes(records_bytes, cursor)?; // timestampDelta
        Self::read_varint_from_bytes(records_bytes, cursor)?; // offsetDelta

        // Read key - PERFORMANCE: removed per-record logging
        let key_length = Self::read_varint_from_bytes(records_bytes, cursor)?;
        let key = if key_length > 0 {
            let key_len_usize = key_length.max(0) as usize;
            if *cursor + key_len_usize > records_bytes.len() {
                return Err(AdapterError::InvalidFormat(
                    "Buffer too small for record key".to_string(),
                ));
            }
            let end_pos = *cursor + key_len_usize;
            if end_pos < *cursor {
                return Err(AdapterError::InvalidFormat(
                    "Invalid key length causing slice overflow".to_string(),
                ));
            }
            // PERFORMANCE: Use slice() for zero-copy when possible
            let key_bytes = records_bytes.slice(*cursor..end_pos);
            *cursor += key_len_usize;
            Some(key_bytes)
        } else {
            None
        };

        // Read value - PERFORMANCE: removed per-record logging, use zero-copy slice
        let value_length = Self::read_varint_from_bytes(records_bytes, cursor)?;
        let value = if value_length > 0 {
            let value_len_usize = value_length.max(0) as usize;
            if *cursor + value_len_usize > records_bytes.len() {
                return Err(AdapterError::InvalidFormat(
                    "Buffer too small for record value".to_string(),
                ));
            }
            let end_pos = *cursor + value_len_usize;
            if end_pos < *cursor {
                return Err(AdapterError::InvalidFormat(
                    "Invalid value length causing slice overflow".to_string(),
                ));
            }
            // PERFORMANCE: Use slice() for zero-copy
            let value_bytes = records_bytes.slice(*cursor..end_pos);
            *cursor += value_len_usize;
            value_bytes
        } else {
            Bytes::new()
        };

        // Skip headers for now
        let headers_count = Self::read_varint_from_bytes(records_bytes, cursor)?;
        for _ in 0..headers_count {
            // Skip header key and value
            let header_key_len = Self::read_varint_from_bytes(records_bytes, cursor)?;
            *cursor += header_key_len as usize;
            let header_value_len = Self::read_varint_from_bytes(records_bytes, cursor)?;
            *cursor += header_value_len as usize;
        }

        // Ensure we're at the expected position
        if *cursor != record_data_end {
            tracing::warn!(
                "Record parsing position mismatch: expected {}, actual {}",
                record_data_end,
                *cursor
            );
            *cursor = record_data_end;
        }

        // Create FluxMQ message - PERFORMANCE: no clone needed, already have ownership
        let message = Message {
            key,
            value,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            headers: HashMap::new(),
        };

        Ok(Some(message))
    }

    /// Read a varint from bytes at cursor position
    fn read_varint_from_bytes(bytes: &Bytes, cursor: &mut usize) -> Result<i32> {
        let mut value = 0u32; // Use u32 for varint decoding
        let mut shift = 0;
        let start_cursor = *cursor;

        loop {
            if shift >= 32 {
                tracing::error!(
                    "🔍 VARINT: Varint too large at cursor {} (>32 bits)",
                    *cursor
                );
                return Err(AdapterError::InvalidFormat("Varint too large".to_string()));
            }

            if *cursor >= bytes.len() {
                tracing::error!(
                    "🔍 VARINT: Cursor {} beyond buffer len {}",
                    *cursor,
                    bytes.len()
                );
                return Err(AdapterError::InvalidFormat(
                    "Failed to read varint byte".to_string(),
                ));
            }

            let byte = bytes[*cursor];
            *cursor += 1;

            value |= ((byte & 0x7F) as u32) << shift;

            tracing::debug!(
                "🔍 VARINT: Read byte {:02x} at pos {}, value so far: {}",
                byte,
                *cursor - 1,
                value
            );

            if (byte & 0x80) == 0 {
                break;
            }

            shift += 7;
        }

        // Apply ZigZag decoding for signed varints
        let final_value = Self::zigzag_decode(value);

        tracing::debug!(
            "🔍 VARINT: Raw value: {}, ZigZag decoded: {} (cursor {} -> {})",
            value,
            final_value,
            start_cursor,
            *cursor
        );

        Ok(final_value)
    }

    /// ZigZag decode for signed varints - converts unsigned varint to signed int
    fn zigzag_decode(value: u32) -> i32 {
        ((value >> 1) as i32) ^ (-((value & 1) as i32))
    }

    /// Decompress LZ4 compressed data
    fn decompress_lz4(compressed_data: &[u8]) -> Result<Vec<u8>> {
        use std::io::Read;

        // Method 1: Try LZ4 Frame format (recommended by docs, likely what Kafka uses)
        let mut frame_decoder = lz4_flex::frame::FrameDecoder::new(compressed_data);
        let mut frame_result = Vec::new();
        if frame_decoder.read_to_end(&mut frame_result).is_ok() && !frame_result.is_empty() {
            tracing::debug!(
                "LZ4 decompression successful (frame format): {} -> {} bytes",
                compressed_data.len(),
                frame_result.len()
            );
            return Ok(frame_result);
        }

        // Method 2: Try size-prepended format
        if let Ok(decompressed) = lz4_flex::decompress_size_prepended(compressed_data) {
            tracing::debug!(
                "LZ4 decompression successful (size-prepended): {} -> {} bytes",
                compressed_data.len(),
                decompressed.len()
            );
            return Ok(decompressed);
        }

        // Method 3: Try with different potential uncompressed sizes
        for uncompressed_size in [
            1024, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288,
        ] {
            if let Ok(decompressed) = lz4_flex::decompress(compressed_data, uncompressed_size) {
                tracing::debug!(
                    "LZ4 decompression successful (estimated size {}): {} -> {} bytes",
                    uncompressed_size,
                    compressed_data.len(),
                    decompressed.len()
                );
                return Ok(decompressed);
            }
        }

        // Method 4: Try to extract uncompressed size from data prefix (Kafka-specific)
        if compressed_data.len() >= 4 {
            // Try big-endian size prefix
            let potential_size = u32::from_be_bytes([
                compressed_data[0],
                compressed_data[1],
                compressed_data[2],
                compressed_data[3],
            ]) as usize;

            // Reasonable size check (max 10MB uncompressed)
            if potential_size > 0 && potential_size < 10_485_760 {
                if let Ok(decompressed) =
                    lz4_flex::decompress(&compressed_data[4..], potential_size)
                {
                    tracing::debug!(
                        "LZ4 decompression successful (BE size prefix {}): {} -> {} bytes",
                        potential_size,
                        compressed_data.len(),
                        decompressed.len()
                    );
                    return Ok(decompressed);
                }
            }

            // Try little-endian size prefix
            let potential_size_le = u32::from_le_bytes([
                compressed_data[0],
                compressed_data[1],
                compressed_data[2],
                compressed_data[3],
            ]) as usize;

            if potential_size_le > 0 && potential_size_le < 10_485_760 {
                if let Ok(decompressed) =
                    lz4_flex::decompress(&compressed_data[4..], potential_size_le)
                {
                    tracing::debug!(
                        "LZ4 decompression successful (LE size prefix {}): {} -> {} bytes",
                        potential_size_le,
                        compressed_data.len(),
                        decompressed.len()
                    );
                    return Ok(decompressed);
                }
            }
        }

        tracing::error!(
            "LZ4 decompression failed with all methods, data size: {} bytes, first 16 bytes: {:02x?}",
            compressed_data.len(),
            compressed_data.get(..16).unwrap_or(compressed_data)
        );
        Err(AdapterError::InvalidFormat(
            "LZ4 decompression failed: unable to decompress with any method".to_string(),
        ))
    }

    /// Decompress Snappy compressed data
    fn decompress_snappy(compressed_data: &[u8]) -> Result<Vec<u8>> {
        match snap::raw::Decoder::new().decompress_vec(compressed_data) {
            Ok(decompressed) => {
                tracing::debug!(
                    "Snappy decompression successful: {} -> {} bytes",
                    compressed_data.len(),
                    decompressed.len()
                );
                Ok(decompressed)
            }
            Err(e) => {
                tracing::error!("Snappy decompression failed: {}", e);
                Err(AdapterError::InvalidFormat(format!(
                    "Snappy decompression failed: {}",
                    e
                )))
            }
        }
    }

    /// Decompress Gzip compressed data
    fn decompress_gzip(compressed_data: &[u8]) -> Result<Vec<u8>> {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let mut decoder = GzDecoder::new(compressed_data);
        let mut decompressed = Vec::new();

        match decoder.read_to_end(&mut decompressed) {
            Ok(_) => {
                tracing::debug!(
                    "Gzip decompression successful: {} -> {} bytes",
                    compressed_data.len(),
                    decompressed.len()
                );
                Ok(decompressed)
            }
            Err(e) => {
                tracing::error!("Gzip decompression failed: {}", e);
                Err(AdapterError::InvalidFormat(format!(
                    "Gzip decompression failed: {}",
                    e
                )))
            }
        }
    }

    /// Decompress ZSTD compressed data
    fn decompress_zstd(compressed_data: &[u8]) -> Result<Vec<u8>> {
        match zstd::decode_all(compressed_data) {
            Ok(decompressed) => {
                tracing::debug!(
                    "ZSTD decompression successful: {} -> {} bytes",
                    compressed_data.len(),
                    decompressed.len()
                );
                Ok(decompressed)
            }
            Err(e) => {
                tracing::error!("ZSTD decompression failed: {}", e);
                Err(AdapterError::InvalidFormat(format!(
                    "ZSTD decompression failed: {}",
                    e
                )))
            }
        }
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
        api_version: i16,
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
                    api_version,
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
        api_version: i16,
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
                    api_version,
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
        api_version: i16,
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

                        let high_watermark = partition_resp
                            .messages
                            .last()
                            .map(|(offset, _)| *offset as i64 + 1)
                            .unwrap_or(0);

                        KafkaPartitionFetchResponse {
                            partition: partition_resp.partition as i32,
                            error_code: partition_resp.error_code,
                            high_watermark,
                            last_stable_offset: high_watermark, // Must match high_watermark for read_uncommitted
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
            api_version,
            throttle_time_ms: 0,
            error_code: Self::map_error_code(fluxmq_resp.error_code),
            session_id: 0,
            responses: topic_responses,
        })
    }

    /// Encode FluxMQ messages as proper Kafka RecordBatch format (KIP-98)
    /// Uses magic byte 2 (RecordBatch v2) format for Java Kafka client 4.x compatibility
    fn encode_messages_as_record_batch(messages: &[(u64, Message)]) -> Bytes {
        if messages.is_empty() {
            return Bytes::new();
        }

        // Get base offset and last offset
        let base_offset = messages.first().map(|(o, _)| *o as i64).unwrap_or(0);
        let last_offset_delta = (messages.len() - 1) as i32;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        // First, encode all records
        let mut records_buffer = Vec::new();
        for (idx, (_, message)) in messages.iter().enumerate() {
            // Record format (varint encoded):
            // length (varint) + attributes (1 byte) + timestampDelta (varint) + offsetDelta (varint)
            // + keyLength (varint) + key + valueLength (varint) + value + headers (varint array)

            let mut record = Vec::new();

            // Attributes (1 byte) - no control records, no transactional
            record.push(0u8);

            // Timestamp delta (varint) - 0 for simplicity
            Self::write_varint(&mut record, 0);

            // Offset delta (varint)
            Self::write_varint(&mut record, idx as i64);

            // Key length (varint) - -1 for null key
            Self::write_varint(&mut record, -1);

            // No key bytes (null key)

            // Value length (varint)
            Self::write_varint(&mut record, message.value.len() as i64);

            // Value bytes
            record.extend_from_slice(&message.value);

            // Headers count (varint) - 0 headers
            Self::write_varint(&mut record, 0);

            // Write record length (varint) followed by record data
            Self::write_varint(&mut records_buffer, record.len() as i64);
            records_buffer.extend_from_slice(&record);
        }

        // Calculate CRC32C for the batch (from partition leader epoch to end of records)
        // RecordBatch header (after baseOffset and batchLength):
        // partitionLeaderEpoch(4) + magic(1) + crc(4) + attributes(2) + lastOffsetDelta(4)
        // + firstTimestamp(8) + maxTimestamp(8) + producerId(8) + producerEpoch(2)
        // + baseSequence(4) + recordCount(4) + records...

        let batch_length = 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + records_buffer.len();

        // Build the batch
        let mut batch = Vec::with_capacity(8 + 4 + batch_length);

        // Base offset (8 bytes)
        batch.extend_from_slice(&base_offset.to_be_bytes());

        // Batch length (4 bytes) - everything after this field
        batch.extend_from_slice(&(batch_length as i32).to_be_bytes());

        // Partition leader epoch (4 bytes)
        batch.extend_from_slice(&0i32.to_be_bytes());

        // Magic byte (1 byte) - 2 for RecordBatch v2
        batch.push(2u8);

        // CRC placeholder - we'll calculate this later (4 bytes)
        let crc_position = batch.len();
        batch.extend_from_slice(&0u32.to_be_bytes());

        // Attributes (2 bytes) - no compression, no timestamp type, no transactional, no control
        batch.extend_from_slice(&0i16.to_be_bytes());

        // Last offset delta (4 bytes)
        batch.extend_from_slice(&last_offset_delta.to_be_bytes());

        // First timestamp (8 bytes)
        batch.extend_from_slice(&timestamp.to_be_bytes());

        // Max timestamp (8 bytes) - same as first for simplicity
        batch.extend_from_slice(&timestamp.to_be_bytes());

        // Producer ID (8 bytes) - -1 for non-transactional
        batch.extend_from_slice(&(-1i64).to_be_bytes());

        // Producer epoch (2 bytes) - -1 for non-transactional
        batch.extend_from_slice(&(-1i16).to_be_bytes());

        // Base sequence (4 bytes) - -1 for non-idempotent
        batch.extend_from_slice(&(-1i32).to_be_bytes());

        // Record count (4 bytes)
        batch.extend_from_slice(&(messages.len() as i32).to_be_bytes());

        // Records
        batch.extend_from_slice(&records_buffer);

        // Calculate CRC32C over everything from attributes to end of records
        // CRC32C covers bytes from attributes (after magic byte and crc field) to end
        let crc_data = &batch[(crc_position + 4)..]; // Skip the CRC placeholder
        let crc = crc32c::crc32c(crc_data);

        // Update CRC in the batch
        batch[crc_position..crc_position + 4].copy_from_slice(&crc.to_be_bytes());

        tracing::debug!(
            "Created Kafka RecordBatch v2 with {} bytes for {} messages (base_offset={}, crc=0x{:08x})",
            batch.len(),
            messages.len(),
            base_offset,
            crc
        );

        Bytes::from(batch)
    }

    /// Write a signed varint (zigzag encoded) to a buffer
    fn write_varint(buf: &mut Vec<u8>, value: i64) {
        // Zigzag encode: (value << 1) ^ (value >> 63)
        let zigzag = ((value << 1) ^ (value >> 63)) as u64;
        Self::write_unsigned_varint(buf, zigzag);
    }

    /// Write an unsigned varint to a buffer
    fn write_unsigned_varint(buf: &mut Vec<u8>, mut value: u64) {
        loop {
            let byte = (value & 0x7F) as u8;
            value >>= 7;
            if value == 0 {
                buf.push(byte);
                break;
            } else {
                buf.push(byte | 0x80);
            }
        }
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
        api_version: i16,
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
            api_version,
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
        api_version: i16,
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
            api_version,
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
