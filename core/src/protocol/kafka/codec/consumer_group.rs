//! Kafka Consumer Group APIs (ApiKeys 8-16) codec implementation.

use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;
#[allow(unused_imports)]
use tracing::debug;

use super::super::messages::*;
use super::{KafkaCodecError, Result};

impl super::KafkaCodec {
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
    pub(crate) fn decode_offset_commit_request(
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
            Self::decode_array_count(cursor, "OffsetCommit topics")?
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
                Self::decode_array_count(cursor, "OffsetCommit partitions")?
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

    pub(crate) fn encode_offset_commit_response(
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

    pub(crate) fn decode_offset_fetch_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaOffsetFetchRequest> {
        let group_id = Self::decode_string(cursor)?;
        let topic_count = cursor.get_i32();

        let topics = if topic_count == -1 {
            None // Fetch all topics
        } else {
            let topic_count = topic_count.max(0).min(10_000);
            let mut topics = Vec::with_capacity(topic_count as usize);

            for _ in 0..topic_count {
                let topic = Self::decode_string(cursor)?;
                let partition_count = cursor.get_i32().max(0).min(100_000);
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

    pub(crate) fn encode_offset_fetch_response(
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

    pub(crate) fn decode_find_coordinator_request(
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

    pub(crate) fn encode_find_coordinator_response(
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

    pub(crate) fn decode_list_groups_request(
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
            let states_filter_count = cursor.get_i32().max(0).min(100);
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

    pub(crate) fn encode_list_groups_response(
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

    pub(crate) fn decode_heartbeat_request(
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

    pub(crate) fn encode_heartbeat_response(
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

    pub(crate) fn decode_leave_group_request(
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

    pub(crate) fn encode_leave_group_response(
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

    pub(crate) fn decode_sync_group_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaSyncGroupRequest> {
        let group_id = Self::decode_string(cursor)?;

        // Defensive: check remaining bytes before reading i32
        let remaining = cursor.get_ref().len().saturating_sub(cursor.position() as usize);
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
        let remaining = cursor.get_ref().len().saturating_sub(cursor.position() as usize);
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

        let assignment_count = cursor.get_i32().max(0).min(1_000);
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

    pub(crate) fn encode_sync_group_response(
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

    pub(crate) fn decode_join_group_request(
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

        let protocol_count = cursor.get_i32().max(0).min(100);
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

    pub(crate) fn encode_join_group_response(
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

    pub(crate) fn decode_describe_groups_request(
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
            if cursor.remaining() < 4 {
                return Err(KafkaCodecError::BufferUnderrun {
                    needed: 4,
                    available: cursor.remaining(),
                });
            }
            let group_count = cursor.get_i32();
            if group_count < 0 {
                return Err(KafkaCodecError::InvalidFormat(
                    "Negative group count in DescribeGroups request".to_string(),
                ));
            }
            let mut groups = Vec::with_capacity((group_count as usize).min(1000));
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

    pub(crate) fn encode_describe_groups_response(
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
}
