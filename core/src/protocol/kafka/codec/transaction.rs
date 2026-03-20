//! Kafka Transaction APIs (ApiKeys 22-28) codec implementation.

use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;
#[allow(unused_imports)]
use tracing::debug;

use super::super::messages::*;
use super::Result;
use crate::transaction::messages::{
    InitProducerIdRequest, AddPartitionsToTxnRequest, AddPartitionsToTxnTopic,
    AddOffsetsToTxnRequest, EndTxnRequest, WriteTxnMarkersRequest,
    WritableTxnMarker, WritableTxnMarkerTopic, TxnOffsetCommitRequest,
    TxnOffsetCommitRequestTopic, TxnOffsetCommitRequestPartition,
};

impl super::KafkaCodec {
    // ========================================================================
    // TRANSACTION API RESPONSE ENCODING
    // ========================================================================

    /// Encode InitProducerId response (API Key 22)
    /// Flexible versions: 2+ (uses compact encoding with tagged fields)
    pub(crate) fn encode_init_producer_id_response(
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
    pub(crate) fn encode_add_partitions_to_txn_response(
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
    pub(crate) fn encode_add_offsets_to_txn_response(
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
    pub(crate) fn encode_end_txn_response(
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
    pub(crate) fn encode_write_txn_markers_response(
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
    pub(crate) fn encode_txn_offset_commit_response(
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
    pub(crate) fn decode_init_producer_id_request(
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
    pub(crate) fn decode_add_partitions_to_txn_request(
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
    pub(crate) fn decode_add_offsets_to_txn_request(
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
    pub(crate) fn decode_end_txn_request(
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
    pub(crate) fn decode_write_txn_markers_request(
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
    pub(crate) fn decode_txn_offset_commit_request(
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
