//! Kafka Produce API (ApiKey 0) codec implementation.

use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;
#[allow(unused_imports)]
use tracing::debug;

use super::super::messages::*;
use super::Result;

impl super::KafkaCodec {
    pub(crate) fn decode_produce_request(
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

        let topic_count = cursor.get_i32().max(0).min(10_000);
        #[cfg(debug_assertions)]
        debug!("Decoding produce request: topic_count={}", topic_count);
        let mut topic_data = Vec::with_capacity(topic_count as usize);

        for i in 0..topic_count {
            let topic = Self::decode_string(cursor)?;
            #[cfg(debug_assertions)]
            debug!("Decoded topic {}: '{}'", i, topic);
            let partition_count = cursor.get_i32().max(0).min(100_000);
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

    pub(crate) fn encode_produce_response(response: &KafkaProduceResponse, buf: &mut BytesMut) -> Result<()> {
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
}
