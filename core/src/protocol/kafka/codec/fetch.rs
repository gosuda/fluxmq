//! Kafka Fetch API (ApiKey 1) codec implementation.

use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;
#[allow(unused_imports)]
use tracing::debug;

use super::super::messages::*;
use super::Result;

impl super::KafkaCodec {
    pub(crate) fn decode_fetch_request(
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

        let topic_count = cursor.get_i32().max(0).min(10_000);
        let mut topics = Vec::with_capacity(topic_count as usize);

        for _ in 0..topic_count {
            let topic = Self::decode_string(cursor)?;
            let partition_count = cursor.get_i32().max(0).min(100_000);
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
        let forgotten_topic_count = cursor.get_i32().max(0).min(10_000);
        let mut forgotten_topics_data = Vec::with_capacity(forgotten_topic_count as usize);

        for _ in 0..forgotten_topic_count {
            let topic = Self::decode_string(cursor)?;
            let partition_count = cursor.get_i32().max(0).min(100_000);
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

    pub(crate) fn encode_fetch_response(response: &KafkaFetchResponse, buf: &mut BytesMut) -> Result<()> {
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
}
