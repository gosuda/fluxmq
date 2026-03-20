//! Kafka Metadata (ApiKey 3), ListOffsets (ApiKey 2), and ApiVersions (ApiKey 18) codec implementation.

use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;
#[allow(unused_imports)]
use tracing::{debug, warn, error};

use super::super::messages::*;
use super::Result;

impl super::KafkaCodec {
    // ========================================================================
    // LIST OFFSETS REQUEST/RESPONSE
    // ========================================================================

    pub(crate) fn decode_list_offsets_request(
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

        let topic_count = topic_count.max(0).min(10_000);
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
            let partition_count = partition_count.max(0).min(100_000);

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

    pub(crate) fn decode_metadata_request(
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
            let topic_count = topic_count.max(0).min(50_000);
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

    pub(crate) fn encode_metadata_response(
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

    pub(crate) fn encode_list_offsets_response(
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
    // API VERSIONS REQUEST/RESPONSE
    // ========================================================================

    pub(crate) fn decode_api_versions_request(
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

    pub(crate) fn encode_api_versions_response(
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
}
