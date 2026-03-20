//! Kafka Admin, Security, and Cluster Coordination APIs codec implementation.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::Cursor;
#[allow(unused_imports)]
use tracing::{debug, warn};

use super::super::messages::*;
use super::{KafkaCodecError, Result};

impl super::KafkaCodec {
    // ========================================================================
    // CREATE TOPICS REQUEST/RESPONSE (API KEY 19)
    // ========================================================================

    pub(crate) fn decode_create_topics_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaCreateTopicsRequest> {
        let topic_count = cursor.get_i32().max(0).min(1_000);
        let mut topics = Vec::with_capacity(topic_count as usize);

        for _ in 0..topic_count {
            let name = Self::decode_string(cursor)?;
            let num_partitions = cursor.get_i32();
            let replication_factor = cursor.get_i16();

            // Skip assignments (empty for simple case)
            let assignment_count = cursor.get_i32().max(0).min(10_000);
            for _ in 0..assignment_count {
                let _partition_id = cursor.get_i32();
                let replica_count = cursor.get_i32().max(0).min(1_000);
                for _ in 0..replica_count {
                    let _broker_id = cursor.get_i32();
                }
            }

            // Read configs
            let config_count = cursor.get_i32().max(0).min(1_000);
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

    pub(crate) fn encode_create_topics_response(
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

    // ========================================================================
    // DELETE TOPICS REQUEST/RESPONSE (API KEY 20)
    // ========================================================================

    /// Decode DeleteTopics request
    pub(crate) fn decode_delete_topics_request(
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

        let topic_count = topic_count.max(0).min(10_000);
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
    pub(crate) fn encode_delete_topics_response(
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

    // ========================================================================
    // SASL HANDSHAKE REQUEST/RESPONSE (API KEY 17)
    // ========================================================================

    /// Decode SASL Handshake request
    pub(crate) fn decode_sasl_handshake_request(
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
    pub(crate) fn encode_sasl_handshake_response(
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

    // ========================================================================
    // SASL AUTHENTICATE REQUEST/RESPONSE (API KEY 36)
    // ========================================================================

    /// Decode SASL Authenticate request
    pub(crate) fn decode_sasl_authenticate_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaSaslAuthenticateRequest> {
        use std::io::Read;

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
    pub(crate) fn encode_sasl_authenticate_response(
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
    // DESCRIBE CONFIGS REQUEST/RESPONSE (API KEY 32)
    // ========================================================================

    pub(crate) fn decode_describe_configs_request(
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

        let resources_length = resources_length.max(0).min(1_000);
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
                let config_keys_length = config_keys_length.max(0).min(1_000);
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

    pub(crate) fn encode_describe_configs_response(
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

    pub(crate) fn decode_alter_configs_request(
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

        let resources_length = resources_length.max(0).min(1_000);
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
            let configs_length = configs_length.max(0).min(1_000);

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

    pub(crate) fn encode_alter_configs_response(
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

    pub(crate) fn decode_incremental_alter_configs_request(
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

        let resources_length = resources_length.max(0).min(1_000);
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
            let configs_length = configs_length.max(0).min(1_000);

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

    pub(crate) fn encode_incremental_alter_configs_response(
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

    // ========================================================================
    // DELETE RECORDS API (ApiKey = 21)
    // ========================================================================

    pub(crate) fn encode_delete_records_response(
        resp: &KafkaDeleteRecordsResponse,
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

    pub(crate) fn encode_create_partitions_response(
        resp: &KafkaCreatePartitionsResponse,
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

    pub(crate) fn encode_delete_groups_response(
        resp: &KafkaDeleteGroupsResponse,
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

    pub(crate) fn encode_alter_partition_reassignments_response(
        resp: &KafkaAlterPartitionReassignmentsResponse,
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

    pub(crate) fn encode_leader_and_isr_response(
        resp: &KafkaLeaderAndIsrResponse,
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

    pub(crate) fn encode_stop_replica_response(
        resp: &KafkaStopReplicaResponse,
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

    pub(crate) fn encode_update_metadata_response(
        resp: &KafkaUpdateMetadataResponse,
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

    pub(crate) fn encode_controlled_shutdown_response(
        resp: &KafkaControlledShutdownResponse,
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

    pub(crate) fn decode_delete_records_request(
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
            let num_topics = Self::decode_array_count(cursor, "DeleteRecords topics")?;
            let mut topics = Vec::with_capacity(num_topics);
            for _ in 0..num_topics {
                let topic_name = Self::decode_string(cursor)?;
                let num_partitions = Self::decode_array_count(cursor, "DeleteRecords partitions")?;
                let mut partitions = Vec::with_capacity(num_partitions);
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

    pub(crate) fn decode_create_partitions_request(
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
            let num_topics = Self::decode_array_count(cursor, "CreatePartitions topics")?;
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
                        let num_broker_ids = Self::decode_array_count(cursor, "CreatePartitions broker_ids")?;
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

    pub(crate) fn decode_delete_groups_request(
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
            let num_groups = Self::decode_array_count(cursor, "DeleteGroups groups")?;
            let mut groups = Vec::with_capacity(num_groups.min(1000));
            for _ in 0..num_groups {
                groups.push(Self::decode_string(cursor)?);
            }
            groups
        };

        Ok(KafkaDeleteGroupsRequest { header, groups })
    }

    // ==================== AlterPartitionReassignments API (45) ====================

    pub(crate) fn decode_alter_partition_reassignments_request(
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

    pub(crate) fn decode_list_partition_reassignments_request(
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

    pub(crate) fn encode_list_partition_reassignments_response(
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

    pub(crate) fn decode_offset_delete_request(
        header: KafkaRequestHeader,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<KafkaOffsetDeleteRequest> {
        // This API is NOT flexible (flexibleVersions: none)
        let group_id = Self::decode_string(cursor)?;

        let topic_count = Self::decode_array_count(cursor, "OffsetDelete topics")?;
        let mut topics = Vec::with_capacity(topic_count.min(1000));
        for _ in 0..topic_count {
            let name = Self::decode_string(cursor)?;
            let partition_count = Self::decode_array_count(cursor, "OffsetDelete partitions")?;
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

    pub(crate) fn encode_offset_delete_response(
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

    pub(crate) fn decode_describe_cluster_request(
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

    pub(crate) fn encode_describe_cluster_response(
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

    pub(crate) fn decode_describe_producers_request(
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

    pub(crate) fn encode_describe_producers_response(
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

    pub(crate) fn decode_push_telemetry_request(
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

    pub(crate) fn encode_push_telemetry_response(
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

    pub(crate) fn decode_offset_for_leader_epoch_request(
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

    pub(crate) fn encode_offset_for_leader_epoch_response(
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

    pub(crate) fn decode_describe_acls_request(
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

    pub(crate) fn encode_describe_acls_response(
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

    pub(crate) fn decode_create_acls_request(
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

    pub(crate) fn encode_create_acls_response(
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

    pub(crate) fn decode_delete_acls_request(
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

    pub(crate) fn encode_delete_acls_response(
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
