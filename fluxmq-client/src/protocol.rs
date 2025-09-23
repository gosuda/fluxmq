//! Protocol types and utilities for FluxMQ client communication
//! Uses Kafka wire protocol for compatibility

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use std::time::SystemTime;

use crate::error::FluxmqClientError;

pub type TopicName = String;
pub type PartitionId = u32;
pub type Offset = u64;
pub type CorrelationId = i32;

/// High-level record for producing messages
#[derive(Debug, Clone)]
pub struct ProduceRecord {
    pub topic: TopicName,
    pub partition: Option<PartitionId>, // None for auto-assignment
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: HashMap<String, Bytes>,
    pub timestamp: Option<u64>,
}

impl ProduceRecord {
    /// Create a new record builder
    pub fn builder() -> ProduceRecordBuilder {
        ProduceRecordBuilder::new()
    }

    /// Create a simple record with topic and value
    pub fn new<T: Into<TopicName>, V: Into<Bytes>>(topic: T, value: V) -> Self {
        Self {
            topic: topic.into(),
            partition: None,
            key: None,
            value: value.into(),
            headers: HashMap::new(),
            timestamp: None,
        }
    }

    /// Create a record with topic, key, and value
    pub fn with_key<T: Into<TopicName>, K: Into<Bytes>, V: Into<Bytes>>(
        topic: T,
        key: K,
        value: V,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition: None,
            key: Some(key.into()),
            value: value.into(),
            headers: HashMap::new(),
            timestamp: None,
        }
    }
}

/// Builder for ProduceRecord
#[derive(Debug, Default)]
pub struct ProduceRecordBuilder {
    topic: Option<TopicName>,
    partition: Option<PartitionId>,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: HashMap<String, Bytes>,
    timestamp: Option<u64>,
}

impl ProduceRecordBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn topic<T: Into<TopicName>>(mut self, topic: T) -> Self {
        self.topic = Some(topic.into());
        self
    }

    pub fn partition(mut self, partition: PartitionId) -> Self {
        self.partition = Some(partition);
        self
    }

    pub fn key<K: Into<Bytes>>(mut self, key: K) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn value<V: Into<Bytes>>(mut self, value: V) -> Self {
        self.value = Some(value.into());
        self
    }

    pub fn header<K: Into<String>, V: Into<Bytes>>(mut self, key: K, value: V) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    pub fn timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn build(self) -> ProduceRecord {
        let topic = self.topic.expect("Topic is required");
        let value = self.value.expect("Value is required");

        ProduceRecord {
            topic,
            partition: self.partition,
            key: self.key,
            value,
            headers: self.headers,
            timestamp: self.timestamp,
        }
    }
}

/// High-level record for consuming messages
#[derive(Debug, Clone)]
pub struct ConsumeRecord {
    pub topic: TopicName,
    pub partition: PartitionId,
    pub offset: Offset,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: HashMap<String, Bytes>,
    pub timestamp: u64,
}

/// Metadata for a produce operation
#[derive(Debug, Clone)]
pub struct ProduceMetadata {
    pub topic: TopicName,
    pub partition: PartitionId,
    pub offset: Offset,
}

/// Topic partition identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: TopicName,
    pub partition: PartitionId,
}

impl TopicPartition {
    pub fn new(topic: TopicName, partition: PartitionId) -> Self {
        Self { topic, partition }
    }
}

/// Message structure compatible with Kafka wire protocol
#[derive(Debug, Clone)]
pub struct Message {
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub timestamp: u64,
    pub headers: HashMap<String, Bytes>,
}

impl Message {
    pub fn new<V: Into<Bytes>>(value: V) -> Self {
        Self {
            key: None,
            value: value.into(),
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            headers: HashMap::new(),
        }
    }

    pub fn with_key<K: Into<Bytes>>(mut self, key: K) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn with_header<K: Into<String>, V: Into<Bytes>>(mut self, key: K, value: V) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}

/// Kafka wire protocol utilities for FluxMQ client
/// This module provides basic Kafka protocol compatibility
pub mod kafka {
    use super::*;
    use bytes::{Buf, BufMut};
    use std::io::Cursor;

    /// Kafka API keys
    pub const PRODUCE_API_KEY: i16 = 0;
    pub const FETCH_API_KEY: i16 = 1;
    pub const METADATA_API_KEY: i16 = 3;

    /// Write a length-prefixed string
    pub fn put_string(buf: &mut BytesMut, s: &str) {
        if s.is_empty() {
            buf.put_i16(-1);
        } else {
            buf.put_i16(s.len() as i16);
            buf.put_slice(s.as_bytes());
        }
    }

    /// Read a length-prefixed string
    pub fn get_string(buf: &mut Cursor<&[u8]>) -> Result<Option<String>, FluxmqClientError> {
        if buf.remaining() < 2 {
            return Err(FluxmqClientError::protocol(
                "Insufficient bytes for string length",
            ));
        }

        let len = buf.get_i16();
        if len == -1 {
            return Ok(None);
        }

        if len < 0 || buf.remaining() < len as usize {
            return Err(FluxmqClientError::protocol("Invalid string length"));
        }

        let mut bytes = vec![0; len as usize];
        buf.copy_to_slice(&mut bytes);
        let s = String::from_utf8(bytes)
            .map_err(|_| FluxmqClientError::protocol("Invalid UTF-8 string"))?;
        Ok(Some(s))
    }

    /// Write bytes array with length prefix
    pub fn put_bytes(buf: &mut BytesMut, bytes: &[u8]) {
        buf.put_i32(bytes.len() as i32);
        buf.put_slice(bytes);
    }

    /// Read bytes array with length prefix
    pub fn get_bytes(buf: &mut Cursor<&[u8]>) -> Result<Option<Bytes>, FluxmqClientError> {
        if buf.remaining() < 4 {
            return Err(FluxmqClientError::protocol(
                "Insufficient bytes for bytes length",
            ));
        }

        let len = buf.get_i32();
        if len == -1 {
            return Ok(None);
        }

        if len < 0 || buf.remaining() < len as usize {
            return Err(FluxmqClientError::protocol("Invalid bytes length"));
        }

        let mut bytes = vec![0; len as usize];
        buf.copy_to_slice(&mut bytes);
        Ok(Some(Bytes::from(bytes)))
    }
}

/// Basic Kafka request structure for client communication
#[derive(Debug, Clone)]
pub struct KafkaRequest {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub payload: Bytes,
}

/// Basic Kafka response structure for client communication
#[derive(Debug, Clone)]
pub struct KafkaResponse {
    pub correlation_id: i32,
    pub payload: Bytes,
}

impl KafkaRequest {
    pub fn new(api_key: i16, correlation_id: i32, payload: Bytes) -> Self {
        Self {
            api_key,
            api_version: 0, // Default version
            correlation_id,
            client_id: Some("fluxmq-rust-client".to_string()),
            payload,
        }
    }

    /// Encode request to bytes using Kafka wire protocol
    pub fn encode(&self) -> Result<Bytes, FluxmqClientError> {
        let mut buf = BytesMut::new();

        // Message length will be filled later
        buf.put_i32(0); // Placeholder for length

        // Request header
        buf.put_i16(self.api_key);
        buf.put_i16(self.api_version);
        buf.put_i32(self.correlation_id);

        // Client ID
        if let Some(ref client_id) = self.client_id {
            kafka::put_string(&mut buf, client_id);
        } else {
            buf.put_i16(-1);
        }

        // Payload
        buf.put_slice(&self.payload);

        // Update message length (excluding the length field itself)
        let total_len = buf.len() - 4;
        (&mut buf[0..4]).put_i32(total_len as i32);

        Ok(buf.freeze())
    }

    /// Decode response from bytes
    pub fn decode_response(mut data: Bytes) -> Result<KafkaResponse, FluxmqClientError> {
        if data.len() < 4 {
            return Err(FluxmqClientError::protocol("Response too short"));
        }

        let correlation_id = data.get_i32();
        let payload = data;

        Ok(KafkaResponse {
            correlation_id,
            payload,
        })
    }
}

/// Compatibility layer for existing code - using Kafka protocol underneath
/// These types provide the same API as before but use Kafka wire protocol

/// Produce request for sending messages
#[derive(Debug, Clone)]
pub struct ProduceRequest {
    pub correlation_id: i32,
    pub topic: String,
    pub partition: u32,
    pub messages: Vec<Message>,
    pub acks: i16,
    pub timeout_ms: u32,
}

/// Produce response
#[derive(Debug, Clone)]
pub struct ProduceResponse {
    pub correlation_id: i32,
    pub topic: String,
    pub partition: u32,
    pub base_offset: u64,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// Fetch request for consuming messages
#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub correlation_id: i32,
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub max_bytes: u32,
    pub timeout_ms: u32,
}

/// Fetch response
#[derive(Debug, Clone)]
pub struct FetchResponse {
    pub correlation_id: i32,
    pub topic: String,
    pub partition: u32,
    pub messages: Vec<(u64, Message)>, // (offset, message) pairs
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// Metadata request
#[derive(Debug, Clone)]
pub struct MetadataRequest {
    pub correlation_id: i32,
    pub topics: Vec<String>,
}

/// Metadata response structures
#[derive(Debug, Clone)]
pub struct BrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub id: u32,
    pub leader: Option<i32>,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub leader_epoch: i32,
}

#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub name: String,
    pub error_code: i16,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone)]
pub struct MetadataResponse {
    pub correlation_id: i32,
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>,
    pub api_version: i16,
}

/// Request enum for API compatibility
#[derive(Debug, Clone)]
pub enum Request {
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    Metadata(MetadataRequest),
}

/// Response enum for API compatibility  
#[derive(Debug, Clone)]
pub enum Response {
    Produce(ProduceResponse),
    Fetch(FetchResponse),
    Metadata(MetadataResponse),
}

/// Convert Message to match expected From<&ProduceRecord> trait
impl From<&ProduceRecord> for Message {
    fn from(record: &ProduceRecord) -> Self {
        Message {
            key: record.key.clone(),
            value: record.value.clone(),
            timestamp: record.timestamp.unwrap_or_else(|| {
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
            }),
            headers: record.headers.clone(),
        }
    }
}

/// Convert (offset, message) tuple to ConsumeRecord
impl From<(u64, Message)> for ConsumeRecord {
    fn from((offset, message): (u64, Message)) -> Self {
        ConsumeRecord {
            topic: String::new(), // Will be filled by caller
            partition: 0,         // Will be filled by caller
            offset,
            key: message.key,
            value: message.value,
            headers: message.headers,
            timestamp: message.timestamp,
        }
    }
}

/// Client codec for compatibility - uses Kafka wire protocol underneath
pub struct ClientCodec {
    correlation_counter: std::sync::atomic::AtomicI32,
}

impl ClientCodec {
    pub fn new() -> Self {
        Self {
            correlation_counter: std::sync::atomic::AtomicI32::new(1),
        }
    }

    pub fn next_correlation_id(&self) -> i32 {
        self.correlation_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for ClientCodec {
    fn default() -> Self {
        Self::new()
    }
}

// Implement the tokio codec traits to make it compatible with Framed
impl tokio_util::codec::Encoder<Request> for ClientCodec {
    type Error = FluxmqClientError;

    fn encode(&mut self, item: Request, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let correlation_id = self.next_correlation_id();

        // Create Kafka request based on the request type
        let kafka_request = match item {
            Request::Produce(produce_req) => {
                // Encode produce request as Kafka PRODUCE API call
                let mut payload = bytes::BytesMut::new();

                // Simplified produce payload encoding
                kafka::put_string(&mut payload, &produce_req.topic);
                payload.put_u32(produce_req.partition);
                payload.put_i16(produce_req.acks);
                payload.put_u32(produce_req.timeout_ms);
                payload.put_u32(produce_req.messages.len() as u32);

                for message in &produce_req.messages {
                    if let Some(ref key) = message.key {
                        kafka::put_bytes(&mut payload, key);
                    } else {
                        payload.put_i32(-1);
                    }
                    kafka::put_bytes(&mut payload, &message.value);
                    payload.put_u64(message.timestamp);
                    payload.put_u32(message.headers.len() as u32);
                    for (k, v) in &message.headers {
                        kafka::put_string(&mut payload, k);
                        kafka::put_bytes(&mut payload, v);
                    }
                }

                KafkaRequest::new(kafka::PRODUCE_API_KEY, correlation_id, payload.freeze())
            }
            Request::Fetch(fetch_req) => {
                let mut payload = bytes::BytesMut::new();

                kafka::put_string(&mut payload, &fetch_req.topic);
                payload.put_u32(fetch_req.partition);
                payload.put_u64(fetch_req.offset);
                payload.put_u32(fetch_req.max_bytes);
                payload.put_u32(fetch_req.timeout_ms);

                KafkaRequest::new(kafka::FETCH_API_KEY, correlation_id, payload.freeze())
            }
            Request::Metadata(metadata_req) => {
                let mut payload = bytes::BytesMut::new();

                payload.put_u32(metadata_req.topics.len() as u32);
                for topic in &metadata_req.topics {
                    kafka::put_string(&mut payload, topic);
                }

                KafkaRequest::new(kafka::METADATA_API_KEY, correlation_id, payload.freeze())
            }
        };

        // Encode the Kafka request
        let encoded = kafka_request.encode()?;
        dst.extend_from_slice(&encoded);

        Ok(())
    }
}

impl tokio_util::codec::Decoder for ClientCodec {
    type Item = Response;
    type Error = FluxmqClientError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None); // Need at least length prefix
        }

        let message_len = (&src[0..4]).get_i32() as usize;
        if src.len() < 4 + message_len {
            return Ok(None); // Need complete message
        }

        let frame = src.split_to(4 + message_len);
        let kafka_response = KafkaRequest::decode_response(frame.freeze())?;

        // For now, return a simple produce response - in a full implementation
        // this would parse the response based on the original request type
        let response = Response::Produce(ProduceResponse {
            correlation_id: kafka_response.correlation_id,
            topic: "unknown".to_string(), // Would need request tracking
            partition: 0,
            base_offset: 0,
            error_code: 0,
            error_message: None,
        });

        Ok(Some(response))
    }
}
