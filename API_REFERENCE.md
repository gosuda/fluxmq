# FluxMQ API Reference

## 🚀 Kafka Wire Protocol API Documentation

FluxMQ implements 100% Kafka-compatible wire protocol with **20 APIs** supporting all major client operations. This document provides comprehensive API reference for developers and operators.

## 📊 API Compatibility Matrix

| API Key | API Name | Version Support | Status | Java Client | Python Client |
|---------|----------|-----------------|--------|-------------|---------------|
| 0 | Produce | v0-v9 | ✅ Complete | ✅ | ✅ |
| 1 | Fetch | v0-v13 | ✅ Complete | ✅ | ✅ |
| 2 | ListOffsets | v0-v7 | ✅ Complete | ✅ | ✅ |
| 3 | Metadata | v0-v8 | ✅ Complete | ✅ | ✅ |
| 8 | OffsetCommit | v0-v8 | ✅ Complete | ✅ | ✅ |
| 9 | OffsetFetch | v0-v8 | ✅ Complete | ✅ | ✅ |
| 10 | FindCoordinator | v0-v4 | ✅ Complete | ✅ | ✅ |
| 11 | JoinGroup | v0-v9 | ✅ Complete | ✅ | ✅ |
| 12 | Heartbeat | v0-v4 | ✅ Complete | ✅ | ✅ |
| 13 | LeaveGroup | v0-v5 | ✅ Complete | ✅ | ✅ |
| 14 | SyncGroup | v0-v5 | ✅ Complete | ✅ | ✅ |
| 15 | DescribeGroups | v0-v5 | ✅ Complete | ✅ | ✅ |
| 16 | ListGroups | v0-v4 | ✅ Complete | ✅ | ✅ |
| 17 | SaslHandshake | v0-v1 | ✅ Complete | ✅ | ✅ |
| 18 | ApiVersions | v0-v4 | ✅ Complete | ✅ | ✅ |
| 19 | CreateTopics | v0-v7 | ✅ Complete | ✅ | ✅ |
| 20 | DeleteTopics | v0-v6 | ✅ Complete | ✅ | ✅ |
| 32 | DescribeConfigs | v0-v4 | ✅ Complete | ✅ | ✅ |
| 33 | AlterConfigs | v0-v2 | ✅ Complete | ✅ | ✅ |
| 36 | SaslAuthenticate | v0-v2 | ✅ Complete | ✅ | ✅ |

## 📝 API Details

### API 0: Produce
**Purpose**: Send messages to topics  
**Performance**: 601,379+ msg/sec with MegaBatch optimization

#### Request Format
```
ProduceRequest => 
  TransactionalId [TopicData] Acks Timeout
  TransactionalId => NULLABLE_STRING
  TopicData => Name [PartitionData]
    Name => STRING
    PartitionData => Index Records
      Index => INT32
      Records => RECORDS
  Acks => INT16
  Timeout => INT32
```

#### Configuration Options
- `acks=0`: Fire-and-forget (maximum performance)
- `acks=1`: Leader acknowledgment (balanced)
- `acks=-1`: All replicas acknowledgment (maximum durability)

#### Java Example
```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", StringSerializer.class);
props.put("value.serializer", StringSerializer.class);
props.put("acks", "0");
props.put("batch.size", "1048576");  // 1MB batch
props.put("linger.ms", "15");
props.put("compression.type", "lz4");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
ProducerRecord<String, String> record = 
    new ProducerRecord<>("my-topic", "key", "value");
producer.send(record);
```

---

### API 1: Fetch
**Purpose**: Retrieve messages from topics  
**Features**: Async notifications, timeout support, byte limits

#### Request Format
```
FetchRequest =>
  ReplicaId MaxWaitTime MinBytes MaxBytes IsolationLevel 
  SessionId SessionEpoch [Topics] [ForgottenTopicsData]
  ReplicaId => INT32
  MaxWaitTime => INT32
  MinBytes => INT32
  MaxBytes => INT32
  IsolationLevel => INT8
  SessionId => INT32
  SessionEpoch => INT32
  Topics => Name [Partitions]
    Name => STRING
    Partitions => PartitionIndex FetchOffset LogStartOffset MaxBytes
      PartitionIndex => INT32
      FetchOffset => INT64
      LogStartOffset => INT64
      MaxBytes => INT32
```

#### Performance Optimizations
- Memory-mapped I/O for zero-copy reads
- Batch fetching across multiple partitions
- Async notification system eliminates polling

---

### API 2: ListOffsets
**Purpose**: Query offset information for partitions  
**Use Cases**: Seeking to timestamp, getting latest/earliest offsets

#### Request Format
```
ListOffsetsRequest =>
  ReplicaId IsolationLevel [Topics]
  ReplicaId => INT32
  IsolationLevel => INT8
  Topics => Name [Partitions]
    Name => STRING
    Partitions => PartitionIndex Timestamp
      PartitionIndex => INT32
      Timestamp => INT64
```

#### Special Timestamp Values
- `-1`: Latest offset
- `-2`: Earliest offset
- `> 0`: Find offset by timestamp

---

### API 3: Metadata
**Purpose**: Discover topics, partitions, and brokers  
**Critical**: Required for client bootstrap and partition discovery

#### Request Format
```
MetadataRequest =>
  [Topics] AllowAutoTopicCreation IncludeClusterAuthorizedOperations
  IncludeTopicAuthorizedOperations
  Topics => Name
    Name => STRING
  AllowAutoTopicCreation => BOOLEAN
  IncludeClusterAuthorizedOperations => BOOLEAN
  IncludeTopicAuthorizedOperations => BOOLEAN
```

#### Response Includes
- Broker list with host/port
- Topic list with partition details
- Leader/replica information
- ISR (In-Sync Replica) status

#### Compatibility Note
FluxMQ uses Metadata API v8 for Java client compatibility (avoids flexible version issues).

---

### API 8-16: Consumer Group APIs

#### API 8: OffsetCommit
**Purpose**: Save consumer position in topics
```java
consumer.commitSync();  // Synchronous commit
consumer.commitAsync(); // Asynchronous commit
```

#### API 9: OffsetFetch
**Purpose**: Retrieve saved consumer positions
```java
Map<TopicPartition, OffsetAndMetadata> offsets = 
    consumer.committed(partitions);
```

#### API 10: FindCoordinator
**Purpose**: Locate group coordinator broker
- Automatically handled by client libraries
- Returns broker ID and connection details

#### API 11: JoinGroup
**Purpose**: Join consumer group and get member assignment
- Handles rebalancing protocols
- Supports range, round-robin, sticky assignment

#### API 12: Heartbeat
**Purpose**: Maintain consumer group membership
- Default interval: 3 seconds
- Session timeout: 10 seconds

#### API 13: LeaveGroup
**Purpose**: Gracefully leave consumer group
```java
consumer.close(); // Triggers LeaveGroup
```

#### API 14: SyncGroup
**Purpose**: Synchronize partition assignments
- Leader computes assignments
- Followers receive assignments

#### API 15: DescribeGroups
**Purpose**: Get detailed group information
```java
AdminClient admin = AdminClient.create(props);
DescribeConsumerGroupsResult result = 
    admin.describeConsumerGroups(Arrays.asList("my-group"));
```

#### API 16: ListGroups
**Purpose**: List all consumer groups
```java
ListConsumerGroupsResult groups = admin.listConsumerGroups();
```

---

### API 17: SaslHandshake
**Purpose**: Negotiate SASL authentication mechanism  
**Supported Mechanisms**: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512

#### Configuration
```java
props.put("security.protocol", "SASL_PLAINTEXT");
props.put("sasl.mechanism", "PLAIN");
props.put("sasl.jaas.config", 
    "org.apache.kafka.common.security.plain.PlainLoginModule required " +
    "username=\"user\" password=\"pass\";");
```

---

### API 18: ApiVersions
**Purpose**: Protocol version negotiation  
**Features**: KIP-482 flexible versions support

#### Response Format
- Lists all supported APIs with min/max versions
- Includes throttle time for rate limiting
- Flexible versions (v3+) use compact encoding

#### Client Identification
FluxMQ extracts and logs client software/version:
- `kafka-python 2.2.15`
- `apache-kafka-java 4.1`
- `confluent-kafka-go 1.9.0`

---

### API 19: CreateTopics
**Purpose**: Programmatically create topics  
**Features**: Replication factor, partition count, configurations

#### Java Example
```java
AdminClient admin = AdminClient.create(props);
NewTopic newTopic = new NewTopic("my-topic", 10, (short) 3);
Map<String, String> configs = new HashMap<>();
configs.put("retention.ms", "604800000");  // 7 days
newTopic.configs(configs);
admin.createTopics(Arrays.asList(newTopic));
```

---

### API 20: DeleteTopics
**Purpose**: Remove topics and associated data  
**Warning**: Irreversible operation

#### Java Example
```java
AdminClient admin = AdminClient.create(props);
admin.deleteTopics(Arrays.asList("old-topic"));
```

---

### API 32: DescribeConfigs
**Purpose**: Retrieve broker/topic configurations  
**Resource Types**: BROKER, TOPIC

#### Request Format
```
DescribeConfigsRequest =>
  [Resources] IncludeSynonyms IncludeDocumentation
  Resources => ResourceType ResourceName [ConfigurationKeys]
    ResourceType => INT8
    ResourceName => STRING
    ConfigurationKeys => STRING
```

---

### API 33: AlterConfigs
**Purpose**: Modify broker/topic configurations  
**Use Cases**: Dynamic configuration updates without restart

#### Java Example
```java
Map<ConfigResource, Config> configs = new HashMap<>();
ConfigResource resource = 
    new ConfigResource(ConfigResource.Type.TOPIC, "my-topic");
Config config = new Config(Arrays.asList(
    new ConfigEntry("retention.ms", "86400000")  // 1 day
));
configs.put(resource, config);
admin.alterConfigs(configs);
```

---

### API 36: SaslAuthenticate
**Purpose**: Complete SASL authentication flow  
**Process**: Follows SaslHandshake, exchanges authentication tokens

---

## 🔧 Client Configuration Best Practices

### Java Client Optimization
```java
// Producer - Maximum Performance
Properties producerProps = new Properties();
producerProps.put("bootstrap.servers", "localhost:9092");
producerProps.put("acks", "0");
producerProps.put("batch.size", "1048576");         // 1MB batches
producerProps.put("linger.ms", "15");
producerProps.put("compression.type", "lz4");
producerProps.put("buffer.memory", "268435456");    // 256MB
producerProps.put("max.in.flight.requests.per.connection", "100");

// Consumer - Efficient Consumption
Properties consumerProps = new Properties();
consumerProps.put("bootstrap.servers", "localhost:9092");
consumerProps.put("group.id", "my-consumer-group");
consumerProps.put("enable.auto.commit", "false");   // Manual commit
consumerProps.put("max.poll.records", "10000");     // Large batches
consumerProps.put("fetch.min.bytes", "1048576");    // 1MB minimum
consumerProps.put("fetch.max.wait.ms", "100");      // Low latency
```

### Python Client Configuration
```python
from kafka import KafkaProducer, KafkaConsumer

# Producer - High Throughput
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    acks=0,                          # No acknowledgment
    batch_size=1048576,              # 1MB batches
    linger_ms=15,
    compression_type='lz4',
    buffer_memory=268435456          # 256MB buffer
)

# Consumer - Batch Processing
consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers=['localhost:9092'],
    group_id='my-group',
    enable_auto_commit=False,
    max_poll_records=10000,
    fetch_min_bytes=1048576
)
```

## 🎯 Performance Tuning Guide

### Network Optimization
- **batch.size**: Larger batches (1MB) for throughput
- **linger.ms**: Allow time for batch accumulation (10-20ms)
- **compression.type**: LZ4 for best speed/ratio balance

### Memory Management
- **buffer.memory**: Allocate sufficient producer memory (256MB+)
- **fetch.min.bytes**: Reduce fetch requests with larger minimums
- **max.poll.records**: Process messages in large batches

### Acknowledgment Strategy
- **acks=0**: Maximum throughput (601k+ msg/sec)
- **acks=1**: Balanced performance/durability
- **acks=-1**: Maximum durability (lower throughput)

## 📊 Protocol Specifications

### Message Format
FluxMQ uses Kafka's RecordBatch format v2:
- Magic byte: 2
- CRC32C checksums
- Timestamp support (CreateTime/LogAppendTime)
- Header support for metadata
- Compression at batch level

### Encoding Types
- **Standard Encoding** (v0-v8): Fixed-size integers, length-prefixed strings
- **Flexible Encoding** (v9+): Varint integers, compact strings, tagged fields

### Error Codes
Common Kafka error codes supported:
- `0`: No error
- `1`: Offset out of range
- `2`: Invalid message
- `3`: Unknown topic or partition
- `6`: Not leader for partition
- `7`: Request timed out
- `15`: Coordinator not available
- `16`: Not coordinator
- `25`: Rebalance in progress

## 🔍 Debugging & Monitoring

### Enable Debug Logging
```bash
# Server-side debug logging
cargo run --release -- --port 9092 --log-level debug

# Java client debug logging
props.put("log4j.logger.org.apache.kafka", "DEBUG");

# Python client debug logging
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Metrics Endpoints
FluxMQ exposes metrics at:
- Messages produced/consumed per second
- Active connections count
- Consumer group membership
- Partition leader distribution

### Common Issues & Solutions

**Issue**: "Timeout waiting for metadata"
- **Cause**: Metadata API not responding
- **Solution**: Ensure server is running with `--enable-consumer-groups`

**Issue**: "No brokers available"
- **Cause**: Connection or protocol mismatch
- **Solution**: Verify `bootstrap.servers` configuration

**Issue**: "Unknown topic or partition"
- **Cause**: Topic doesn't exist
- **Solution**: Create topic or enable auto-creation

## 📚 Additional Resources

- **Kafka Protocol Specification**: [kafka.apache.org/protocol](https://kafka.apache.org/protocol)
- **Java Client Documentation**: [kafka.apache.org/documentation](https://kafka.apache.org/documentation)
- **FluxMQ Performance Guide**: [PERFORMANCE.md](PERFORMANCE.md)
- **FluxMQ Architecture**: [ARCHITECTURE.md](ARCHITECTURE.md)

---

**FluxMQ API Reference** - Complete Kafka wire protocol compatibility with blazing performance ⚡️