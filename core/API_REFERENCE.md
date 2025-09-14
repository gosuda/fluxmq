# FluxMQ API Reference Guide

> **Complete API Documentation for FluxMQ**
> 
> This comprehensive reference covers all FluxMQ APIs, protocols, and integration patterns for developers building high-performance messaging applications.

## Table of Contents

- [Quick Start](#quick-start)
- [Core APIs](#core-apis)
- [Kafka Protocol Compatibility](#kafka-protocol-compatibility)
- [Client Integration](#client-integration)
- [Performance APIs](#performance-apis)
- [Administrative APIs](#administrative-apis)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)

## Quick Start

### Basic Server Setup

```rust
use fluxmq::{BrokerServer, BrokerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BrokerConfig {
        port: 9092,
        host: "0.0.0.0".to_string(),
        enable_consumer_groups: true,
        data_dir: Some("./fluxmq-data".into()),
        ..Default::default()
    };
    
    let server = BrokerServer::new(config).await?;
    println!("FluxMQ server starting on port 9092");
    server.run().await?;
    Ok(())
}
```

### Client Connection

```rust
// Using with any Kafka client library
// Java: org.apache.kafka:kafka-clients
// Python: kafka-python
// Node.js: kafkajs
// Go: sarama, confluent-kafka-go

// Example connection string
bootstrap_servers = "localhost:9092"
```

## Core APIs

### BrokerServer

The main server component that handles client connections and message routing.

#### Constructor

```rust
pub async fn new(config: BrokerConfig) -> Result<Self>
```

**Parameters:**
- `config: BrokerConfig` - Server configuration parameters

**Returns:** `Result<BrokerServer>` - Configured server instance

**Example:**
```rust
let config = BrokerConfig {
    port: 9092,
    host: "127.0.0.1".to_string(),
    enable_consumer_groups: true,
    enable_tls: false,
    max_connections: 10000,
    buffer_size: 64 * 1024,
    data_dir: Some("./data".into()),
    ..Default::default()
};

let server = BrokerServer::new(config).await?;
```

#### Methods

##### `run()`
```rust
pub async fn run(self) -> Result<()>
```
Starts the server and runs until shutdown.

##### `metrics()`
```rust
pub fn metrics(&self) -> Arc<MetricsRegistry>
```
Returns reference to server metrics for monitoring.

### BrokerConfig

Complete server configuration structure.

#### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `port` | `u16` | `9092` | TCP port to listen on |
| `host` | `String` | `"0.0.0.0"` | IP address to bind to |
| `enable_consumer_groups` | `bool` | `false` | Enable consumer group coordination |
| `enable_tls` | `bool` | `false` | Enable TLS/SSL encryption |
| `enable_acl` | `bool` | `false` | Enable access control lists |
| `data_dir` | `Option<PathBuf>` | `None` | Data storage directory |
| `max_connections` | `usize` | `1000` | Maximum concurrent connections |
| `buffer_size` | `usize` | `8192` | Per-connection buffer size |
| `segment_size` | `u64` | `1GB` | Storage segment size |

#### Example Configurations

**Development Setup:**
```rust
BrokerConfig {
    port: 9092,
    host: "localhost".to_string(),
    enable_consumer_groups: true,
    data_dir: Some("./dev-data".into()),
    ..Default::default()
}
```

**Production Setup:**
```rust
BrokerConfig {
    port: 9092,
    host: "0.0.0.0".to_string(),
    enable_consumer_groups: true,
    enable_tls: true,
    enable_acl: true,
    tls_cert: Some("server.crt".into()),
    tls_key: Some("server.key".into()),
    acl_config: Some("acl.json".into()),
    data_dir: Some("/var/lib/fluxmq".into()),
    max_connections: 10000,
    buffer_size: 64 * 1024,
    segment_size: 2 * 1024 * 1024 * 1024, // 2GB
}
```

**High-Performance Setup:**
```rust
BrokerConfig {
    port: 9092,
    host: "0.0.0.0".to_string(),
    enable_consumer_groups: true,
    data_dir: Some("/mnt/fast-ssd/fluxmq".into()),
    max_connections: 50000,
    buffer_size: 128 * 1024, // 128KB buffers
    segment_size: 4 * 1024 * 1024 * 1024, // 4GB segments
    ..Default::default()
}
```

### Storage APIs

#### HybridStorage

High-performance storage system with memory and disk tiers.

```rust
use fluxmq::storage::HybridStorage;

// Create storage instance
let storage = HybridStorage::new("./data".into()).await?;

// Append messages
let message = Message {
    key: Some(b"user123".to_vec()),
    value: b"user login event".to_vec(),
    timestamp: SystemTime::now(),
    ..Default::default()
};

let offset = storage.append("user-events", 0, vec![message]).await?;
println!("Message stored at offset: {}", offset);

// Fetch messages
let messages = storage.fetch("user-events", 0, offset, 100).await?;
for (msg_offset, message) in messages {
    println!("Message at {}: {:?}", msg_offset, message.value);
}
```

#### InMemoryStorage

Fast in-memory storage for development and testing.

```rust
use fluxmq::storage::InMemoryStorage;

let storage = InMemoryStorage::new();
let messages = vec![
    Message { value: b"hello".to_vec(), ..Default::default() },
    Message { value: b"world".to_vec(), ..Default::default() },
];

let offset = storage.append("test-topic", 0, messages).await?;
```

### Consumer Group APIs

#### ConsumerGroupCoordinator

Manages consumer groups with partition assignment and rebalancing.

```rust
use fluxmq::consumer::{ConsumerGroupCoordinator, ConsumerGroupConfig};

let config = ConsumerGroupConfig {
    default_session_timeout_ms: 30000,
    default_assignment_strategy: AssignmentStrategy::RoundRobin,
    ..Default::default()
};

let coordinator = ConsumerGroupCoordinator::new(config);
```

#### ConsumerGroupMessage

Protocol messages for consumer group operations.

**Join Group:**
```rust
use fluxmq::consumer::ConsumerGroupMessage;

let join_request = ConsumerGroupMessage::JoinGroup {
    group_id: "my-consumer-group".to_string(),
    consumer_id: "consumer-001".to_string(),
    client_id: "my-app-v1.0".to_string(),
    client_host: "10.0.0.100".to_string(),
    session_timeout_ms: 30000,
    rebalance_timeout_ms: 300000,
    protocol_type: "consumer".to_string(),
    group_protocols: vec![],
};
```

**Offset Management:**
```rust
let offset_commit = ConsumerGroupMessage::OffsetCommit {
    group_id: "my-group".to_string(),
    consumer_id: "consumer-001".to_string(),
    generation_id: 1,
    retention_time_ms: -1,
    offsets: vec![
        TopicPartitionOffset {
            topic: "events".to_string(),
            partition: 0,
            offset: 12345,
            metadata: Some("processed batch".to_string()),
        },
    ],
};
```

### Metrics APIs

#### MetricsRegistry

Comprehensive performance monitoring system.

```rust
use fluxmq::metrics::MetricsRegistry;
use std::sync::Arc;

let metrics = Arc::new(MetricsRegistry::new());

// Start background collection
let metrics_clone = Arc::clone(&metrics);
tokio::spawn(async move {
    metrics_clone.start_background_tasks().await;
});

// Record metrics in message processing
metrics.throughput.record_message_produced(batch_size);
metrics.throughput.record_message_consumed(1);

// Get performance snapshot
let snapshot = metrics.get_snapshot().await;
println!("Throughput: {} msg/sec", snapshot.messages_per_second);
println!("Connections: {}", snapshot.active_connections);
```

#### HTTP Metrics Endpoint

```rust
use fluxmq::HttpMetricsServer;

let metrics_server = HttpMetricsServer::new(8080, metrics).await?;

tokio::spawn(async move {
    metrics_server.run().await
});

// Metrics available at: http://localhost:8080/metrics
```

## Kafka Protocol Compatibility

FluxMQ implements 20 Kafka APIs with full wire protocol compatibility:

### Core APIs (0-3)

| API | Name | Version | Status | Description |
|-----|------|---------|---------|-------------|
| 0 | Produce | v0-v9 | ✅ Complete | Message publishing with batching |
| 1 | Fetch | v0-v13 | ✅ Complete | Message consumption with async notifications |
| 2 | ListOffsets | v0-v7 | ✅ Complete | Offset querying with timestamp support |
| 3 | Metadata | v0-v12 | ✅ Complete | Topic/partition discovery |

### Consumer Group APIs (8-16)

| API | Name | Version | Status | Description |
|-----|------|---------|---------|-------------|
| 8 | OffsetCommit | v0-v8 | ✅ Complete | Save consumer position |
| 9 | OffsetFetch | v0-v8 | ✅ Complete | Retrieve saved positions |
| 10 | FindCoordinator | v0-v4 | ✅ Complete | Locate group coordinator |
| 11 | JoinGroup | v0-v9 | ✅ Complete | Join consumer group |
| 12 | Heartbeat | v0-v4 | ✅ Complete | Maintain group membership |
| 13 | LeaveGroup | v0-v5 | ✅ Complete | Leave consumer group |
| 14 | SyncGroup | v0-v5 | ✅ Complete | Synchronize assignments |
| 15 | DescribeGroups | v0-v5 | ✅ Complete | Get group information |
| 16 | ListGroups | v0-v4 | ✅ Complete | List all groups |

### Security & Admin APIs

| API | Name | Version | Status | Description |
|-----|------|---------|---------|-------------|
| 17 | SaslHandshake | v0-v1 | ✅ Complete | SASL mechanism negotiation |
| 18 | ApiVersions | v0-v4 | ✅ Complete | Protocol version negotiation |
| 19 | CreateTopics | v0-v7 | ✅ Complete | Programmatic topic creation |
| 20 | DeleteTopics | v0-v6 | ✅ Complete | Topic deletion |
| 32 | DescribeConfigs | v0-v4 | ✅ Complete | Configuration inspection |
| 33 | AlterConfigs | v0-v2 | ✅ Complete | Configuration updates |
| 36 | SaslAuthenticate | v0-v2 | ✅ Complete | SASL authentication |

## Client Integration

### Java Client

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
          "org.apache.kafka.common.serialization.StringSerializer");
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
          "org.apache.kafka.common.serialization.StringSerializer");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);

// Send message
ProducerRecord<String, String> record = 
    new ProducerRecord<>("my-topic", "key", "Hello FluxMQ!");
producer.send(record);

producer.close();
```

### Python Client

```python
from kafka import KafkaProducer, KafkaConsumer

# Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: x.encode('utf-8')
)

producer.send('my-topic', 'Hello from Python!')
producer.flush()

# Consumer
consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='python-consumer-group'
)

for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")
```

### Node.js Client

```javascript
const kafka = require('kafkajs');

const client = kafka({
    clientId: 'fluxmq-client',
    brokers: ['localhost:9092']
});

// Producer
const producer = client.producer();
await producer.connect();

await producer.send({
    topic: 'my-topic',
    messages: [
        { key: 'key1', value: 'Hello FluxMQ from Node.js!' }
    ]
});

await producer.disconnect();

// Consumer
const consumer = client.consumer({ groupId: 'nodejs-group' });
await consumer.connect();
await consumer.subscribe({ topic: 'my-topic' });

await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
        console.log({
            topic,
            partition,
            offset: message.offset,
            value: message.value.toString(),
        });
    },
});
```

## Performance APIs

### Benchmarking

```rust
use fluxmq::performance::{PerformanceBenchmark, BenchmarkConfig};

let config = BenchmarkConfig {
    message_count: 100_000,
    message_size: 1024,
    batch_size: 100,
    thread_count: 4,
    topic_name: "benchmark-topic".to_string(),
};

let benchmark = PerformanceBenchmark::new(config);
let results = benchmark.run_producer_benchmark().await?;

println!("Throughput: {} msg/sec", results.messages_per_second);
println!("Latency P99: {} ms", results.p99_latency_ms);
println!("Total time: {} ms", results.total_time_ms);
```

### Performance Monitoring

```rust
// Get real-time performance metrics
let metrics = server.metrics();
let snapshot = metrics.get_snapshot().await;

println!("Performance Metrics:");
println!("- Messages/sec: {}", snapshot.messages_per_second);
println!("- Active connections: {}", snapshot.active_connections);
println!("- Memory usage: {} MB", snapshot.memory_usage_mb);
println!("- CPU usage: {}%", snapshot.cpu_usage_percent);
println!("- Disk I/O: {} MB/s", snapshot.disk_io_mb_per_sec);
```

## Administrative APIs

### Topic Management

```rust
use fluxmq::topic_manager::{TopicManager, TopicConfig};

let topic_manager = TopicManager::new();

// Create topic
let config = TopicConfig {
    num_partitions: 6,
    replication_factor: 1,
    segment_size: 1024 * 1024 * 1024, // 1GB
    retention_ms: Some(7 * 24 * 60 * 60 * 1000), // 7 days
};

topic_manager.create_topic("user-events", config)?;

// List topics
let topics = topic_manager.list_topics();
for topic in topics {
    println!("Topic: {} ({} partitions)", topic.name, topic.num_partitions);
}

// Delete topic
topic_manager.delete_topic("old-topic")?;
```

### Consumer Group Management

```rust
use fluxmq::consumer::{ConsumerGroupManager, GroupStats};

let group_manager = ConsumerGroupManager::new();

// List consumer groups
let groups = group_manager.list_groups().await?;
for group in groups {
    println!("Group: {} (state: {:?})", group.group_id, group.state);
}

// Get group details
let group_info = group_manager.describe_group("my-group").await?;
println!("Group {} has {} members", group_info.group_id, group_info.members.len());

// Get group statistics
let stats = group_manager.get_group_stats("my-group").await?;
println!("Group stats: {} partitions assigned", stats.total_assigned_partitions);
```

## Error Handling

### FluxmqError Enum

```rust
use fluxmq::{FluxmqError, Result};

fn handle_fluxmq_operation() -> Result<()> {
    match some_operation() {
        Ok(result) => {
            println!("Operation successful: {:?}", result);
            Ok(())
        }
        Err(FluxmqError::Storage(io_error)) => {
            eprintln!("Storage error: {}", io_error);
            // Handle storage issues (disk full, permissions, etc.)
            Err(FluxmqError::Storage(io_error))
        }
        Err(FluxmqError::Network(msg)) => {
            eprintln!("Network error: {}", msg);
            // Handle network issues (connection lost, timeout, etc.)
            Err(FluxmqError::Network(msg))
        }
        Err(FluxmqError::KafkaCodec(codec_error)) => {
            eprintln!("Protocol error: {}", codec_error);
            // Handle protocol parsing issues
            Err(FluxmqError::KafkaCodec(codec_error))
        }
        Err(FluxmqError::Config(config_error)) => {
            eprintln!("Configuration error: {}", config_error);
            // Handle configuration validation issues
            Err(FluxmqError::Config(config_error))
        }
        Err(other) => {
            eprintln!("Other error: {}", other);
            Err(other)
        }
    }
}
```

### Common Error Scenarios

**Storage Errors:**
```rust
// Handle disk space issues
match storage.append(topic, partition, messages).await {
    Err(FluxmqError::Storage(ref e)) if e.kind() == ErrorKind::WriteZero => {
        eprintln!("Disk full! Cannot write more data.");
        // Implement cleanup or alerting logic
    }
    Ok(offset) => println!("Messages stored at offset: {}", offset),
    Err(e) => eprintln!("Storage error: {}", e),
}
```

**Network Errors:**
```rust
// Handle connection issues
match server.accept_connection().await {
    Err(FluxmqError::Network(msg)) if msg.contains("Too many connections") => {
        eprintln!("Connection limit reached, rejecting new connections");
        // Implement backpressure or load balancing
    }
    Ok(connection) => {
        // Process new connection
    }
    Err(e) => eprintln!("Network error: {}", e),
}
```

## Best Practices

### Performance Optimization

**1. Batch Operations**
```rust
// Good: Batch multiple messages
let messages = vec![msg1, msg2, msg3, msg4, msg5];
storage.append(topic, partition, messages).await?;

// Avoid: Individual message operations
// storage.append(topic, partition, vec![msg1]).await?;
// storage.append(topic, partition, vec![msg2]).await?;
```

**2. Connection Management**
```rust
// Good: Reuse connections
let producer = KafkaProducer::new(config);
for message in messages {
    producer.send(message).await?;
}

// Avoid: Creating new connections per message
// for message in messages {
//     let producer = KafkaProducer::new(config);
//     producer.send(message).await?;
// }
```

**3. Memory Management**
```rust
// Good: Use Arc for shared data
let metrics = Arc::new(MetricsRegistry::new());
let metrics_clone = Arc::clone(&metrics);

// Good: Use proper buffer sizes
let config = BrokerConfig {
    buffer_size: 64 * 1024, // 64KB per connection
    ..Default::default()
};
```

### Security Configuration

**1. TLS Setup**
```rust
let config = BrokerConfig {
    enable_tls: true,
    tls_cert: Some("server.crt".into()),
    tls_key: Some("server.key".into()),
    tls_ca: Some("ca.crt".into()), // For mutual TLS
    ..Default::default()
};
```

**2. ACL Configuration**
```json
{
  "rules": [
    {
      "principal": "User:alice",
      "operation": "Read",
      "resource": "Topic:user-events",
      "allow": true
    },
    {
      "principal": "User:producer-service",
      "operation": "Write",
      "resource": "Topic:*",
      "allow": true
    }
  ]
}
```

### Monitoring Setup

**1. Metrics Collection**
```rust
let metrics = Arc::new(MetricsRegistry::new());

// Start background metrics tasks
let metrics_clone = Arc::clone(&metrics);
tokio::spawn(async move {
    metrics_clone.start_background_tasks().await;
});

// Start HTTP metrics endpoint
let http_server = HttpMetricsServer::new(8080, metrics).await?;
tokio::spawn(async move {
    http_server.run().await
});
```

**2. Health Checks**
```rust
// Implement health check endpoint
async fn health_check(metrics: Arc<MetricsRegistry>) -> Result<HealthStatus> {
    let snapshot = metrics.get_snapshot().await;
    
    if snapshot.active_connections > 10000 {
        return Ok(HealthStatus::Degraded("High connection count".into()));
    }
    
    if snapshot.disk_usage_percent > 90 {
        return Ok(HealthStatus::Critical("Disk space low".into()));
    }
    
    Ok(HealthStatus::Healthy)
}
```

### Deployment Patterns

**1. Single Node**
```rust
// Simple deployment for development or small workloads
let config = BrokerConfig {
    port: 9092,
    data_dir: Some("/var/lib/fluxmq".into()),
    max_connections: 1000,
    ..Default::default()
};
```

**2. High Availability**
```rust
// Production deployment with replication
let config = BrokerConfig {
    port: 9092,
    enable_replication: true,
    cluster_nodes: vec![
        "fluxmq-1:9092".to_string(),
        "fluxmq-2:9092".to_string(),
        "fluxmq-3:9092".to_string(),
    ],
    data_dir: Some("/mnt/storage/fluxmq".into()),
    max_connections: 10000,
    ..Default::default()
};
```

---

## API Versioning

FluxMQ follows semantic versioning for API compatibility:

- **Major version changes**: Breaking API changes
- **Minor version changes**: New features, backward compatible
- **Patch version changes**: Bug fixes, no API changes

Current API version: `1.0.0`

## Support and Resources

- **Documentation**: `cargo doc --open` for latest API docs
- **Examples**: See `examples/` directory for complete usage patterns
- **Performance Guide**: See `DEVELOPER_GUIDE.md` for optimization tips
- **Contributing**: See `CONTRIBUTING.md` for development workflow

---

*This API reference is automatically updated with each release. For the most current documentation, always refer to `cargo doc --open`.*