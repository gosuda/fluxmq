# FluxMQ Rust Client SDK

A high-performance, async Rust client library for FluxMQ message broker.

## Features

- 🚀 **High Performance**: Zero-copy operations with `bytes::Bytes`
- ⚡ **Async/Await**: Built on tokio for non-blocking I/O  
- 🔄 **Connection Pooling**: Efficient connection reuse and management
- 🎯 **Auto-Partitioning**: Hash-based and round-robin partition assignment
- 📦 **Batch Operations**: Efficient message batching for throughput
- 🛡️ **Type Safety**: Strong typing with comprehensive error handling
- 📊 **Observability**: Built-in metrics and tracing support
- 🔧 **Builder Pattern**: Intuitive configuration with builders

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
fluxmq-client = "0.1.0"
tokio = { version = "1.0", features = ["full"] }
```

## Quick Start

### Producer Example

```rust
use fluxmq_client::*;

#[tokio::main]
async fn main() -> Result<(), FluxmqClientError> {
    // Create producer
    let producer = ProducerBuilder::new()
        .brokers(vec!["localhost:9092"])
        .acks(1)
        .build()
        .await?;
    
    // Send a simple message
    let record = ProduceRecord::new("my-topic", "Hello FluxMQ!");
    let metadata = producer.send(record).await?;
    
    println!("Message sent to partition {} at offset {}", 
             metadata.partition, metadata.offset);
    
    producer.close().await?;
    Ok(())
}
```

### Consumer Example

```rust
use fluxmq_client::*;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), FluxmqClientError> {
    // Create consumer
    let consumer = ConsumerBuilder::new()
        .brokers(vec!["localhost:9092"])
        .group_id("my-consumer-group")
        .topics(vec!["my-topic"])
        .build()
        .await?;
    
    // Consume messages as a stream
    let mut stream = consumer.stream();
    while let Some(record) = stream.next().await {
        match record {
            Ok(record) => {
                println!("Received: key={:?}, value={}", 
                         record.key, String::from_utf8_lossy(&record.value));
                consumer.commit_sync().await?;
            }
            Err(e) => eprintln!("Error: {}", e),
        }
    }
    
    consumer.close().await?;
    Ok(())
}
```

## Advanced Usage

### Producer with All Options

```rust
let producer = ProducerBuilder::new()
    .brokers(vec!["broker1:9092", "broker2:9092"])
    .acks(-1) // Wait for all replicas
    .max_message_size(10 * 1024 * 1024) // 10MB
    .delivery_timeout(Duration::from_secs(60))
    .build()
    .await?;

// Rich record with metadata
let record = ProduceRecord::builder()
    .topic("orders")
    .key("user-123")
    .value(serde_json::to_string(&order)?)
    .header("content-type", "application/json")
    .header("source", "web-app")
    .timestamp(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64)
    .build();

let metadata = producer.send(record).await?;
```

### Batch Producer

```rust
let records = vec![
    ProduceRecord::with_key("topic", "key1", "value1"),
    ProduceRecord::with_key("topic", "key2", "value2"),
    ProduceRecord::with_key("topic", "key3", "value3"),
];

let batch_metadata = producer.send_batch(records).await?;
for metadata in batch_metadata {
    println!("Sent to partition {} offset {}", 
             metadata.partition, metadata.offset);
}
```

### Poll-based Consumer

```rust
let consumer = ConsumerBuilder::new()
    .brokers(vec!["localhost:9092"])
    .group_id("my-group")
    .topics(vec!["topic1", "topic2"])
    .max_poll_records(500)
    .session_timeout(Duration::from_secs(30))
    .build()
    .await?;

loop {
    let records = consumer.poll().await?;
    for record in records {
        // Process record
        process_record(record).await?;
    }
    consumer.commit_sync().await?;
}
```

### Admin Operations

```rust
use fluxmq_client::*;

let admin = AdminClient::new(ClientConfig::default()).await?;

// List all topics
let topics = admin.list_topics().await?;
println!("Topics: {:?}", topics);

// Get topic metadata
let metadata = admin.get_metadata(vec!["my-topic".to_string()]).await?;
for topic in metadata {
    println!("Topic: {}, Partitions: {}", 
             topic.name, topic.partitions.len());
}
```

## Configuration

### Producer Configuration

```rust
let config = ProducerConfigBuilder::new()
    .brokers(vec!["localhost:9092"])
    .acks(1)                                    // 0, 1, or -1
    .max_message_size(1024 * 1024)             // 1MB
    .delivery_timeout(Duration::from_secs(30))
    .build();

let producer = Producer::new(config).await?;
```

### Consumer Configuration

```rust
let config = ConsumerConfigBuilder::new()
    .brokers(vec!["localhost:9092"])
    .group_id("my-group")
    .topics(vec!["topic1", "topic2"])
    .session_timeout(Duration::from_secs(10))
    .heartbeat_interval(Duration::from_secs(3))
    .max_poll_records(500)
    .build();

let consumer = Consumer::new(config).await?;
```

## Error Handling

The client provides comprehensive error handling:

```rust
match producer.send(record).await {
    Ok(metadata) => println!("Success: {:?}", metadata),
    Err(FluxmqClientError::Connection { message }) => {
        eprintln!("Connection error: {}", message);
        // Retry logic here
    },
    Err(FluxmqClientError::Timeout { timeout_ms }) => {
        eprintln!("Timeout after {}ms", timeout_ms);
        // Timeout handling
    },
    Err(FluxmqClientError::TopicNotFound { topic }) => {
        eprintln!("Topic '{}' not found", topic);
        // Topic creation logic
    },
    Err(e) => eprintln!("Other error: {}", e),
}
```

## Metrics and Observability

```rust
use fluxmq_client::metrics::global_metrics;

// Get metrics snapshot
let metrics = global_metrics().snapshot();
println!("Records sent: {}", metrics.records_sent);
println!("Avg send latency: {:.2}μs", metrics.average_send_latency_us);
println!("Records consumed: {}", metrics.records_consumed);
```

## Examples

Run the examples:

```bash
# Start FluxMQ broker first
cargo run --bin fluxmq -- --port 9092

# Run producer example
cargo run --example simple_producer

# Run consumer example (in another terminal)
cargo run --example simple_consumer
```

## Performance

The Rust SDK is designed for high performance:

- **Zero-copy**: Uses `bytes::Bytes` for message data
- **Connection pooling**: Reuses connections efficiently  
- **Async I/O**: Non-blocking operations with tokio
- **Batch processing**: Sends multiple messages in single requests
- **Low allocations**: Minimizes memory allocations in hot paths

Benchmark results show throughput comparable to the core FluxMQ broker.

## Compatibility

- **Rust**: 1.70+ (2021 edition)
- **FluxMQ**: Compatible with FluxMQ 0.1.0+
- **Platforms**: Linux, macOS, Windows
- **Async Runtime**: tokio 1.0+

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License - see [LICENSE](../LICENSE) file for details.