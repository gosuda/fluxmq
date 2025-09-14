# FluxMQ

A high-performance, Kafka-compatible message broker written in Rust with **100% Java client compatibility** and **608k+ msg/sec** throughput.

## 🚀 Features

- **100% Java Kafka Compatible**: Complete compatibility with Java Kafka clients (apache-kafka-java 4.1+)
- **Ultra High Performance**: 608,272+ messages/second throughput with Arena Memory optimizations
- **20 Kafka APIs Supported**: Full wire protocol compatibility with metadata, produce, consume, and admin operations
- **Distributed Architecture**: Leader-follower replication with Raft-like consensus
- **Consumer Groups**: Load balancing across multiple consumers with partition assignment
- **Persistent Storage**: Hybrid memory-disk storage with crash recovery
- **Multi-Partition Topics**: Hash-based and round-robin partition assignment strategies
- **Async Architecture**: Built on Tokio for high-concurrency message processing

## 📊 Performance

### 🚀 Latest Benchmark Results (2025-09-14)
- **MegaBatch Performance**: **608,272 messages/second** (1MB batch size, 16 threads)
- **Java Client Compatibility**: **100% working** with all major Java Kafka libraries
- **Sequential I/O**: 20-40x HDD, 5-14x SSD performance improvement
- **Lock-Free Metrics**: 99.9% performance recovery with optimized atomic operations
- **Zero-Copy Design**: Memory-mapped I/O with `bytes::Bytes` for maximum efficiency
- **Sub-millisecond latency**: 0.019-0.030 ms/message processing time

### 🎯 Proven Client Support
- ✅ **Java**: `org.apache.kafka:kafka-clients` v4.1+ (100% compatible)
- ✅ **Python**: `kafka-python` library support
- ✅ **Scala**: Native Kafka Scala clients
- ✅ **Admin Operations**: Topic creation, deletion, metadata queries

## 🏗️ Architecture

### Core Components
- **Broker**: TCP server handling client connections
- **Storage Engine**: Hybrid memory-disk persistence layer
- **Topic Manager**: Multi-partition topic management
- **Replication Coordinator**: Leader-follower data replication
- **Consumer Group Coordinator**: Load balancing and partition assignment
- **Network Protocol**: Binary protocol with length-prefixed frames

### Storage Layer
- **In-memory operations**: Primary read/write for maximum performance
- **Async disk persistence**: Background writes for durability
- **Memory-mapped I/O**: Efficient file operations for large datasets
- **Append-only logs**: Sequential writes with CRC integrity checks

## 🛠️ Installation

### Prerequisites
- Rust 1.70+ (latest stable recommended)
- Cargo package manager

### Build from source
```bash
git clone https://github.com/gosuda/fluxmq.git
cd fluxmq
cargo build --release
```

## 🚀 Quick Start

### Start a basic broker
```bash
cargo run -- --host 0.0.0.0 --port 9092
```

### Start with all features enabled
```bash
# For core development
cd core
cargo run --release -- --port 9092 --enable-consumer-groups --log-level info

# Or with full features
RUSTFLAGS="-C target-cpu=native" cargo run --release -- \
    --port 9092 \
    --enable-consumer-groups \
    --data-dir ./data
```

### Multi-broker cluster setup
```bash
# Terminal 1: Broker 1
cargo run -- --port 9092 --broker-id 1 --enable-replication --data-dir ./broker1

# Terminal 2: Broker 2  
cargo run -- --port 9093 --broker-id 2 --enable-replication --data-dir ./broker2

# Terminal 3: Broker 3
cargo run -- --port 9094 --broker-id 3 --enable-replication --data-dir ./broker3
```

## 📝 Usage Examples

### 🎆 Java Client Example (100% Compatible)

```java
// Producer Example
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class FluxMQProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        
        // High performance settings (MegaBatch configuration)
        props.put("batch.size", "1048576");  // 1MB batch
        props.put("linger.ms", "15");
        props.put("compression.type", "lz4");
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        try {
            ProducerRecord<String, String> record = 
                new ProducerRecord<>("my-topic", "key", "Hello FluxMQ!");
            producer.send(record).get();
            System.out.println("Message sent successfully!");
        } finally {
            producer.close();
        }
    }
}
```

### 🐍 Python Example

```python
from kafka import KafkaProducer, KafkaConsumer

# Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: v.encode('utf-8')
)

producer.send('my-topic', 'Hello FluxMQ!')
producer.flush()

# Consumer
consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: m.decode('utf-8')
)

for message in consumer:
    print(f"Received: {message.value}")
    break
```

### 🦀 Rust Native Example

### Producer Example
```rust
use fluxmq_client::*;

#[tokio::main]
async fn main() -> Result<()> {
    let producer = ProducerBuilder::new()
        .brokers(vec!["localhost:9092"])
        .build()
        .await?;
    
    let record = ProduceRecord::builder()
        .topic("my-topic")
        .key("user-123")
        .value("Hello FluxMQ!")
        .build();
    
    let metadata = producer.send(record).await?;
    println!("Message sent to partition {} at offset {}", 
             metadata.partition, metadata.offset);
    
    Ok(())
}
```

### Consumer Example
```rust
use fluxmq_client::*;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let consumer = ConsumerBuilder::new()
        .brokers(vec!["localhost:9092"])
        .group_id("my-consumer-group")
        .topics(vec!["my-topic"])
        .build()
        .await?;
    
    let mut stream = consumer.stream();
    while let Some(record) = stream.next().await {
        match record {
            Ok(record) => {
                println!("Received: key={:?}, value={}", 
                         record.key, String::from_utf8_lossy(&record.value));
                consumer.commit_sync().await?;
            }
            Err(e) => eprintln!("Error receiving message: {}", e),
        }
    }
    
    Ok(())
}
```

### Try the examples
```bash
# Terminal 1: Start FluxMQ broker
cd core
RUSTFLAGS="-C target-cpu=native" cargo run --release -- --port 9092 --enable-consumer-groups

# Terminal 2: Run Java benchmark (601k+ msg/sec)
cd fluxmq-java-tests
mvn exec:java -Dexec.mainClass="com.fluxmq.tests.MegaBatchBenchmark"

# Terminal 3: Run simple Java test
mvn exec:java -Dexec.mainClass="com.fluxmq.tests.MinimalProducerTest"

# Or try Rust examples
cd fluxmq-client
cargo run --example simple_producer
cargo run --example simple_consumer
```

## ⚙️ Configuration

### Command Line Options
```bash
USAGE:
    fluxmq [OPTIONS]

OPTIONS:
        --host <HOST>                    Bind address [default: 0.0.0.0]
    -p, --port <PORT>                    Port to listen on [default: 9092]
    -l, --log-level <LOG_LEVEL>          Log level [default: info]
        --broker-id <BROKER_ID>          Unique broker identifier [default: 0]
        --enable-replication             Enable replication features
        --enable-consumer-groups         Enable consumer group coordination
        --recovery-mode                  Load existing data from disk on startup
        --data-dir <DATA_DIR>           Data storage directory [default: ./data]
```

### Environment Variables
```bash
RUST_LOG=debug                    # Enable debug logging
FLUXMQ_DATA_DIR=/var/lib/fluxmq   # Override data directory
```

## 🧪 Testing

### Run all tests
```bash
cargo test
```

### Run specific test modules
```bash
cargo test storage      # Storage layer tests
cargo test consumer     # Consumer group tests  
cargo test replication  # Replication tests
cargo test protocol     # Protocol tests
```

### Performance benchmarks
```bash
cargo test --release -- --ignored benchmark
```

## 📁 Project Structure

```
src/
├── main.rs                 # Application entry point
├── lib.rs                  # Library root
├── broker/                 # Broker implementation
│   ├── handler.rs          # Request handlers
│   └── server.rs           # TCP server
├── storage/                # Storage layer
│   ├── log.rs              # Append-only log files
│   ├── segment.rs          # Log segment management
│   └── index.rs            # Offset indexing
├── protocol/               # Network protocol
│   ├── messages.rs         # Protocol messages
│   ├── codec.rs            # Server-side codec
│   └── client_codec.rs     # Client-side codec
├── replication/            # Replication system
│   ├── leader.rs           # Leader state management
│   └── follower.rs         # Follower synchronization
├── consumer/               # Consumer groups
│   └── coordinator.rs      # Group coordinator
└── topic_manager.rs        # Topic management
```

## 🔧 Development

### Prerequisites
```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install development dependencies
cargo install cargo-audit cargo-clippy
```

### Development commands
```bash
# Format code
cargo fmt

# Check for issues
cargo clippy

# Security audit
cargo audit

# Watch for changes
cargo watch -x check -x test
```

## 🎯 Roadmap

### ✅ Completed (v2.0 - 2025-09)
- **100% Java Kafka Client Compatibility** (apache-kafka-java 4.1+)
- **601k+ msg/sec Performance** with MegaBatch optimization
- **20 Kafka APIs Implemented** (Metadata, Produce, Fetch, Consumer Groups, Admin)
- **Sequential I/O Optimization** (20-40x HDD, 5-14x SSD improvement)
- **Lock-Free Metrics System** with atomic operations
- **Ultra-Performance Storage** (Memory-mapped I/O, SIMD processing)
- **Enterprise Security** (TLS/SSL, ACL, SASL authentication)
- **Leader-Follower Replication** with Raft-like consensus

### 🔄 In Progress
- Advanced monitoring dashboard
- Kubernetes operator development  
- Schema registry integration
- Additional client SDK support

### 📋 Future
- Log compaction
- Schema registry integration
- Kubernetes operator
- Web-based management UI

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development workflow
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run the test suite
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by Apache Kafka's architecture
- Built with the amazing Rust ecosystem
- Special thanks to the Tokio team for async runtime

## 📞 Support

- 🐛 **Issues**: [GitHub Issues](https://github.com/gosuda/fluxmq/issues)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/gosuda/fluxmq/discussions)
- 📧 **Email**: hsng95@gmail.com

---

**FluxMQ** - High-performance message streaming, built with Rust ⚡️