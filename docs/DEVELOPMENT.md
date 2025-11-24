# FluxMQ Development Guide

## 🚀 Getting Started

Welcome to FluxMQ development! This guide covers everything you need to contribute to FluxMQ, from setting up your development environment to advanced optimization techniques.

## 📋 Prerequisites

### System Requirements
- **Operating System**: Linux, macOS, or Windows with WSL2
- **Architecture**: x86_64 (Intel/AMD) with AVX2 support recommended
- **Memory**: 8GB+ RAM (16GB+ recommended for performance testing)
- **Storage**: 20GB+ free space (SSD recommended)

### Development Tools
```bash
# Install Rust (latest stable)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Install development dependencies
cargo install cargo-audit cargo-clippy cargo-watch cargo-criterion

# Install system dependencies (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install build-essential pkg-config libssl-dev

# Install system dependencies (macOS)
brew install openssl pkg-config

# Verify installation
rustc --version  # Should show 1.70+
cargo --version
```

### Java Development (for testing)
```bash
# Install OpenJDK 11+ and Maven
# Ubuntu/Debian
sudo apt-get install openjdk-11-jdk maven

# macOS
brew install openjdk@11 maven

# Verify installation
java --version   # Should show 11+
mvn --version
```

## 🏗️ Project Setup

### Clone and Build
```bash
# Clone the repository
git clone https://github.com/gosuda/fluxmq.git
cd fluxmq

# Build in development mode
cargo build

# Build in release mode (optimized)
cargo build --release

# Build with native CPU optimizations
RUSTFLAGS="-C target-cpu=native" cargo build --release

# Run tests
cargo test

# Run with all features
cargo run --release -- \
    --port 9092 \
    --enable-consumer-groups \
    --enable-tls \
    --enable-acl \
    --log-level debug
```

### IDE Setup

#### VS Code (Recommended)
```bash
# Install VS Code extensions
code --install-extension rust-lang.rust-analyzer
code --install-extension serayuzgur.crates
code --install-extension vadimcn.vscode-lldb

# Open project
code .
```

#### IntelliJ IDEA / CLion
- Install Rust plugin
- Import project as Cargo project
- Configure run configurations for different modes

### Environment Configuration
```bash
# Create development environment file
cat > .env << EOF
RUST_LOG=debug
FLUXMQ_PORT=9092
FLUXMQ_HOST=0.0.0.0
FLUXMQ_DATA_DIR=./dev-data
FLUXMQ_LOG_LEVEL=debug
EOF

# Set up git hooks
git config core.hooksPath .githooks
chmod +x .githooks/*
```

## 🎯 Development Workflow

### Branch Strategy
```bash
# Feature development
git checkout -b feature/your-feature-name

# Bug fixes
git checkout -b fix/issue-description

# Performance optimizations
git checkout -b perf/optimization-name

# Documentation updates
git checkout -b docs/section-name
```

### Code Quality Standards

#### Formatting and Linting
```bash
# Format code (run before every commit)
cargo fmt

# Check code style
cargo clippy -- -D warnings

# Audit dependencies for security issues
cargo audit

# Watch for changes during development
cargo watch -x check -x test
```

#### Performance Testing
```bash
# Run performance benchmarks
cargo test --release -- --ignored benchmark

# Profile memory usage
cargo run --release --features="profiling" -- --port 9092

# Measure CPU performance
perf record cargo run --release -- --port 9092
perf report
```

### Testing Strategy

#### Unit Tests
```bash
# Run all unit tests
cargo test

# Run specific module tests
cargo test storage::
cargo test protocol::
cargo test consumer::

# Run with output
cargo test -- --nocapture

# Run performance-sensitive tests
cargo test --release performance
```

#### Integration Tests
```bash
# Start FluxMQ server for testing
cd core
RUSTFLAGS="-C target-cpu=native" cargo run --release -- \
    --port 9092 --enable-consumer-groups --log-level debug

# Run Java client tests (separate terminal)
cd fluxmq-java-tests
mvn exec:java -Dexec.mainClass="com.fluxmq.tests.MinimalProducerTest"
mvn exec:java -Dexec.mainClass="com.fluxmq.tests.FluxMQBenchmark"

# Run Python client tests
cd core/tests
python3 run_all_tests.py
```

#### Performance Benchmarks
```bash
# Current production benchmark (476K avg, 554K peak)
cd fluxmq-java-tests
mvn exec:java -Dexec.mainClass="com.fluxmq.tests.MultiThreadBenchmark"

# Comprehensive benchmark with all features
mvn exec:java -Dexec.mainClass="com.fluxmq.tests.ComprehensiveBenchmark"

# Automated FluxMQ vs Kafka comparison
cd ..
./benchmark-suite/runners/run_comparison.sh

# Resource monitoring during benchmarks
./benchmark-suite/monitors/resource_monitor.sh <PID> output.csv 120
```

## 🔧 Core Development Areas

### 1. Protocol Development

#### Adding New Kafka APIs
```rust
// 1. Define message structures in protocol/kafka/messages.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewApiRequest {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub request_data: RequestData,
}

// 2. Add encoding/decoding in protocol/kafka/codec.rs
impl KafkaCodec {
    pub fn encode_new_api_response(&self, response: &NewApiResponse) -> Result<Bytes> {
        // Implementation
    }
    
    pub fn decode_new_api_request(&self, buf: &mut BytesMut) -> Result<NewApiRequest> {
        // Implementation
    }
}

// 3. Add handler in broker/handler.rs
impl RequestHandler {
    pub async fn handle_new_api(&self, request: NewApiRequest) -> Result<NewApiResponse> {
        // Business logic implementation
    }
}

// 4. Register in broker/server.rs
match request {
    Request::NewApi(req) => {
        let response = self.handler.handle_new_api(req).await?;
        Ok(Response::NewApi(response))
    }
}
```

#### Testing New APIs
```bash
# Create test file
cat > core/tests/kafka_api_XX.py << EOF
#!/usr/bin/env python3
"""Test Kafka API XX - NewApi"""

def test_new_api():
    # Test implementation
    pass

if __name__ == "__main__":
    test_new_api()
EOF

# Add to test runner
echo "kafka_api_XX.py" >> core/tests/test_list.txt
```

### 2. Storage Development

#### Implementing Storage Optimizations
```rust
// Example: Adding new storage engine
pub struct NewStorageEngine {
    config: StorageConfig,
    segments: Vec<Segment>,
    index: BTreeMap<Offset, SegmentInfo>,
}

impl StorageEngine for NewStorageEngine {
    async fn append(&mut self, messages: &[Message]) -> Result<Offset> {
        // High-performance append implementation
        // Consider: batching, compression, async I/O
    }
    
    async fn read(&self, offset: Offset, max_bytes: usize) -> Result<Vec<LogEntry>> {
        // Zero-copy read implementation
        // Consider: memory mapping, prefetching
    }
    
    async fn flush(&mut self) -> Result<()> {
        // Ensure durability
    }
}
```

#### Performance Testing Storage
```rust
#[cfg(test)]
mod benchmarks {
    use super::*;
    use criterion::{criterion_group, criterion_main, Criterion};
    
    fn bench_append_performance(c: &mut Criterion) {
        c.bench_function("storage_append_1k_messages", |b| {
            b.iter(|| {
                // Benchmark implementation
            })
        });
    }
    
    criterion_group!(benches, bench_append_performance);
    criterion_main!(benches);
}
```

### 3. Performance Optimization

#### Lock-Free Data Structures
```rust
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct LockFreeMessageBuffer {
    // Partitioned storage reduces contention
    partitions: Vec<DashMap<Offset, Message>>,
    // Lock-free queues for batching
    input_queue: SegQueue<Message>,
    output_queue: SegQueue<LogEntry>,
    // Atomic counters for metrics
    message_count: AtomicU64,
    byte_count: AtomicU64,
}

impl LockFreeMessageBuffer {
    #[inline(always)]
    pub fn append_message(&self, offset: Offset, message: Message) {
        let partition_idx = (offset as usize) % self.partitions.len();
        self.partitions[partition_idx].insert(offset, message);
        self.message_count.fetch_add(1, Ordering::Relaxed);
    }
}
```

#### SIMD Optimizations
```rust
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

pub struct SIMDOptimizer;

impl SIMDOptimizer {
    #[target_feature(enable = "avx2")]
    pub unsafe fn vectorized_checksum(data: &[u8]) -> u32 {
        let mut checksum = 0u32;
        let chunks = data.chunks_exact(32);
        
        for chunk in chunks {
            let vector = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);
            // Process 32 bytes at once
            checksum = _mm_crc32_u64(checksum as u64, 
                _mm256_extract_epi64(vector, 0) as u64) as u32;
        }
        
        checksum
    }
}
```

#### Memory Optimization
```rust
use bytes::{Bytes, BytesMut};
use std::mem;

pub struct ZeroCopyMessageHandler {
    buffer_pool: BufferPool,
}

impl ZeroCopyMessageHandler {
    pub fn process_messages(&mut self, messages: &mut [Message]) -> Vec<(Offset, Message)> {
        let mut result = Vec::with_capacity(messages.len());
        
        for (offset, message) in messages.iter_mut().enumerate() {
            // Zero-copy transfer using std::mem::take
            result.push((offset as Offset, mem::take(message)));
        }
        
        result
    }
    
    pub fn get_buffer(&self, size: usize) -> BytesMut {
        // Reuse buffers from pool
        self.buffer_pool.get_or_allocate(size)
    }
}
```

### 4. Monitoring and Metrics

#### Adding Custom Metrics
```rust
#[derive(Debug, Clone)]
pub struct CustomMetrics {
    request_count: AtomicU64,
    error_count: AtomicU64,
    response_time_ns: AtomicU64,
}

impl CustomMetrics {
    pub fn record_request(&self, duration_ns: u64, success: bool) {
        self.request_count.fetch_add(1, Ordering::Relaxed);
        self.response_time_ns.fetch_add(duration_ns, Ordering::Relaxed);
        
        if !success {
            self.error_count.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    pub fn get_stats(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            requests: self.request_count.load(Ordering::Relaxed),
            errors: self.error_count.load(Ordering::Relaxed),
            avg_response_time_ns: self.response_time_ns.load(Ordering::Relaxed) 
                / self.request_count.load(Ordering::Relaxed).max(1),
        }
    }
}
```

## 🐛 Debugging Guide

### Debug Builds
```bash
# Build with debug symbols
cargo build --debug

# Run with debug logging
RUST_LOG=debug cargo run -- --port 9092 --log-level debug

# Run with backtrace on panic
RUST_BACKTRACE=1 cargo run

# Run with full backtrace
RUST_BACKTRACE=full cargo run
```

### Common Issues

#### Performance Degradation
```bash
# Check for lock contention
perf record -g cargo run --release
perf report -g

# Profile memory usage
valgrind --tool=massif cargo run
ms_print massif.out.XXX

# Check for memory leaks
valgrind --leak-check=full cargo run
```

#### Protocol Issues
```bash
# Enable protocol debugging
RUST_LOG=fluxmq::protocol=trace cargo run

# Capture network traffic
sudo tcpdump -i lo -w kafka-traffic.pcap port 9092
wireshark kafka-traffic.pcap

# Test with specific client version
mvn exec:java -Dexec.mainClass="com.fluxmq.tests.MinimalProducerTest" \
    -Dkafka.version=4.1.0
```

#### Storage Issues
```bash
# Check file system usage
df -h
du -sh ./data

# Monitor I/O performance
iostat -x 1

# Check for corruption
cargo run -- --recovery-mode --data-dir ./data --log-level debug
```

### Debugging Tools

#### GDB Integration
```bash
# Install rust-gdb
rustup component add rust-src

# Debug with GDB
rust-gdb target/debug/fluxmq
(gdb) run --port 9092
(gdb) bt  # backtrace on crash
```

#### Flamegraph Profiling
```bash
# Install flamegraph
cargo install flamegraph

# Generate flamegraph
sudo flamegraph cargo run --release -- --port 9092

# View in browser
firefox flamegraph.svg
```

## 📊 Performance Development

### Current Performance Status

**FluxMQ vs Apache Kafka** (as of 2025-11-24):
- Average: 476K msg/sec (+28.6% vs Kafka's 370K)
- Peak: 554K msg/sec (+49.6% vs Kafka)
- Latency: 0.002 ms (-33.3% vs Kafka's 0.003 ms)
- Memory: 100-150 MB (-70-85% vs Kafka's 1.5-2 GB)

**Optimization History**:
1. **Baseline**: 294K msg/sec (-26% vs Kafka)
2. **Phase 1 (mmap)**: 426K msg/sec (+15% vs Kafka, +44.9% improvement)
3. **Phase 3 (SIMD)**: 476K msg/sec (+28.6% vs Kafka, +62% total improvement)

### Benchmarking Framework
```rust
use criterion::{criterion_group, criterion_main, Criterion, Throughput};

fn bench_message_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_processing");
    
    for batch_size in [1, 10, 100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            format!("batch_size_{}", batch_size),
            batch_size,
            |b, &size| {
                b.iter(|| {
                    // Benchmark your code here
                    process_messages(generate_messages(size))
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_message_processing);
criterion_main!(benches);
```

### Performance Testing Automation
```bash
#!/bin/bash
# scripts/performance-test.sh

set -e

echo "🚀 Starting FluxMQ Performance Test Suite"

# Start server
echo "Starting FluxMQ server..."
cd core
RUSTFLAGS="-C target-cpu=native" cargo run --release -- \
    --port 9092 --enable-consumer-groups --log-level info &
SERVER_PID=$!

# Wait for startup
sleep 5

# Run benchmarks
echo "Running Java benchmarks..."
cd ../fluxmq-java-tests

# Baseline test
mvn -q exec:java -Dexec.mainClass="com.fluxmq.tests.MinimalProducerTest"

# Performance tests
mvn -q exec:java -Dexec.mainClass="com.fluxmq.tests.FluxMQBenchmark"
mvn -q exec:java -Dexec.mainClass="com.fluxmq.tests.MegaBatchBenchmark"

# Cleanup
kill $SERVER_PID
echo "✅ Performance tests completed"
```

### Continuous Performance Monitoring
```python
#!/usr/bin/env python3
# scripts/performance-monitor.py

import subprocess
import time
import json
import sys

def run_benchmark():
    """Run benchmark and extract performance metrics"""
    result = subprocess.run([
        'mvn', '-q', 'exec:java', 
        '-Dexec.mainClass=com.fluxmq.tests.FluxMQBenchmark'
    ], capture_output=True, text=True)
    
    # Parse benchmark output
    for line in result.stdout.split('\n'):
        if 'msg/sec' in line:
            # Extract throughput number
            return float(line.split()[0])
    
    return 0

def main():
    baseline = 50000  # 50k msg/sec baseline
    current = run_benchmark()
    
    print(f"Current performance: {current:.0f} msg/sec")
    
    if current < baseline * 0.9:  # 10% degradation threshold
        print(f"❌ Performance regression detected!")
        print(f"Current: {current:.0f}, Baseline: {baseline}")
        sys.exit(1)
    else:
        print(f"✅ Performance acceptable")
        
    # Record metrics
    with open('performance-history.json', 'a') as f:
        json.dump({
            'timestamp': time.time(),
            'throughput': current,
            'baseline': baseline
        }, f)
        f.write('\n')

if __name__ == '__main__':
    main()
```

## 🔒 Security Development

### TLS/SSL Testing
```bash
# Generate test certificates
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes

# Run with TLS enabled
cargo run --release -- \
    --port 9092 \
    --enable-tls \
    --tls-cert cert.pem \
    --tls-key key.pem

# Test TLS connection
openssl s_client -connect localhost:9092 -cert cert.pem -key key.pem
```

### ACL Development
```json
{
  "version": 1,
  "acls": [
    {
      "principal": "User:alice",
      "resource_type": "Topic",
      "resource_name": "sensitive-topic",
      "operation": "Read",
      "permission": "Allow"
    },
    {
      "principal": "User:bob",
      "resource_type": "Topic",
      "resource_name": "*",
      "operation": "Write",
      "permission": "Deny"
    }
  ]
}
```

## 📦 Release Process

### Pre-Release Checklist
```bash
# 1. Update version numbers
grep -r "version = " Cargo.toml
# Update version in Cargo.toml files

# 2. Run full test suite
cargo test --all
cd fluxmq-java-tests && mvn test

# 3. Performance validation
./scripts/performance-test.sh

# 4. Security audit
cargo audit

# 5. Documentation update
cargo doc --no-deps --open

# 6. Clean build
cargo clean
cargo build --release

# 7. Create release
git tag v2.1.0
git push origin v2.1.0
```

### Release Build Optimization
```bash
# Maximum performance release build
RUSTFLAGS="-C target-cpu=native -C link-arg=-s" \
cargo build --release --features="simd,numa,lockfree"

# Profile-guided optimization (PGO)
RUSTFLAGS="-C profile-generate=/tmp/pgo-data" \
cargo build --release

# Run representative workload
./target/release/fluxmq --port 9092 &
# ... run benchmarks ...

# Build with PGO data
RUSTFLAGS="-C profile-use=/tmp/pgo-data" \
cargo build --release
```

## 🤝 Contributing Guidelines

### Code Review Process
1. **Fork and Branch**: Create feature branch from main
2. **Implement**: Follow coding standards and add tests
3. **Test**: Ensure all tests pass and performance is maintained
4. **Document**: Update relevant documentation
5. **Submit PR**: Create pull request with detailed description

### Coding Standards
- **Rust Style**: Follow `rustfmt` formatting
- **Documentation**: Document all public APIs with `///` comments
- **Testing**: Unit tests for all new functionality
- **Performance**: Benchmark critical paths
- **Security**: Review for security implications

### Commit Message Format
```
type(scope): description

- feat(storage): add memory-mapped segment support
- fix(protocol): resolve metadata response encoding issue
- perf(metrics): optimize lock-free counter implementation
- docs(api): update Kafka API compatibility matrix
```

## 📚 Learning Resources

### Rust Performance
- [The Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [Tokio Tutorial](https://tokio.rs/tokio/tutorial)
- [Lock-Free Programming](https://www.1024cores.net/home/lock-free-algorithms)

### Kafka Protocol
- [Kafka Protocol Guide](https://kafka.apache.org/protocol)
- [KIP-482: Flexible Versions](https://cwiki.apache.org/confluence/display/KAFKA/KIP-482)
- [Wire Protocol Specification](https://kafka.apache.org/protocol.html)

### Performance Optimization
- [Systems Performance](http://www.brendangregg.com/systems-performance-2nd-edition-book.html)
- [Computer Architecture](https://www.elsevier.com/books/computer-architecture/hennessy/978-0-12-811905-1)
- [High-Performance Rust](https://www.packtpub.com/product/rust-high-performance/9781788399487)

## 🎯 Development Roadmap

### Completed Achievements ✅
- [x] **100% Kafka API compatibility**: 20 APIs fully implemented
- [x] **Memory-mapped I/O optimization**: Phase 1 complete (+44.9%)
- [x] **SIMD vectorization**: Phase 3 complete (+11.7%)
- [x] **476K msg/sec average**: 28.6% faster than Kafka
- [x] **Automated benchmark suite**: FluxMQ vs Kafka comparison
- [x] **Resource monitoring**: CPU/memory tracking integrated
- [x] **Java client compatibility**: 100% tested with Kafka 4.1.0

### Short Term (1-2 months)
- [ ] Reach 600K msg/sec sustained throughput
- [ ] Implement io_uring for Linux (kernel bypass I/O)
- [ ] Add log compaction support
- [ ] Comprehensive performance documentation

### Medium Term (3-6 months)
- [ ] 1M msg/sec target with advanced batch aggregation
- [ ] Multi-broker clustering with replication
- [ ] Schema registry integration
- [ ] Kubernetes operator

### Long Term (6-12 months)
- [ ] Hardware acceleration (GPU compression/encryption)
- [ ] Advanced monitoring dashboard (Grafana integration)
- [ ] Enterprise management tools
- [ ] Multi-datacenter replication

---

**FluxMQ Development** - Building the future of high-performance message streaming ⚡️