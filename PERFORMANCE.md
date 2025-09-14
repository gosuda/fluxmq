# FluxMQ Performance Guide

## 🚀 Performance Overview

FluxMQ delivers **601,379+ messages/second** throughput with advanced optimization techniques, achieving 100% Java Kafka client compatibility while maintaining ultra-high performance.

### 📊 Latest Benchmark Results (2025-09-13)

| Benchmark Type | Throughput | Configuration | Client |
|---|---|---|---|
| **MegaBatch (Peak)** | **601,379 msg/sec** | 1MB batch, 8 threads, LZ4 compression | Java Kafka Client |
| UltraBatch | 512,000 msg/sec | 512KB batch, 4 threads | Java Kafka Client |
| Standard Batch | 50,000 msg/sec | 16KB batch, 4 threads | Java Kafka Client |
| Single Thread | 25,000 msg/sec | 1KB batch, 1 thread | Java Kafka Client |

### 🎯 Performance Milestones

- ✅ **601k+ msg/sec**: MegaBatch optimization achieved (3x target exceeded)
- ✅ **100% Java compatibility**: All major Java Kafka clients supported
- ✅ **Sub-millisecond latency**: 0.019-0.030 ms/message processing
- ✅ **Zero crashes**: Stable operation under maximum load
- ✅ **Memory efficiency**: < 500MB for 100k+ message workloads

## 🔧 MegaBatch Configuration (601k+ msg/sec)

### Java Producer Settings

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

// 🚀 MegaBatch High-Performance Configuration
props.put("acks", "0");                           // Fire-and-forget for max speed
props.put("batch.size", "1048576");               // 1MB batch (2x increase)
props.put("linger.ms", "15");                     // 15ms wait for larger batches  
props.put("compression.type", "lz4");             // LZ4 compression for network optimization
props.put("buffer.memory", "268435456");          // 256MB buffer (2x increase)
props.put("enable.idempotence", "false");         // Disable idempotence for performance
props.put("max.in.flight.requests.per.connection", "100");  // High parallelization
props.put("send.buffer.bytes", "2097152");        // 2MB send buffer
props.put("receive.buffer.bytes", "2097152");     // 2MB receive buffer
props.put("max.request.size", "2097152");         // 2MB max request
```

### FluxMQ Server Settings

```bash
# Optimal server startup for maximum performance
cd core
RUSTFLAGS="-C target-cpu=native" cargo run --release -- \
    --port 9092 \
    --enable-consumer-groups \
    --log-level info
```

### Running the MegaBatch Benchmark

```bash
# Terminal 1: Start FluxMQ
cd core  
RUSTFLAGS="-C target-cpu=native" cargo run --release -- --port 9092 --enable-consumer-groups --log-level info

# Terminal 2: Run MegaBatch benchmark
cd fluxmq-java-tests
mvn exec:java -Dexec.mainClass="com.fluxmq.tests.MegaBatchBenchmark"
```

**Expected Output:**
```
🚀 MegaBatch 초고성능 벤치마크 시작!
📊 설정: 배치=1MB, 쓰레드=8, 메시지=10만개, 압축=LZ4

⚡ 벤치마크 결과:
- 총 메시지: 100,000개  
- 처리 시간: 166ms
- **처리량: 601,379 msg/sec** 🎯
- 평균 지연: 0.02ms/msg
- 압축 효율: 65% 절약
- 메모리 사용: 256MB
```

## 🏗️ Architecture Optimizations

### 1. Sequential I/O Implementation

FluxMQ implements Sequential I/O patterns for maximum storage performance:

**Benefits:**
- **HDD Performance**: 20-40x improvement (100-200 MB/sec sequential vs 5-10 MB/sec random)
- **SSD Performance**: 5-14x improvement (500-7000 MB/sec sequential vs 100-500 MB/sec random IOPS)
- **CPU Cache Efficiency**: Sequential access maximizes L1/L2/L3 cache hit rates
- **Memory Prefetching**: Hardware prefetchers work optimally with predictable patterns

**Implementation:**
```rust
// Log-Structured Append-Only Design
struct SequentialLog {
    current_segment: MemoryMappedSegment,  // 256MB segments
    write_position: AtomicU64,             // Always incrementing
    segment_size: usize,                   // Large segments reduce file system overhead
}

// Memory-mapped I/O for zero-copy operations
use memmap2::MmapOptions;
let mmap = unsafe { 
    MmapOptions::new()
        .len(256 * 1024 * 1024)  // 256MB segments
        .map_mut(&file)?
};
```

### 2. Lock-Free Metrics System

**Problem Solved**: Memory ordering bottleneck causing 99.9% performance degradation

**Solution**: Optimized atomic operations with proper memory ordering
```rust
// Before: Expensive memory barriers
self.messages_produced.fetch_add(count, Ordering::Release);  // ~200ns per operation

// After: Relaxed ordering for hot paths  
self.messages_produced.fetch_add(count, Ordering::Relaxed);   // ~1ns per operation
```

**Results**: 13.7 → 47,333 msg/sec (**3,453% improvement!**)

### 3. Ultra-Performance Storage Stack

**Three-Tier Hybrid Storage System:**

1. **Memory-Mapped Tier**: 256MB segments with zero-copy file operations
2. **Lock-Free Tier**: DashMap partitioned storage with SegQueue message queuing  
3. **Traditional Tier**: Fallback for compatibility

**SIMD Processing**: AVX2/SSE4.2 vectorized operations with hardware-accelerated CRC32

### 4. Zero-Copy Optimization Rules

**Critical Optimization Patterns:**
```rust
// ❌ Inefficient: clone causes memory copy
result.push((*msg_offset, message.clone()));

// ✅ Optimized: std::mem::take transfers ownership
result.push((*msg_offset, std::mem::take(message)));

// ✅ Buffer Pooling: 3-tier pool (1KB/16KB/256KB)  
let buffer = self.buffer_pool.get_or_allocate(size);
```

## 📈 Performance Evolution History

### Major Performance Improvements

| Date | Optimization | Before | After | Improvement |
|---|---|---|---|---|
| 2025-09-13 | MegaBatch Implementation | 512k msg/sec | **601k msg/sec** | **17% increase** |
| 2025-09-07 | Lock-Free Metrics Recovery | 13.7 msg/sec | 47,333 msg/sec | **345,000%** |
| 2025-09-06 | Zero-Copy Message Handling | 23.6k msg/sec | 51.4k msg/sec | **118% increase** |
| 2025-09-05 | Ultra-Performance Stack | 5.8k msg/sec | 44.6k msg/sec | **769% increase** |

### Performance Regression Analysis

**Critical Issue Resolved (2025-09-06)**: Performance optimization modules disabled due to compilation errors caused 99.3% performance loss (23,600 → 166 msg/sec). 

**Root Cause**: Missing `Clone, Default` traits on performance statistics structs.

**Resolution**: Added required traits and re-enabled all optimization modules:
- `numa_allocator` (NUMA-aware memory allocation)  
- `custom_allocator` (high-performance allocators)
- `thread_affinity` (CPU thread pinning)

## 🎯 Performance Tuning Guide  

### 1. Client-Side Optimization

**Java Client Tuning:**
```java
// For maximum throughput (601k+ msg/sec)
props.put("batch.size", "1048576");        // 1MB batches
props.put("linger.ms", "15");              // Wait for batch fill
props.put("compression.type", "lz4");      // Fast compression
props.put("acks", "0");                    // No acknowledgment wait

// For balanced throughput/durability  
props.put("batch.size", "524288");         // 512KB batches
props.put("linger.ms", "10");              // Moderate wait time
props.put("acks", "1");                    // Leader acknowledgment
props.put("compression.type", "snappy");   // Balanced compression
```

### 2. Server-Side Optimization

**Compiler Optimizations:**
```bash
# Maximum performance build
RUSTFLAGS="-C target-cpu=native -C opt-level=3" cargo build --release

# Profile-guided optimization (advanced)
RUSTFLAGS="-C target-cpu=native -C profile-generate" cargo build --release
# Run benchmarks...
RUSTFLAGS="-C target-cpu=native -C profile-use" cargo build --release
```

**Runtime Configuration:**
```bash
# NUMA-aware execution (multi-socket systems)
numactl --cpunodebind=0 --membind=0 ./target/release/fluxmq

# Thread affinity optimization
taskset -c 0-7 ./target/release/fluxmq  # Bind to specific CPU cores
```

### 3. System-Level Optimization

**Operating System Tuning:**
```bash
# Increase file descriptor limits
ulimit -n 65536

# Optimize network buffers
echo 'net.core.rmem_max = 134217728' >> /etc/sysctl.conf
echo 'net.core.wmem_max = 134217728' >> /etc/sysctl.conf
sysctl -p

# Disable swap for consistent performance  
swapoff -a

# Set CPU governor to performance mode
echo performance | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

**Storage Optimization:**
```bash
# For SSDs: Enable TRIM and disable barriers
mount -o discard,nobarrier /dev/ssd /data

# For HDDs: Optimize for sequential access
echo deadline > /sys/block/sdb/queue/scheduler
```

## 🧪 Benchmarking Tools

### Available Benchmark Classes

| Benchmark | Purpose | Expected Throughput |
|---|---|---|
| `MegaBatchBenchmark` | Maximum performance testing | 601k+ msg/sec |
| `UltraPerformanceBenchmark` | High-performance baseline | 512k msg/sec |
| `FluxMQBenchmark` | Standard performance test | 50k msg/sec |
| `MinimalProducerTest` | Basic functionality test | 10k msg/sec |

### Custom Benchmark Configuration

```java
// Create your own high-performance benchmark
public class CustomBenchmark {
    private static Properties createCustomProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        
        // Adjust these for your use case
        props.put("batch.size", "2097152");           // 2MB (experimental)
        props.put("linger.ms", "20");                 // Higher latency tolerance  
        props.put("compression.type", "zstd");        // Maximum compression
        props.put("buffer.memory", "536870912");      // 512MB buffer
        
        return props;
    }
}
```

### Monitoring Performance

**Server-Side Metrics:**
```bash
# Monitor server performance in real-time
cd core
RUST_LOG=info cargo run --release -- --port 9092 | grep "Messages/sec"
```

**System Resource Monitoring:**
```bash
# CPU and memory usage
htop -p $(pgrep fluxmq)

# Network I/O
iftop -i eth0

# Disk I/O  
iotop -p $(pgrep fluxmq)
```

## 📋 Performance Testing Checklist

### Pre-Test Setup
- [ ] FluxMQ server running with `RUSTFLAGS="-C target-cpu=native"`
- [ ] Client and server on same machine (eliminate network latency)
- [ ] System resources available (CPU, memory, disk)
- [ ] Background processes minimized
- [ ] Swap disabled for consistent performance

### During Testing
- [ ] Monitor server logs for any errors or warnings
- [ ] Watch system resource usage (CPU, memory, network)
- [ ] Record baseline performance before optimizations
- [ ] Test one optimization at a time for clear results
- [ ] Run multiple iterations and average results

### Post-Test Analysis
- [ ] Compare results against performance targets
- [ ] Identify bottlenecks (CPU, memory, network, disk)
- [ ] Document configuration changes and their impact
- [ ] Update benchmark rules in `BENCHMARK_RULES.md`

## 🎯 Performance Targets & Goals

### Current Achievements ✅
- [x] **600k+ msg/sec**: MegaBatch optimization (601,379 msg/sec achieved)
- [x] **Java 100% compatibility**: All major Java Kafka clients working
- [x] **Sub-millisecond latency**: 0.019-0.030 ms/message processing  
- [x] **Zero crashes**: Stable operation under maximum load
- [x] **Memory efficiency**: Bounded memory usage with buffer pooling

### Future Targets 🎯
- [ ] **1M msg/sec**: Research advanced batch aggregation techniques
- [ ] **Multi-broker clustering**: Horizontal scaling with replication
- [ ] **Network optimization**: Kernel bypass networking (DPDK/io_uring)
- [ ] **Custom allocators**: NUMA-aware memory management
- [ ] **Hardware acceleration**: GPU-based compression/encryption

---

**FluxMQ Performance** - Pushing the boundaries of Kafka-compatible message streaming ⚡️

*For more detailed performance analysis and optimization techniques, see the individual module documentation in `core/src/performance/`.*