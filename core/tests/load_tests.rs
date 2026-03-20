//! Load tests for FluxMQ storage layer.
//!
//! These tests verify sustained throughput, latency distribution, and data integrity
//! under realistic production-like workloads. Run with:
//! ```
//! cargo test --release --test load_tests -- --nocapture --ignored
//! ```

use bytes::Bytes;
use fluxmq::storage::InMemoryStorage;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Maximum fetch size: 256 MB.
const MAX_FETCH_BYTES: u32 = 1024 * 1024 * 256;

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Create a test message with the given payload size.
fn make_message(size: usize) -> fluxmq::protocol::Message {
    fluxmq::protocol::Message {
        key: Some(Bytes::from("load-test-key")),
        value: Bytes::from(vec![0x42u8; size]),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        headers: HashMap::new(),
    }
}

/// Compute the value at a given percentile from a **sorted** slice of durations
/// (in microseconds or any consistent unit).
fn percentile(sorted_values: &[u128], pct: f64) -> u128 {
    assert!(
        !sorted_values.is_empty(),
        "percentile called on empty slice"
    );
    let idx = ((pct / 100.0) * (sorted_values.len() as f64 - 1.0)).round() as usize;
    sorted_values[idx.min(sorted_values.len() - 1)]
}

/// Paginated fetch that drains all messages from a single partition.
///
/// `fetch_messages_arc` returns at most 10,000 messages per call, so when
/// the total message count exceeds that limit we must loop with advancing
/// offsets until an empty page is returned.
fn fetch_all(
    storage: &InMemoryStorage,
    topic: &str,
    partition: u32,
) -> Vec<(u64, Arc<fluxmq::protocol::Message>)> {
    let mut all = Vec::new();
    let mut offset: u64 = 0;
    loop {
        let page = storage
            .fetch_messages_arc(topic, partition, offset, MAX_FETCH_BYTES)
            .expect("fetch_all: fetch_messages_arc failed");
        if page.is_empty() {
            break;
        }
        // Advance past the last returned offset.
        offset = page.last().unwrap().0 + 1;
        all.extend(page);
    }
    all
}

// ---------------------------------------------------------------------------
// 1. Sustained produce throughput
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "long-running load test"]
async fn test_sustained_produce_throughput() {
    let storage = InMemoryStorage::new();
    let total_messages: usize = 100_000;
    let batch_size: usize = 100;
    let num_batches = total_messages / batch_size;

    let mut latencies_us: Vec<u128> = Vec::with_capacity(num_batches);

    let wall_start = Instant::now();

    for _ in 0..num_batches {
        let batch: Vec<_> = (0..batch_size).map(|_| make_message(256)).collect();

        let batch_start = Instant::now();
        storage
            .append_messages("throughput-topic", 0, batch)
            .expect("append_messages failed");
        latencies_us.push(batch_start.elapsed().as_micros());
    }

    let wall_elapsed = wall_start.elapsed();

    // Sort for percentile computation.
    latencies_us.sort_unstable();

    let p50 = percentile(&latencies_us, 50.0);
    let p95 = percentile(&latencies_us, 95.0);
    let p99 = percentile(&latencies_us, 99.0);
    let throughput = total_messages as f64 / wall_elapsed.as_secs_f64();

    println!("=== Sustained Produce Throughput ===");
    println!("Total messages : {total_messages}");
    println!("Batch size     : {batch_size}");
    println!("Wall time      : {wall_elapsed:?}");
    println!("Throughput     : {throughput:.0} msg/sec");
    println!("Latency p50    : {p50} us");
    println!("Latency p95    : {p95} us");
    println!("Latency p99    : {p99} us");

    assert!(
        throughput > 50_000.0,
        "throughput {throughput:.0} msg/sec is below the 50 000 msg/sec threshold"
    );
}

// ---------------------------------------------------------------------------
// 2. Concurrent produce + consume
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "long-running load test"]
async fn test_concurrent_produce_consume() {
    let storage = Arc::new(InMemoryStorage::new());
    let messages_per_producer: usize = 10_000;
    let num_tasks: usize = 4;
    let total_expected = num_tasks * messages_per_producer;

    let wall_start = Instant::now();

    // --- Producers -----------------------------------------------------------
    let mut producer_handles = Vec::with_capacity(num_tasks);
    for task_id in 0..num_tasks {
        let s = Arc::clone(&storage);
        let partition = task_id as u32;
        producer_handles.push(tokio::spawn(async move {
            for batch_start in (0..messages_per_producer).step_by(100) {
                let count = std::cmp::min(100, messages_per_producer - batch_start);
                let batch: Vec<_> = (0..count).map(|_| make_message(256)).collect();
                s.append_messages("concurrent-topic", partition, batch)
                    .expect("producer append failed");
            }
        }));
    }

    // Wait for all producers.
    for h in producer_handles {
        h.await.expect("producer task panicked");
    }

    let produce_elapsed = wall_start.elapsed();

    // --- Consumers -----------------------------------------------------------
    let consume_start = Instant::now();
    let mut consumer_handles = Vec::with_capacity(num_tasks);
    for task_id in 0..num_tasks {
        let s = Arc::clone(&storage);
        let partition = task_id as u32;
        consumer_handles.push(tokio::spawn(async move {
            let fetched = s
                .fetch_messages_arc("concurrent-topic", partition, 0, MAX_FETCH_BYTES)
                .expect("consumer fetch failed");
            fetched.len()
        }));
    }

    let mut total_consumed: usize = 0;
    for h in consumer_handles {
        total_consumed += h.await.expect("consumer task panicked");
    }

    let consume_elapsed = consume_start.elapsed();
    let total_elapsed = wall_start.elapsed();

    let produce_throughput = total_expected as f64 / produce_elapsed.as_secs_f64();
    let consume_throughput = total_consumed as f64 / consume_elapsed.as_secs_f64();

    println!("=== Concurrent Produce + Consume ===");
    println!("Producers      : {num_tasks} x {messages_per_producer} msgs");
    println!("Produce time   : {produce_elapsed:?}  ({produce_throughput:.0} msg/sec)");
    println!("Consume time   : {consume_elapsed:?}  ({consume_throughput:.0} msg/sec)");
    println!("Total time     : {total_elapsed:?}");
    println!("Produced       : {total_expected}");
    println!("Consumed       : {total_consumed}");

    assert_eq!(
        total_consumed, total_expected,
        "message loss detected: consumed {total_consumed} != produced {total_expected}"
    );
}

// ---------------------------------------------------------------------------
// 3. Multi-partition throughput
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "long-running load test"]
async fn test_multi_partition_throughput() {
    let storage = InMemoryStorage::new();
    let total_messages: usize = 50_000;
    let num_partitions: u32 = 16;
    let batch_size: usize = 100;

    let wall_start = Instant::now();

    for i in 0..total_messages {
        // Accumulate a batch then flush.
        // For simplicity, produce one batch per partition round.
        // We produce `batch_size` messages at a time in a round-robin fashion.
        if i % batch_size == 0 {
            let partition = ((i / batch_size) as u32) % num_partitions;
            let batch: Vec<_> = (0..batch_size).map(|_| make_message(256)).collect();
            storage
                .append_messages("multi-part-topic", partition, batch)
                .expect("append_messages failed");
        }
    }

    let wall_elapsed = wall_start.elapsed();
    let throughput = total_messages as f64 / wall_elapsed.as_secs_f64();

    println!("=== Multi-Partition Throughput ===");
    println!("Total messages : {total_messages}");
    println!("Partitions     : {num_partitions}");
    println!("Wall time      : {wall_elapsed:?}");
    println!("Throughput     : {throughput:.0} msg/sec");

    // Verify per-partition message counts.
    let batches_total = total_messages / batch_size; // 500
    let batches_per_partition = batches_total / num_partitions as usize;
    let remainder = batches_total % num_partitions as usize;

    for partition in 0..num_partitions {
        let expected_batches = batches_per_partition
            + if (partition as usize) < remainder {
                1
            } else {
                0
            };
        let expected_count = expected_batches * batch_size;

        let fetched = storage
            .fetch_messages_arc("multi-part-topic", partition, 0, MAX_FETCH_BYTES)
            .expect("fetch failed");

        assert_eq!(
            fetched.len(),
            expected_count,
            "partition {partition}: expected {expected_count} messages, got {}",
            fetched.len()
        );
    }

    println!("All {num_partitions} partitions verified.");
}

// ---------------------------------------------------------------------------
// 4. Message ordering under load
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "long-running load test"]
async fn test_message_ordering_under_load() {
    let storage = InMemoryStorage::new();
    let total_messages: usize = 50_000;
    let batch_size: usize = 500;

    // Produce in batches with sequential keys.
    for batch_start in (0..total_messages).step_by(batch_size) {
        let count = std::cmp::min(batch_size, total_messages - batch_start);
        let batch: Vec<_> = (0..count)
            .map(|i| {
                let seq = batch_start + i;
                fluxmq::protocol::Message {
                    key: Some(Bytes::from(format!("key-{seq:08}"))),
                    value: Bytes::from(vec![0x42u8; 128]),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    headers: HashMap::new(),
                }
            })
            .collect();
        storage
            .append_messages("ordering-topic", 0, batch)
            .expect("append failed");
    }

    // Fetch everything back (paginated to handle the 10k per-call cap).
    let fetched = fetch_all(&storage, "ordering-topic", 0);

    assert_eq!(
        fetched.len(),
        total_messages,
        "expected {total_messages} messages, got {}",
        fetched.len()
    );

    // Verify strict offset ordering: each offset must equal previous + 1.
    for window in fetched.windows(2) {
        let (prev_offset, _) = &window[0];
        let (curr_offset, _) = &window[1];
        assert_eq!(
            *curr_offset,
            *prev_offset + 1,
            "ordering violation: offset {curr_offset} does not follow {prev_offset}"
        );
    }

    // Verify first offset is 0.
    assert_eq!(fetched[0].0, 0, "first offset should be 0");

    println!("=== Message Ordering Under Load ===");
    println!("Messages verified : {total_messages}");
    println!("Offset range      : 0..{}", fetched.last().unwrap().0);
    println!("Strict ordering   : PASS");
}

// ---------------------------------------------------------------------------
// 5. Large message throughput
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "long-running load test"]
async fn test_large_message_throughput() {
    let storage = InMemoryStorage::new();
    let total_messages: usize = 10_000;
    let message_size: usize = 10 * 1024; // 10 KB
    let batch_size: usize = 100;

    let wall_start = Instant::now();

    for batch_start in (0..total_messages).step_by(batch_size) {
        let count = std::cmp::min(batch_size, total_messages - batch_start);
        let batch: Vec<_> = (0..count).map(|_| make_message(message_size)).collect();
        storage
            .append_messages("large-msg-topic", 0, batch)
            .expect("append failed");
    }

    let wall_elapsed = wall_start.elapsed();

    let total_bytes = total_messages * message_size;
    let throughput_msg = total_messages as f64 / wall_elapsed.as_secs_f64();
    let throughput_mb = (total_bytes as f64 / (1024.0 * 1024.0)) / wall_elapsed.as_secs_f64();

    // Fetch all messages back and verify sizes.
    let fetched = storage
        .fetch_messages_arc("large-msg-topic", 0, 0, MAX_FETCH_BYTES)
        .expect("fetch failed");

    assert_eq!(
        fetched.len(),
        total_messages,
        "expected {total_messages} messages, got {}",
        fetched.len()
    );

    for (offset, msg) in &fetched {
        assert_eq!(
            msg.value.len(),
            message_size,
            "message at offset {offset} has wrong size: {} != {message_size}",
            msg.value.len()
        );
    }

    println!("=== Large Message Throughput ===");
    println!("Total messages : {total_messages}");
    println!("Message size   : {} KB", message_size / 1024);
    println!("Total data     : {:.1} MB", total_bytes as f64 / (1024.0 * 1024.0));
    println!("Wall time      : {wall_elapsed:?}");
    println!("Throughput     : {throughput_msg:.0} msg/sec");
    println!("Throughput     : {throughput_mb:.1} MB/sec");
    println!("All message sizes verified.");
}
