//! Chaos engineering tests for FluxMQ.
//!
//! Tests system behavior under adverse conditions: resource pressure,
//! concurrent stress, crash recovery, and boundary conditions.
//!
//! Run with: `cargo test --release --test chaos_tests -- --nocapture --ignored`

use bytes::Bytes;
use fluxmq::storage::InMemoryStorage;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fluxmq::protocol::Message;

/// Create a test message with a payload of the given size.
fn make_message(size: usize) -> Message {
    let payload = vec![0xABu8; size];
    Message {
        key: None,
        value: Bytes::from(payload),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        headers: HashMap::new(),
    }
}

/// Simulate 1000 concurrent "connections" (tokio tasks), each performing
/// 100 append operations to random partitions (0..32). Verifies that the
/// total message count equals 100,000 with no data loss across all partitions.
#[tokio::test]
#[ignore = "chaos: high concurrency storm"]
async fn test_connection_storm_storage() {
    let storage = Arc::new(InMemoryStorage::new());
    let num_connections = 1000usize;
    let ops_per_connection = 100usize;
    let num_partitions = 32u32;
    let topic = "storm-topic";

    let start = Instant::now();

    let mut handles = Vec::with_capacity(num_connections);
    for conn_id in 0..num_connections {
        let storage = Arc::clone(&storage);
        let handle = tokio::spawn(async move {
            // Deterministic partition selection based on connection id and op index
            // to avoid pulling in a random number generator dependency.
            for op in 0..ops_per_connection {
                let partition = ((conn_id * 7 + op * 13) % num_partitions as usize) as u32;
                let msg = make_message(64);
                storage
                    .append_messages(topic, partition, vec![msg])
                    .expect("append must succeed");
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.expect("task must not panic");
    }

    let elapsed = start.elapsed();

    // Verify total message count across all partitions
    let mut total: usize = 0;
    for partition in 0..num_partitions {
        let msgs = storage
            .fetch_messages_arc(topic, partition, 0, u32::MAX)
            .expect("fetch must succeed");
        total += msgs.len();
    }

    let expected = num_connections * ops_per_connection;
    assert_eq!(
        total, expected,
        "expected {} messages across all partitions, got {}",
        expected, total
    );

    println!(
        "[connection_storm] {} messages across {} partitions in {:?}",
        total, num_partitions, elapsed
    );
}

/// Continuously append messages (1000 at a time) up to 1,000,000 total.
/// After every 100,000 messages, report estimated memory usage.
/// Verifies that InMemoryStorage holds all messages (documents unbounded
/// growth as a known limitation).
#[tokio::test]
#[ignore = "chaos: memory growth bound documentation"]
async fn test_memory_growth_bound() {
    let storage = InMemoryStorage::new();
    let topic = "memory-topic";
    let partition = 0u32;
    let total_messages = 1_000_000usize;
    let batch_size = 1000usize;
    let report_interval = 100_000usize;
    let msg_payload_size = 128usize;

    let start = Instant::now();
    let mut appended = 0usize;

    while appended < total_messages {
        let batch: Vec<Message> = (0..batch_size).map(|_| make_message(msg_payload_size)).collect();
        storage
            .append_messages(topic, partition, batch)
            .expect("append must succeed");
        appended += batch_size;

        if appended % report_interval == 0 {
            // Estimate: each message carries payload + key + headers + Arc overhead + offset
            // Rough per-message overhead: payload_size + 8 (offset) + 16 (Arc) + ~64 (Message struct)
            let estimated_bytes = appended * (msg_payload_size + 88);
            println!(
                "[memory_growth] {} messages appended, estimated ~{} MB in storage",
                appended,
                estimated_bytes / (1024 * 1024)
            );
        }
    }

    // Verify all messages are retrievable (fetch in chunks because max_bytes caps output)
    let mut fetched_total = 0usize;
    let mut offset = 0u64;
    loop {
        let batch = storage
            .fetch_messages_arc(topic, partition, offset, 10 * 1024 * 1024)
            .expect("fetch must succeed");
        if batch.is_empty() {
            break;
        }
        fetched_total += batch.len();
        offset = batch.last().unwrap().0 + 1;
    }

    assert_eq!(
        fetched_total, total_messages,
        "expected all {} messages to be retrievable, got {}",
        total_messages, fetched_total
    );

    let elapsed = start.elapsed();
    let final_estimated_mb = (total_messages * (msg_payload_size + 88)) / (1024 * 1024);
    println!(
        "[memory_growth] completed: {} messages, ~{} MB estimated, {:?} elapsed",
        total_messages, final_estimated_mb, elapsed
    );
}

/// Start a producer that appends 100,000 messages in batches of 1000.
/// Start a "slow consumer" that fetches 100 messages then sleeps 10ms.
/// Verify the producer completes within reasonable time (< 10 seconds)
/// and all produced messages are accessible despite the slow consumer.
#[tokio::test]
#[ignore = "chaos: slow consumer must not block producers"]
async fn test_slow_consumer_does_not_block_producers() {
    let storage = Arc::new(InMemoryStorage::new());
    let topic = "slow-consumer-topic";
    let partition = 0u32;
    let total_messages = 100_000usize;
    let produce_batch_size = 1000usize;
    let consumer_fetch_size = 100usize;

    let stop_consumer = Arc::new(AtomicBool::new(false));

    // Slow consumer task
    let consumer_storage = Arc::clone(&storage);
    let consumer_stop = Arc::clone(&stop_consumer);
    let consumer_handle = tokio::spawn(async move {
        let mut offset = 0u64;
        let mut consumed = 0u64;
        while !consumer_stop.load(Ordering::Relaxed) {
            let msgs = consumer_storage
                .fetch_messages_arc(topic, partition, offset, (consumer_fetch_size * 256) as u32)
                .expect("fetch must succeed");
            if !msgs.is_empty() {
                consumed += msgs.len() as u64;
                offset = msgs.last().unwrap().0 + 1;
            }
            // Simulate slow processing
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        consumed
    });

    // Producer task
    let producer_storage = Arc::clone(&storage);
    let producer_start = Instant::now();
    let producer_handle = tokio::spawn(async move {
        let mut produced = 0usize;
        while produced < total_messages {
            let batch: Vec<Message> = (0..produce_batch_size).map(|_| make_message(64)).collect();
            producer_storage
                .append_messages(topic, partition, batch)
                .expect("append must succeed");
            produced += produce_batch_size;
        }
        produced
    });

    let produced = producer_handle.await.expect("producer must not panic");
    let producer_elapsed = producer_start.elapsed();

    // Stop the slow consumer
    stop_consumer.store(true, Ordering::Relaxed);
    let consumed = consumer_handle.await.expect("consumer must not panic");

    // Producer must complete within 10 seconds for in-memory storage
    assert!(
        producer_elapsed < Duration::from_secs(10),
        "producer took too long: {:?} (limit: 10s). Slow consumer may be blocking.",
        producer_elapsed
    );

    assert_eq!(produced, total_messages);

    // Verify all messages are accessible regardless of consumer position
    let mut total_fetched = 0usize;
    let mut offset = 0u64;
    loop {
        let batch = storage
            .fetch_messages_arc(topic, partition, offset, 10 * 1024 * 1024)
            .expect("fetch must succeed");
        if batch.is_empty() {
            break;
        }
        total_fetched += batch.len();
        offset = batch.last().unwrap().0 + 1;
    }

    assert_eq!(
        total_fetched, total_messages,
        "all produced messages must be accessible"
    );

    println!(
        "[slow_consumer] producer: {} msgs in {:?}, consumer saw {} msgs before stop",
        produced, producer_elapsed, consumed
    );
}

/// Use AtomicU64 to track the highest produced offset.
/// 2 writer tasks continuously appending, 4 reader tasks continuously
/// fetching from offset 0. Run for 2 seconds. Verify: fetched messages
/// are always in offset order with no gaps within a single fetch result.
#[tokio::test]
#[ignore = "chaos: concurrent read/write consistency"]
async fn test_concurrent_read_write_consistency() {
    let storage = Arc::new(InMemoryStorage::new());
    let topic = "consistency-topic";
    let partition = 0u32;
    let run_duration = Duration::from_secs(2);
    let produced_count = Arc::new(AtomicU64::new(0));

    let stop = Arc::new(AtomicBool::new(false));

    // 2 writer tasks
    let mut writer_handles = Vec::new();
    for writer_id in 0..2u32 {
        let storage = Arc::clone(&storage);
        let produced = Arc::clone(&produced_count);
        let stop = Arc::clone(&stop);
        let handle = tokio::spawn(async move {
            let mut local_count = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let msg = make_message(32);
                let _ = storage.append_messages(topic, partition, vec![msg]);
                local_count += 1;
                produced.fetch_add(1, Ordering::Relaxed);
                // Yield occasionally to allow readers to run
                if local_count % 100 == 0 {
                    tokio::task::yield_now().await;
                }
            }
            (writer_id, local_count)
        });
        writer_handles.push(handle);
    }

    // 4 reader tasks that verify ordering invariants
    let consistency_violations = Arc::new(AtomicU64::new(0));
    let total_reads = Arc::new(AtomicU64::new(0));

    let mut reader_handles = Vec::new();
    for reader_id in 0..4u32 {
        let storage = Arc::clone(&storage);
        let violations = Arc::clone(&consistency_violations);
        let reads = Arc::clone(&total_reads);
        let stop = Arc::clone(&stop);
        let handle = tokio::spawn(async move {
            let mut local_reads = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let msgs = storage
                    .fetch_messages_arc(topic, partition, 0, 1024 * 1024)
                    .unwrap_or_default();

                if msgs.len() > 1 {
                    // Verify offsets are strictly sequential within this fetch
                    for window in msgs.windows(2) {
                        let (prev_offset, _) = &window[0];
                        let (curr_offset, _) = &window[1];
                        if *curr_offset != *prev_offset + 1 {
                            violations.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                local_reads += 1;
                reads.fetch_add(1, Ordering::Relaxed);

                if local_reads % 50 == 0 {
                    tokio::task::yield_now().await;
                }
            }
            (reader_id, local_reads)
        });
        reader_handles.push(handle);
    }

    // Let the test run for the specified duration
    tokio::time::sleep(run_duration).await;
    stop.store(true, Ordering::Relaxed);

    // Collect results
    let mut writer_results = Vec::new();
    for h in writer_handles {
        writer_results.push(h.await.expect("writer must not panic"));
    }

    let mut reader_results = Vec::new();
    for h in reader_handles {
        reader_results.push(h.await.expect("reader must not panic"));
    }

    let total_produced = produced_count.load(Ordering::Relaxed);
    let violations = consistency_violations.load(Ordering::Relaxed);
    let reads = total_reads.load(Ordering::Relaxed);

    assert_eq!(
        violations, 0,
        "detected {} offset ordering violations in {} reads across {} produced messages",
        violations, reads, total_produced
    );

    println!(
        "[read_write_consistency] {} messages produced, {} reads performed, 0 violations",
        total_produced, reads
    );
    for (id, count) in &writer_results {
        println!("  writer {}: {} messages", id, count);
    }
    for (id, count) in &reader_results {
        println!("  reader {}: {} fetches", id, count);
    }
}

/// Verify graceful behavior for edge cases on empty/non-existent topics:
/// - Fetch from non-existent topic returns empty
/// - Fetch from empty partition returns empty
/// - Append empty message list succeeds (returns Ok(0))
/// - No panics or crashes throughout
#[tokio::test]
#[ignore = "chaos: empty topic boundary conditions"]
async fn test_empty_topic_operations() {
    let storage = InMemoryStorage::new();

    // Fetch from non-existent topic: must return empty, not panic
    let result = storage.fetch_messages_arc("nonexistent-topic", 0, 0, u32::MAX);
    assert!(result.is_ok(), "fetch from nonexistent topic must not error");
    assert!(
        result.unwrap().is_empty(),
        "fetch from nonexistent topic must return empty"
    );

    // Fetch from non-existent partition on existing topic
    storage
        .append_messages("existing-topic", 0, vec![make_message(16)])
        .expect("setup append must succeed");

    let result = storage.fetch_messages_arc("existing-topic", 999, 0, u32::MAX);
    assert!(
        result.is_ok(),
        "fetch from nonexistent partition must not error"
    );
    assert!(
        result.unwrap().is_empty(),
        "fetch from nonexistent partition must return empty"
    );

    // Fetch from existing but empty offset range
    let result = storage.fetch_messages_arc("existing-topic", 0, 9999, u32::MAX);
    assert!(
        result.is_ok(),
        "fetch beyond last offset must not error"
    );
    assert!(
        result.unwrap().is_empty(),
        "fetch beyond last offset must return empty"
    );

    // Append empty message list: InMemoryStorage returns Ok(0) for empty batches
    let result = storage.append_messages("existing-topic", 0, vec![]);
    assert!(result.is_ok(), "appending empty batch must not error");
    assert_eq!(
        result.unwrap(),
        0,
        "appending empty batch should return offset 0"
    );

    // Verify the original message is still intact
    let msgs = storage
        .fetch_messages_arc("existing-topic", 0, 0, u32::MAX)
        .expect("fetch must succeed");
    assert_eq!(msgs.len(), 1, "original message must still be present");

    println!("[empty_topic_ops] all boundary conditions handled gracefully");
}

/// Append a single batch of 100,000 messages at once.
/// Verify all messages are retrievable and offsets are sequential 0..99999.
#[tokio::test]
#[ignore = "chaos: large batch append"]
async fn test_large_batch_append() {
    let storage = InMemoryStorage::new();
    let topic = "large-batch-topic";
    let partition = 0u32;
    let batch_size = 100_000usize;

    let batch: Vec<Message> = (0..batch_size).map(|_| make_message(64)).collect();

    let start = Instant::now();
    let base_offset = storage
        .append_messages(topic, partition, batch)
        .expect("large batch append must succeed");
    let append_elapsed = start.elapsed();

    assert_eq!(base_offset, 0, "first batch must start at offset 0");

    // Verify all messages are retrievable with sequential offsets
    let fetch_start = Instant::now();
    let mut all_offsets = Vec::with_capacity(batch_size);
    let mut offset = 0u64;
    loop {
        let msgs = storage
            .fetch_messages_arc(topic, partition, offset, 10 * 1024 * 1024)
            .expect("fetch must succeed");
        if msgs.is_empty() {
            break;
        }
        for (msg_offset, _) in &msgs {
            all_offsets.push(*msg_offset);
        }
        offset = msgs.last().unwrap().0 + 1;
    }
    let fetch_elapsed = fetch_start.elapsed();

    assert_eq!(
        all_offsets.len(),
        batch_size,
        "expected {} messages, got {}",
        batch_size,
        all_offsets.len()
    );

    // Verify offsets are sequential 0..99999
    for (idx, &off) in all_offsets.iter().enumerate() {
        assert_eq!(
            off, idx as u64,
            "offset mismatch at index {}: expected {}, got {}",
            idx, idx, off
        );
    }

    println!(
        "[large_batch] {} messages: append {:?}, full fetch {:?}",
        batch_size, append_elapsed, fetch_elapsed
    );
}
