use bytes::Bytes;
use fluxmq::{
    broker::MessageHandler,
    protocol::{FetchRequest, Message, ProduceRequest, Request},
    Result,
};
use std::time::Instant;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<()> {
    let handler = std::sync::Arc::new(MessageHandler::new()?);

    println!("🚀 FluxMQ Performance Benchmark");
    println!("================================");

    // Benchmark 1: Single-threaded throughput
    println!("\n📈 Benchmark 1: Single-threaded Message Throughput");
    let single_thread_result = benchmark_single_thread(&handler).await?;

    // Benchmark 2: Multi-threaded concurrent producers
    println!("\n⚡ Benchmark 2: Multi-threaded Concurrent Producers");
    let concurrent_result = benchmark_concurrent_producers(&handler).await?;

    // Benchmark 3: Partition distribution analysis
    println!("\n📊 Benchmark 3: Partition Distribution Analysis");
    analyze_partition_distribution(&handler).await?;

    // Benchmark 4: Fetch performance
    println!("\n📖 Benchmark 4: Message Fetch Performance");
    let fetch_result = benchmark_fetch_performance(&handler).await?;

    // Summary
    println!("\n🏆 Performance Summary");
    println!("=====================");
    println!("Single-thread: {:.0} msg/sec", single_thread_result);
    println!("Multi-thread:  {:.0} msg/sec", concurrent_result);
    println!("Fetch rate:    {:.0} msg/sec", fetch_result);
    println!("\n✅ All benchmarks completed successfully!");

    Ok(())
}

async fn benchmark_single_thread(handler: &std::sync::Arc<MessageHandler>) -> Result<f64> {
    let num_messages = 10_000;
    let topic = "benchmark-single";

    let start = Instant::now();

    for i in 0..num_messages {
        let message = Message {
            key: Some(Bytes::from(format!("key-{}", i % 100))), // 100 different keys
            value: Bytes::from(format!("Single thread benchmark message {}", i)),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            headers: std::collections::HashMap::new(),
        };

        let produce_req = ProduceRequest {
            correlation_id: i as i32,
            topic: topic.to_string(),
            partition: u32::MAX, // Auto-assign
            messages: vec![message],
            acks: 1,
            timeout_ms: 5000,
        };

        handler
            .handle_request(Request::Produce(produce_req))
            .await?;
    }

    let duration = start.elapsed();
    let throughput = num_messages as f64 / duration.as_secs_f64();

    println!("  Messages: {}", num_messages);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Throughput: {:.0} messages/second", throughput);

    Ok(throughput)
}

async fn benchmark_concurrent_producers(handler: &std::sync::Arc<MessageHandler>) -> Result<f64> {
    let num_producers = 10;
    let messages_per_producer = 1_000;
    let total_messages = num_producers * messages_per_producer;

    let mut tasks = JoinSet::new();
    let start = Instant::now();

    for producer_id in 0..num_producers {
        let handler = std::sync::Arc::clone(handler);
        tasks.spawn(async move {
            for msg_id in 0..messages_per_producer {
                let message = Message {
                    key: Some(Bytes::from(format!(
                        "producer-{}-key-{}",
                        producer_id,
                        msg_id % 50
                    ))),
                    value: Bytes::from(format!(
                        "Concurrent producer {} message {}",
                        producer_id, msg_id
                    )),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    headers: std::collections::HashMap::new(),
                };

                let produce_req = ProduceRequest {
                    correlation_id: (producer_id * 1000 + msg_id) as i32,
                    topic: "benchmark-concurrent".to_string(),
                    partition: u32::MAX, // Auto-assign
                    messages: vec![message],
                    acks: 1,
                    timeout_ms: 5000,
                };

                handler
                    .handle_request(Request::Produce(produce_req))
                    .await
                    .expect("Failed to produce message");
            }
            producer_id
        });
    }

    // Wait for all producers to complete
    while let Some(_) = tasks.join_next().await {
        // Producers completed
    }

    let duration = start.elapsed();
    let throughput = total_messages as f64 / duration.as_secs_f64();

    println!("  Producers: {}", num_producers);
    println!("  Messages per producer: {}", messages_per_producer);
    println!("  Total messages: {}", total_messages);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Throughput: {:.0} messages/second", throughput);

    Ok(throughput)
}

async fn analyze_partition_distribution(handler: &std::sync::Arc<MessageHandler>) -> Result<()> {
    let topic = "distribution-test";
    let num_messages = 3_000;

    // Send messages with different key patterns
    println!("  Analyzing key-based distribution...");

    for i in 0..num_messages {
        let key_pattern = match i % 5 {
            0 => format!("user-{}", i % 10),    // 10 different user keys
            1 => format!("session-{}", i % 20), // 20 different session keys
            2 => format!("order-{}", i % 15),   // 15 different order keys
            3 => format!("product-{}", i % 8),  // 8 different product keys
            _ => format!("event-{}", i % 12),   // 12 different event keys
        };

        let message = Message {
            key: Some(Bytes::from(key_pattern.clone())),
            value: Bytes::from(format!(
                "Distribution test message {} with key {}",
                i, key_pattern
            )),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            headers: std::collections::HashMap::new(),
        };

        let produce_req = ProduceRequest {
            correlation_id: i as i32,
            topic: topic.to_string(),
            partition: u32::MAX, // Auto-assign based on key
            messages: vec![message],
            acks: 1,
            timeout_ms: 5000,
        };

        handler
            .handle_request(Request::Produce(produce_req))
            .await?;
    }

    // Fetch from each partition to see distribution
    println!("  Partition message distribution:");
    for partition_id in 0..3 {
        let fetch_req = FetchRequest {
            correlation_id: 9000 + partition_id as i32,
            topic: topic.to_string(),
            partition: partition_id,
            offset: 0,
            max_bytes: 10 * 1024 * 1024, // 10MB
            timeout_ms: 1000,
        };

        if let Ok(response) = handler.handle_request(Request::Fetch(fetch_req)).await {
            if let fluxmq::protocol::Response::Fetch(fetch_resp) = response {
                let percentage = (fetch_resp.messages.len() as f64 / num_messages as f64) * 100.0;
                println!(
                    "    Partition {}: {} messages ({:.1}%)",
                    partition_id,
                    fetch_resp.messages.len(),
                    percentage
                );
            }
        }
    }

    Ok(())
}

async fn benchmark_fetch_performance(handler: &std::sync::Arc<MessageHandler>) -> Result<f64> {
    let topic = "fetch-benchmark";
    let num_messages = 5_000;

    // First, populate the topic with messages
    println!("  Populating topic for fetch benchmark...");
    for i in 0..num_messages {
        let message = Message {
            key: Some(Bytes::from(format!("fetch-key-{}", i % 100))),
            value: Bytes::from(format!("Fetch benchmark message {}", i)),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            headers: std::collections::HashMap::new(),
        };

        let produce_req = ProduceRequest {
            correlation_id: i as i32,
            topic: topic.to_string(),
            partition: u32::MAX, // Auto-assign
            messages: vec![message],
            acks: 1,
            timeout_ms: 5000,
        };

        handler
            .handle_request(Request::Produce(produce_req))
            .await?;
    }

    // Now benchmark fetch performance
    println!("  Benchmarking fetch operations...");
    let start = Instant::now();
    let mut total_fetched = 0;

    // Fetch from all partitions
    for partition_id in 0..3 {
        let mut offset = 0;
        loop {
            let fetch_req = FetchRequest {
                correlation_id: 10000 + partition_id as i32,
                topic: topic.to_string(),
                partition: partition_id,
                offset,
                max_bytes: 1024 * 1024, // 1MB per fetch
                timeout_ms: 1000,
            };

            if let Ok(response) = handler.handle_request(Request::Fetch(fetch_req)).await {
                if let fluxmq::protocol::Response::Fetch(fetch_resp) = response {
                    if fetch_resp.messages.is_empty() {
                        break; // No more messages
                    }
                    total_fetched += fetch_resp.messages.len();
                    offset += fetch_resp.messages.len() as u64;
                }
            } else {
                break;
            }
        }
    }

    let duration = start.elapsed();
    let fetch_throughput = total_fetched as f64 / duration.as_secs_f64();

    println!("  Total fetched: {} messages", total_fetched);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!(
        "  Fetch throughput: {:.0} messages/second",
        fetch_throughput
    );

    Ok(fetch_throughput)
}
