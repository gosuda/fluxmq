use anyhow::Result;
use futures::future::join_all;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::{Duration, Instant};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    info!("=== FluxMQ Simple Producer Test ===");

    let num_messages = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    let topic = "test-topic";

    // Create producer with high-performance optimizations
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("message.timeout.ms", "5000")
        // 🚀 PERFORMANCE: Batching and pipelining optimizations
        .set("batch.size", "131072") // 128KB batch
        .set("linger.ms", "1") // 1ms linger for batching
        .set("queue.buffering.max.messages", "1000000") // Queue up to 1M messages
        .set("queue.buffering.max.kbytes", "65536") // 64MB buffer
        .set("compression.type", "none") // No compression
        .set("acks", "1") // Leader ack only
        .create()?;

    info!(
        "Producer created, will send {} messages to topic '{}'",
        num_messages, topic
    );

    let start = Instant::now();
    let mut success_count = 0;
    let mut error_count = 0;

    // 🚀 PERFORMANCE: Batch sending strategy
    // Prepare batch of messages, send them, then await together
    // This leverages rdkafka's internal batching (linger.ms=1ms, batch.size=128KB)
    const BATCH_SIZE: usize = 1000;

    for batch_start in (0..num_messages).step_by(BATCH_SIZE) {
        let batch_end = (batch_start + BATCH_SIZE).min(num_messages);
        let batch_size = batch_end - batch_start;

        // Create owned message data for this batch
        let mut messages: Vec<(String, String)> = Vec::with_capacity(batch_size);
        for i in batch_start..batch_end {
            let key = format!("key-{}", i);
            let value = format!(
                "{{\"id\":{},\"message\":\"Test message {}\",\"timestamp\":{}}}",
                i,
                i,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            );
            messages.push((key, value));
        }

        // Send all messages in this batch and collect futures
        let mut futures = Vec::with_capacity(batch_size);
        for (key, value) in &messages {
            let record = FutureRecord::to(topic).key(key).payload(value);
            futures.push(producer.send(record, Duration::from_secs(30)));
        }

        // Await all sends in this batch
        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(_) => success_count += 1,
                Err(_) => error_count += 1,
            }
        }

        if batch_end % 10000 == 0 || batch_end == num_messages {
            info!("Sent {} messages...", batch_end);
        }
    }

    let total_time = start.elapsed();

    // Calculate statistics
    info!("=== Producer Test Results ===");
    info!("Total messages: {}", num_messages);
    info!("Success: {}", success_count);
    info!("Errors: {}", error_count);
    info!("Total time: {:.2}s", total_time.as_secs_f64());
    info!(
        "Throughput: {:.2} msg/sec",
        success_count as f64 / total_time.as_secs_f64()
    );

    Ok(())
}
