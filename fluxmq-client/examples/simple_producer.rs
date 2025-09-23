//! Simple producer example using FluxMQ Rust SDK

use fluxmq_client::*;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🚀 FluxMQ Rust SDK - Simple Producer Example");
    println!("============================================");

    // Create producer with builder pattern
    let producer = ProducerBuilder::new()
        .brokers(vec!["localhost:9092"])
        .acks(1) // Wait for leader acknowledgment
        .max_message_size(1024 * 1024) // 1MB max message size
        .delivery_timeout(Duration::from_secs(30))
        .build()
        .await?;

    println!("✅ Connected to FluxMQ broker");

    // Example 1: Simple record
    let record = ProduceRecord::new("rust-sdk-topic", "Hello from Rust SDK!");
    let metadata = producer.send(record).await?;
    println!(
        "✅ Sent simple message - partition: {}, offset: {}",
        metadata.partition, metadata.offset
    );

    // Example 2: Record with key
    let record = ProduceRecord::with_key("rust-sdk-topic", "user-123", "User login event");
    let metadata = producer.send(record).await?;
    println!(
        "✅ Sent keyed message - partition: {}, offset: {}",
        metadata.partition, metadata.offset
    );

    // Example 3: Record with builder pattern
    let record = ProduceRecord::builder()
        .topic("rust-sdk-topic")
        .key("order-456")
        .value("Order created: $99.99")
        .header("content-type", "application/json")
        .header("source", "web-app")
        .timestamp(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        )
        .build();

    let metadata = producer.send(record).await?;
    println!(
        "✅ Sent record with headers - partition: {}, offset: {}",
        metadata.partition, metadata.offset
    );

    // Example 4: Batch sending
    let records = vec![
        ProduceRecord::with_key("rust-sdk-topic", "batch-1", "First message in batch"),
        ProduceRecord::with_key("rust-sdk-topic", "batch-2", "Second message in batch"),
        ProduceRecord::with_key("rust-sdk-topic", "batch-3", "Third message in batch"),
    ];

    let batch_metadata = producer.send_batch(records).await?;
    println!("✅ Sent batch of {} messages:", batch_metadata.len());
    for (i, metadata) in batch_metadata.iter().enumerate() {
        println!(
            "   Message {}: partition={}, offset={}",
            i + 1,
            metadata.partition,
            metadata.offset
        );
    }

    // Example 5: High-throughput sending
    println!("\n🚀 High-throughput test - sending 1000 messages...");
    let start = std::time::Instant::now();

    for i in 0..1000 {
        let key = format!("key-{}", i);
        let value = format!("High-throughput message #{}", i);
        let record = ProduceRecord::with_key("rust-sdk-topic", key, value);

        // Send without waiting for each individual response
        tokio::spawn({
            let producer = producer.clone();
            async move {
                if let Err(e) = producer.send(record).await {
                    eprintln!("Error sending message {}: {}", i, e);
                }
            }
        });
    }

    // Wait a bit for messages to be sent
    sleep(Duration::from_secs(2)).await;

    let elapsed = start.elapsed();
    println!(
        "✅ High-throughput test completed in {:?} (~{:.0} msg/sec)",
        elapsed,
        1000.0 / elapsed.as_secs_f64()
    );

    // Flush any remaining messages
    producer.flush().await?;
    println!("✅ All messages flushed");

    // Close producer
    producer.close().await?;
    println!("✅ Producer closed");

    println!("\n🎉 Producer example completed successfully!");
    println!("💡 Try running the consumer example to read these messages:");
    println!("   cargo run --example simple_consumer");

    Ok(())
}
