//! Simple consumer example using FluxMQ Rust SDK

use fluxmq_client::*;
use futures::StreamExt;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("📖 FluxMQ Rust SDK - Simple Consumer Example");
    println!("============================================");

    // Create consumer with builder pattern
    let consumer = ConsumerBuilder::new()
        .brokers(vec!["localhost:9092"])
        .group_id("rust-sdk-consumer-group")
        .topics(vec!["rust-sdk-topic"])
        .session_timeout(Duration::from_secs(30))
        .max_poll_records(100)
        .build()
        .await?;

    println!("✅ Connected to FluxMQ broker");
    println!("📖 Consuming from topic: rust-sdk-topic");
    println!("👥 Consumer group: rust-sdk-consumer-group");

    // Example 1: Poll-based consumption
    println!("\n📖 Example 1: Poll-based consumption (10 polls)");
    for i in 0..10 {
        let records = consumer.poll().await?;
        if records.is_empty() {
            println!("   Poll {}: No records available", i + 1);
            tokio::time::sleep(Duration::from_millis(500)).await;
        } else {
            println!("   Poll {}: Received {} records", i + 1, records.len());
            for (j, record) in records.iter().enumerate() {
                let key_str = record
                    .key
                    .as_ref()
                    .map(|k| String::from_utf8_lossy(k).to_string())
                    .unwrap_or_else(|| "<no key>".to_string());

                let value_str = String::from_utf8_lossy(&record.value);

                println!(
                    "      Record {}: partition={}, offset={}, key='{}', value='{}'",
                    j + 1,
                    record.partition,
                    record.offset,
                    key_str,
                    value_str
                );
            }

            // Commit offsets after processing
            consumer.commit_sync().await?;
            println!("   ✅ Committed offsets");
        }
    }

    // Example 2: Stream-based consumption
    println!("\n📖 Example 2: Stream-based consumption (30 seconds)");
    let mut stream = consumer.stream();
    let mut message_count = 0;
    let start_time = std::time::Instant::now();

    // Consume for 30 seconds
    while start_time.elapsed() < Duration::from_secs(30) {
        tokio::select! {
            record = stream.next() => {
                match record {
                    Some(Ok(record)) => {
                        message_count += 1;

                        let key_str = record
                            .key
                            .as_ref()
                            .map(|k| String::from_utf8_lossy(k).to_string())
                            .unwrap_or_else(|| "<no key>".to_string());

                        let value_str = String::from_utf8_lossy(&record.value);

                        println!(
                            "   📨 Message {}: {}:{} offset={} key='{}' value='{}'",
                            message_count,
                            record.topic,
                            record.partition,
                            record.offset,
                            key_str,
                            value_str.chars().take(50).collect::<String>()
                        );

                        // Commit periodically
                        if message_count % 10 == 0 {
                            consumer.commit_sync().await?;
                            println!("   ✅ Committed offsets (batch of 10)");
                        }
                    }
                    Some(Err(e)) => {
                        eprintln!("   ❌ Error receiving message: {}", e);
                        break;
                    }
                    None => {
                        println!("   ℹ️ Stream ended");
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                if message_count == 0 {
                    print!("   ⏳ Waiting for messages...\r");
                    std::io::Write::flush(&mut std::io::stdout()).unwrap();
                }
            }
        }
    }

    println!("\n✅ Stream consumption completed");
    println!("📊 Total messages consumed: {}", message_count);

    // Final commit
    if message_count > 0 {
        consumer.commit_sync().await?;
        println!("✅ Final commit completed");
    }

    // Close consumer
    consumer.close().await?;
    println!("✅ Consumer closed");

    println!("\n🎉 Consumer example completed successfully!");
    if message_count == 0 {
        println!("💡 No messages were consumed. Try running the producer example first:");
        println!("   cargo run --example simple_producer");
    }

    Ok(())
}
