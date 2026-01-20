use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use std::time::{Duration, Instant};
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    info!("=== FluxMQ Simple Consumer Test ===");

    let target_messages = std::env::args()
        .nth(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);

    let topic = "test-topic";

    // Create consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("group.id", "c2c-test-group")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "30000")
        .create()?;

    consumer.subscribe(&[topic])?;

    info!(
        "Consumer created, waiting for {} messages from topic '{}'",
        target_messages, topic
    );

    let start = Instant::now();
    let mut message_count = 0;
    let mut total_bytes = 0;
    let mut latencies = Vec::new();

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let _recv_time = Instant::now();
                message_count += 1;

                if let Some(payload) = msg.payload() {
                    total_bytes += payload.len();

                    // Try to extract timestamp from message to calculate E2E latency
                    if let Ok(json_str) = std::str::from_utf8(payload) {
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) {
                            if let Some(send_ts) = json["timestamp"].as_u64() {
                                let now_ts = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis()
                                    as u64;
                                if now_ts > send_ts {
                                    latencies.push(Duration::from_millis(now_ts - send_ts));
                                }
                            }
                        }
                    }
                }

                if message_count % 100 == 0 {
                    let elapsed = start.elapsed();
                    let rate = message_count as f64 / elapsed.as_secs_f64();
                    info!("Received {} messages ({:.2} msg/sec)", message_count, rate);
                }

                if message_count >= target_messages {
                    break;
                }
            }
            Err(e) => {
                error!("Error receiving message: {:?}", e);
            }
        }
    }

    let total_time = start.elapsed();

    // Calculate statistics
    info!("=== Consumer Test Results ===");
    info!("Total messages: {}", message_count);
    info!(
        "Total bytes: {} ({:.2} MB)",
        total_bytes,
        total_bytes as f64 / 1024.0 / 1024.0
    );
    info!("Total time: {:.2}s", total_time.as_secs_f64());
    info!(
        "Throughput: {:.2} msg/sec",
        message_count as f64 / total_time.as_secs_f64()
    );
    info!(
        "Throughput: {:.2} MB/sec",
        (total_bytes as f64 / 1024.0 / 1024.0) / total_time.as_secs_f64()
    );

    if !latencies.is_empty() {
        latencies.sort();
        let avg = latencies.iter().sum::<Duration>() / latencies.len() as u32;
        let p50 = latencies[latencies.len() / 2];
        let p95 = latencies[(latencies.len() as f64 * 0.95) as usize];
        let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];

        info!(
            "E2E Latency - Avg: {:.2}ms, P50: {:.2}ms, P95: {:.2}ms, P99: {:.2}ms",
            avg.as_millis(),
            p50.as_millis(),
            p95.as_millis(),
            p99.as_millis()
        );
    }

    Ok(())
}
