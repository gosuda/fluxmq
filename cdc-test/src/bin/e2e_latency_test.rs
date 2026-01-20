//! End-to-End CDC Latency Test
//!
//! Measures the complete CDC pipeline latency:
//! PostgreSQL INSERT -> Debezium -> FluxMQ -> Rust Consumer
//!
//! The test embeds a timestamp in the inserted record and measures
//! when that record arrives at the consumer.

use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_postgres::NoTls;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
struct LatencyRecord {
    insert_time_ms: u64,
    receive_time_ms: u64,
}

impl LatencyRecord {
    fn latency_ms(&self) -> u64 {
        self.receive_time_ms.saturating_sub(self.insert_time_ms)
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    info!("=== FluxMQ CDC End-to-End Latency Test ===");
    info!("Pipeline: PostgreSQL -> Debezium -> FluxMQ -> Consumer");

    let num_records: u32 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let pg_host = std::env::var("PG_HOST").unwrap_or_else(|_| "localhost".to_string());
    let kafka_brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let topic = std::env::var("CDC_TOPIC").unwrap_or_else(|_| "cdc.public.users".to_string());

    info!("PostgreSQL: {}", pg_host);
    info!("Kafka brokers: {}", kafka_brokers);
    info!("CDC topic: {}", topic);
    info!("Test records: {}", num_records);

    // Channel to communicate insert timestamps to consumer
    let (tx, mut rx) = mpsc::channel::<(String, u64)>(1000);

    // Shared state
    let running = Arc::new(AtomicBool::new(true));
    let received_count = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    // Start consumer task
    let consumer_running = running.clone();
    let consumer_received = received_count.clone();
    let consumer_latencies = latencies.clone();
    let consumer_topic = topic.clone();
    let consumer_brokers = kafka_brokers.clone();
    let expected_records = num_records;

    let consumer_handle = tokio::spawn(async move {
        if let Err(e) = run_consumer(
            &consumer_brokers,
            &consumer_topic,
            &mut rx,
            consumer_running,
            consumer_received,
            consumer_latencies,
            expected_records,
        )
        .await
        {
            error!("Consumer error: {}", e);
        }
    });

    // Wait for consumer to be ready
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Connect to PostgreSQL
    let conn_str = format!(
        "host={} user=testuser password=testpass dbname=testdb",
        pg_host
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("Failed to connect to PostgreSQL")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("PostgreSQL connection error: {}", e);
        }
    });

    info!("Connected to PostgreSQL, starting inserts...");

    // Insert records with timestamps
    let insert_start = Instant::now();
    for i in 0..num_records {
        let timestamp_ms = now_ms();
        let name = format!("e2e_user_{}_{}", timestamp_ms, i);
        let email = format!("e2e_{}_{}@test.com", timestamp_ms, i);

        // Send timestamp to consumer for correlation
        tx.send((email.clone(), timestamp_ms)).await?;

        client
            .execute(
                "INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
                &[&name, &email, &(25 + (i % 50) as i32)],
            )
            .await
            .context("Failed to insert record")?;

        if (i + 1) % 10 == 0 {
            info!("Inserted {}/{} records", i + 1, num_records);
        }

        // Small delay to avoid overwhelming Debezium
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let insert_duration = insert_start.elapsed();
    info!(
        "All {} records inserted in {:.2}s",
        num_records,
        insert_duration.as_secs_f64()
    );

    // Wait for all records to be consumed (with timeout)
    let timeout = Duration::from_secs(60);
    let wait_start = Instant::now();

    while received_count.load(Ordering::Relaxed) < num_records as u64 {
        if wait_start.elapsed() > timeout {
            warn!(
                "Timeout waiting for records. Received {}/{}",
                received_count.load(Ordering::Relaxed),
                num_records
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Stop consumer
    running.store(false, Ordering::Relaxed);
    drop(tx);

    // Wait for consumer to finish
    let _ = tokio::time::timeout(Duration::from_secs(5), consumer_handle).await;

    // Calculate statistics
    let latency_data = latencies.lock().await;
    print_statistics(&latency_data, num_records);

    Ok(())
}

async fn run_consumer(
    brokers: &str,
    topic: &str,
    rx: &mut mpsc::Receiver<(String, u64)>,
    running: Arc<AtomicBool>,
    received_count: Arc<AtomicU64>,
    latencies: Arc<tokio::sync::Mutex<Vec<LatencyRecord>>>,
    expected_records: u32,
) -> Result<()> {
    // Build pending timestamps map
    let mut pending: HashMap<String, u64> = HashMap::new();

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "e2e-latency-test-group")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "latest") // Only new messages
        .set("session.timeout.ms", "30000")
        .create()
        .context("Failed to create Kafka consumer")?;

    consumer
        .subscribe(&[topic])
        .context("Failed to subscribe to topic")?;

    info!("Consumer subscribed to topic: {}", topic);

    loop {
        // Check for new insert timestamps
        while let Ok((email, ts)) = rx.try_recv() {
            pending.insert(email, ts);
        }

        // Check if we should stop
        if !running.load(Ordering::Relaxed)
            && received_count.load(Ordering::Relaxed) >= expected_records as u64
        {
            break;
        }

        // Try to receive a message with timeout
        match tokio::time::timeout(Duration::from_millis(100), consumer.recv()).await {
            Ok(Ok(msg)) => {
                let receive_time = now_ms();

                if let Some(payload) = msg.payload() {
                    if let Ok(json_str) = std::str::from_utf8(payload) {
                        // Parse Debezium message to extract email
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) {
                            let email = json["after"]["email"]
                                .as_str()
                                .or_else(|| json["before"]["email"].as_str());

                            if let Some(email) = email {
                                if let Some(insert_time) = pending.remove(email) {
                                    let record = LatencyRecord {
                                        insert_time_ms: insert_time,
                                        receive_time_ms: receive_time,
                                    };

                                    let latency = record.latency_ms();
                                    latencies.lock().await.push(record);

                                    let count = received_count.fetch_add(1, Ordering::Relaxed) + 1;

                                    if count % 10 == 0 || count == expected_records as u64 {
                                        info!(
                                            "Received {}/{} (latency: {}ms)",
                                            count, expected_records, latency
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Ok(Err(e)) => {
                error!("Kafka error: {}", e);
            }
            Err(_) => {
                // Timeout, continue loop
            }
        }
    }

    Ok(())
}

fn print_statistics(latencies: &[LatencyRecord], expected: u32) {
    info!("=== End-to-End CDC Latency Results ===");
    info!("Expected records: {}", expected);
    info!("Received records: {}", latencies.len());

    if latencies.is_empty() {
        warn!("No latency data collected!");
        return;
    }

    let mut values: Vec<u64> = latencies.iter().map(|r| r.latency_ms()).collect();
    values.sort();

    let sum: u64 = values.iter().sum();
    let avg = sum as f64 / values.len() as f64;
    let min = values[0];
    let max = values[values.len() - 1];
    let p50 = values[values.len() / 2];
    let p95 = values[((values.len() as f64 * 0.95) as usize).min(values.len() - 1)];
    let p99 = values[((values.len() as f64 * 0.99) as usize).min(values.len() - 1)];

    info!("Pipeline: PostgreSQL INSERT -> Debezium -> FluxMQ -> Consumer");
    info!("-------------------------------------------");
    info!("Samples: {}", values.len());
    info!("Min:     {} ms", min);
    info!("Avg:     {:.2} ms", avg);
    info!("P50:     {} ms", p50);
    info!("P95:     {} ms", p95);
    info!("P99:     {} ms", p99);
    info!("Max:     {} ms", max);
    info!("-------------------------------------------");

    if avg < 100.0 {
        info!("Performance: EXCELLENT (<100ms avg)");
    } else if avg < 500.0 {
        info!("Performance: GOOD (<500ms avg)");
    } else if avg < 1000.0 {
        info!("Performance: ACCEPTABLE (<1s avg)");
    } else {
        warn!("Performance: NEEDS IMPROVEMENT (>1s avg)");
    }
}
