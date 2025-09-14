//! Simple producer example using FluxMQ core library
//!
//! This example demonstrates how to connect to and send messages to FluxMQ
//! using the core server library directly. For a real client, use Kafka-compatible
//! clients like kafka-python, kafkajs, or Apache Kafka Java clients.

use bytes::Bytes;
use fluxmq::{BrokerConfig, BrokerServer, Message};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 FluxMQ Simple Producer Example");
    println!("=================================");
    println!("📍 Note: This example demonstrates the server API.");
    println!("   For real clients, use Kafka-compatible libraries like:");
    println!("   - Java: org.apache.kafka:kafka-clients");
    println!("   - Python: kafka-python");
    println!("   - Node.js: kafkajs\n");

    // Create and start FluxMQ server
    println!("🔧 Starting FluxMQ server...");

    let config = BrokerConfig {
        port: 9093, // Use different port to avoid conflicts
        host: "127.0.0.1".to_string(),
        enable_consumer_groups: true,
        data_dir: "./fluxmq-example-data".to_string(),
        ..Default::default()
    };

    let server = BrokerServer::new(config)?;

    // Start server in background
    let server = Arc::new(server);
    let server_handle = {
        let server_clone = Arc::clone(&server);
        tokio::spawn(async move {
            if let Err(e) = server_clone.run().await {
                eprintln!("Server error: {}", e);
            }
        })
    };

    // Give server time to start
    time::sleep(Duration::from_millis(100)).await;
    println!("✅ FluxMQ server started on port 9093\n");

    // Create some test messages using the core Message struct
    let messages = vec![
        Message {
            key: Some(Bytes::from("user-123")),
            value: Bytes::from("Hello FluxMQ! Message with key."),
            headers: HashMap::new(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        },
        Message {
            key: None,
            value: Bytes::from("Hello FluxMQ! Message without key."),
            headers: HashMap::new(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        },
        Message {
            key: Some(Bytes::from("order-456")),
            value: Bytes::from("Order processing data."),
            headers: HashMap::new(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        },
    ];

    println!("📝 Example messages created:");
    for (i, message) in messages.iter().enumerate() {
        let key_str = message
            .key
            .as_ref()
            .map(|k| String::from_utf8_lossy(k).to_string())
            .unwrap_or("<no key>".to_string());
        let value_str = String::from_utf8_lossy(&message.value);
        println!(
            "   Message {}: Key='{}', Value='{}'",
            i + 1,
            key_str,
            value_str
        );
    }

    println!("\n✅ Messages prepared successfully!");
    println!("📡 To actually send these messages, use a Kafka client:");
    println!("\n   # Python example:");
    println!("   from kafka import KafkaProducer");
    println!("   producer = KafkaProducer(bootstrap_servers=['localhost:9093'])");
    println!("   producer.send('test-topic', b'Hello FluxMQ!')");
    println!("\n   # Java example:");
    println!("   Properties props = new Properties();");
    println!("   props.put(\"bootstrap.servers\", \"localhost:9093\");");
    println!("   KafkaProducer<String, String> producer = new KafkaProducer<>(props);");
    println!("   producer.send(new ProducerRecord<>(\"test-topic\", \"Hello FluxMQ!\"));");

    // Clean shutdown
    println!("\n🔄 Shutting down server...");
    server_handle.abort();
    time::sleep(Duration::from_millis(100)).await;
    println!("✅ Server stopped");

    println!("\n🎉 Example completed!");
    println!("💡 Next steps:");
    println!("   1. Start FluxMQ: cargo run --release -- --port 9092");
    println!("   2. Use any Kafka client to connect to localhost:9092");
    println!("   3. Send/receive messages using standard Kafka APIs");

    Ok(())
}
