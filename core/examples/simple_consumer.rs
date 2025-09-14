//! Simple consumer example using FluxMQ core library
//!
//! This example demonstrates how to set up FluxMQ server for consumption.
//! For real consumers, use Kafka-compatible clients like kafka-python,
//! kafkajs, or Apache Kafka Java clients.

use bytes::Bytes;
use fluxmq::{BrokerConfig, BrokerServer, Message};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("📖 FluxMQ Simple Consumer Example");
    println!("=================================");
    println!("📍 Note: This example demonstrates the server setup.");
    println!("   For real consumers, use Kafka-compatible libraries like:");
    println!("   - Java: org.apache.kafka:kafka-clients");
    println!("   - Python: kafka-python");
    println!("   - Node.js: kafkajs\n");

    // Create and start FluxMQ server for consumption
    println!("🔧 Setting up FluxMQ server for consumption...");

    let config = BrokerConfig {
        port: 9094, // Use different port to avoid conflicts
        host: "127.0.0.1".to_string(),
        enable_consumer_groups: true,
        data_dir: "./fluxmq-consumer-data".to_string(),
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
    println!("✅ FluxMQ server started on port 9094\n");

    // Create example messages (demonstrating what you would consume)
    let example_messages = vec![
        Message {
            key: Some(Bytes::from("user-123")),
            value: Bytes::from("User login event"),
            headers: HashMap::new(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        },
        Message {
            key: Some(Bytes::from("order-456")),
            value: Bytes::from("Order placed: $99.99"),
            headers: HashMap::new(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        },
        Message {
            key: None,
            value: Bytes::from("System health check"),
            headers: HashMap::new(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        },
    ];

    println!("📋 Example messages that could be consumed:");
    for (i, message) in example_messages.iter().enumerate() {
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

    println!("\n🔍 To consume messages, use a Kafka consumer:");
    println!("\n   # Python example:");
    println!("   from kafka import KafkaConsumer");
    println!("   consumer = KafkaConsumer(");
    println!("       'test-topic',");
    println!("       bootstrap_servers=['localhost:9094'],");
    println!("       group_id='my-consumer-group'");
    println!("   )");
    println!("   for message in consumer:");
    println!("       print(f'Key: {{message.key}}, Value: {{message.value}}')");

    println!("\n   # Java example:");
    println!("   Properties props = new Properties();");
    println!("   props.put(\"bootstrap.servers\", \"localhost:9094\");");
    println!("   props.put(\"group.id\", \"my-consumer-group\");");
    println!("   props.put(\"key.deserializer\", StringDeserializer.class.getName());");
    println!("   props.put(\"value.deserializer\", StringDeserializer.class.getName());");
    println!("   KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);");
    println!("   consumer.subscribe(Arrays.asList(\"test-topic\"));");
    println!("   while (true) {{");
    println!(
        "       ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));"
    );
    println!("       for (ConsumerRecord<String, String> record : records)");
    println!(
        "           System.out.printf(\"Key: %s, Value: %s%n\", record.key(), record.value());"
    );
    println!("   }}");

    // Demonstrate consumer group functionality
    println!("\n🔄 Consumer Group Features Available:");
    println!("   ✅ Automatic partition assignment");
    println!("   ✅ Consumer rebalancing");
    println!("   ✅ Offset management");
    println!("   ✅ Group coordination");
    println!("   ✅ Multiple consumer instances");

    // Clean shutdown
    println!("\n🔄 Shutting down server...");
    server_handle.abort();
    time::sleep(Duration::from_millis(100)).await;
    println!("✅ Server stopped");

    println!("\n🎉 Consumer example completed!");
    println!("💡 Next steps:");
    println!("   1. Start FluxMQ: cargo run --release -- --port 9092 --enable-consumer-groups");
    println!("   2. Use any Kafka consumer client to connect to localhost:9092");
    println!("   3. Subscribe to topics and consume messages");
    println!("   4. Enjoy real-time message streaming!");

    Ok(())
}
