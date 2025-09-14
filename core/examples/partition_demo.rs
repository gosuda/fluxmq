use bytes::Bytes;
use fluxmq::{
    broker::MessageHandler,
    protocol::{FetchRequest, Message, MetadataRequest, ProduceRequest, Request},
    Result,
};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the message handler with topic manager
    let handler = MessageHandler::new()?;

    println!("🚀 FluxMQ Partitioning Demo");
    println!("==========================");

    // Test 1: Auto-create topic by producing a message
    println!("\n📊 Test 1: Auto-create topic by producing first message");
    let init_message = Message {
        key: Some(Bytes::from("init-key")),
        value: Bytes::from("Initializing topic"),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        headers: std::collections::HashMap::new(),
    };

    let init_req = ProduceRequest {
        correlation_id: 1,
        topic: "demo-topic".to_string(),
        partition: u32::MAX, // Auto-assign
        messages: vec![init_message],
        acks: 1,
        timeout_ms: 5000,
    };

    if let Ok(response) = handler.handle_request(Request::Produce(init_req)).await {
        if let fluxmq::protocol::Response::Produce(produce_resp) = response {
            println!(
                "✅ Topic auto-created, first message assigned to partition {}",
                produce_resp.partition
            );
        }
    }

    // Check metadata after topic creation
    println!("\n📊 Check topic metadata:");
    let metadata_req = MetadataRequest {
        correlation_id: 2,
        topics: vec!["demo-topic".to_string()],
        api_version: 7,
        allow_auto_topic_creation: true,
    };

    if let Ok(response) = handler
        .handle_request(Request::Metadata(metadata_req))
        .await
    {
        if let fluxmq::protocol::Response::Metadata(metadata_resp) = response {
            if !metadata_resp.topics.is_empty() {
                println!(
                    "✅ Topic has {} partitions",
                    metadata_resp.topics[0].partitions.len()
                );
                for partition in &metadata_resp.topics[0].partitions {
                    println!(
                        "  - Partition {}: Leader={:?}, Replicas={:?}",
                        partition.id, partition.leader, partition.replicas
                    );
                }
            }
        }
    }

    // Test 2: Produce messages with auto-partitioning (using u32::MAX for auto-assignment)
    println!("\n📝 Test 2: Auto-partition assignment based on message keys");
    let test_keys = vec!["user1", "user2", "user3", "user1", "user4"];

    for (i, key) in test_keys.iter().enumerate() {
        let message = Message {
            key: Some(Bytes::from(key.to_string())),
            value: Bytes::from(format!("Message {} for {}", i, key)),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            headers: std::collections::HashMap::new(),
        };

        let produce_req = ProduceRequest {
            correlation_id: i as i32 + 10,
            topic: "demo-topic".to_string(),
            partition: u32::MAX, // Auto-assign based on key
            messages: vec![message],
            acks: 1,
            timeout_ms: 5000,
        };

        if let Ok(response) = handler.handle_request(Request::Produce(produce_req)).await {
            if let fluxmq::protocol::Response::Produce(produce_resp) = response {
                println!(
                    "  Key '{}' -> Partition {} (offset {})",
                    key, produce_resp.partition, produce_resp.base_offset
                );
            }
        }
    }

    // Test 3: Round-robin assignment for keyless messages
    println!("\n🔄 Test 3: Round-robin assignment for keyless messages");
    for i in 0..6 {
        let message = Message {
            key: None,
            value: Bytes::from(format!("Keyless message {}", i)),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            headers: std::collections::HashMap::new(),
        };

        let produce_req = ProduceRequest {
            correlation_id: i as i32 + 20,
            topic: "demo-topic".to_string(),
            partition: u32::MAX, // Auto-assign using round-robin
            messages: vec![message],
            acks: 1,
            timeout_ms: 5000,
        };

        if let Ok(response) = handler.handle_request(Request::Produce(produce_req)).await {
            if let fluxmq::protocol::Response::Produce(produce_resp) = response {
                println!("  Message {} -> Partition {}", i, produce_resp.partition);
            }
        }
    }

    // Test 4: Try to fetch from each partition
    println!("\n📖 Test 4: Fetch messages from each partition");
    for partition_id in 0..3 {
        let fetch_req = FetchRequest {
            correlation_id: 30 + partition_id as i32,
            topic: "demo-topic".to_string(),
            partition: partition_id,
            offset: 0,
            max_bytes: 1024,
            timeout_ms: 1000,
        };

        if let Ok(response) = handler.handle_request(Request::Fetch(fetch_req)).await {
            if let fluxmq::protocol::Response::Fetch(fetch_resp) = response {
                println!(
                    "  Partition {}: {} messages (error_code: {})",
                    partition_id,
                    fetch_resp.messages.len(),
                    fetch_resp.error_code
                );
            }
        }
    }

    // Test 5: Error handling - try invalid partition
    println!("\n❌ Test 5: Error handling for invalid partition");
    let produce_req = ProduceRequest {
        correlation_id: 40,
        topic: "demo-topic".to_string(),
        partition: 99, // Invalid partition
        messages: vec![Message::new("test")],
        acks: 1,
        timeout_ms: 5000,
    };

    if let Ok(response) = handler.handle_request(Request::Produce(produce_req)).await {
        if let fluxmq::protocol::Response::Produce(produce_resp) = response {
            println!(
                "  Partition 99 -> Error Code: {} ({})",
                produce_resp.error_code,
                produce_resp
                    .error_message
                    .unwrap_or("No error message".to_string())
            );
        }
    }

    println!("\n✨ Demo completed successfully!");
    Ok(())
}
