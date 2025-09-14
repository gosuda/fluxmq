use bytes::Bytes;
use fluxmq::{
    broker::{BrokerServer, MessageHandler},
    config::BrokerConfig,
    protocol::{FetchRequest, Message, MetadataRequest, ProduceRequest, Request, Response},
};

#[tokio::test]
async fn test_message_handler_produce_and_fetch() {
    let handler = MessageHandler::new().unwrap();

    // Create a produce request
    let message = Message::new("test message");
    let produce_req = ProduceRequest {
        correlation_id: 1,
        topic: "test-topic".to_string(),
        partition: 0,
        messages: vec![message],
        acks: 1,
        timeout_ms: 5000,
    };

    // Handle produce request
    let response = handler
        .handle_request(Request::Produce(produce_req))
        .await
        .expect("Failed to handle produce request");

    // Verify produce response
    match response {
        Response::Produce(produce_resp) => {
            assert_eq!(produce_resp.correlation_id, 1);
            assert_eq!(produce_resp.topic, "test-topic");
            assert_eq!(produce_resp.partition, 0);
            assert_eq!(produce_resp.base_offset, 0);
            assert_eq!(produce_resp.error_code, 0);
        }
        _ => panic!("Expected produce response"),
    }

    // Create a fetch request
    let fetch_req = FetchRequest {
        correlation_id: 2,
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 1024,
        timeout_ms: 3000,
    };

    // Handle fetch request
    let response = handler
        .handle_request(Request::Fetch(fetch_req))
        .await
        .expect("Failed to handle fetch request");

    // Verify fetch response
    match response {
        Response::Fetch(fetch_resp) => {
            assert_eq!(fetch_resp.correlation_id, 2);
            assert_eq!(fetch_resp.topic, "test-topic");
            assert_eq!(fetch_resp.partition, 0);
            assert_eq!(fetch_resp.messages.len(), 1);
            assert_eq!(fetch_resp.messages[0].0, 0); // offset
            assert_eq!(fetch_resp.messages[0].1.value, Bytes::from("test message"));
            assert_eq!(fetch_resp.error_code, 0);
        }
        _ => panic!("Expected fetch response"),
    }
}

#[tokio::test]
async fn test_message_handler_metadata() {
    let handler = MessageHandler::new().unwrap();

    // First, produce a message to create a topic
    let message = Message::new("test message");
    let produce_req = ProduceRequest {
        correlation_id: 1,
        topic: "test-topic".to_string(),
        partition: 0,
        messages: vec![message],
        acks: 1,
        timeout_ms: 5000,
    };

    handler
        .handle_request(Request::Produce(produce_req))
        .await
        .expect("Failed to handle produce request");

    // Create a metadata request
    let metadata_req = MetadataRequest {
        correlation_id: 3,
        topics: vec!["test-topic".to_string()],
        api_version: 7,
        allow_auto_topic_creation: true,
    };

    // Handle metadata request
    let response = handler
        .handle_request(Request::Metadata(metadata_req))
        .await
        .expect("Failed to handle metadata request");

    // Verify metadata response
    match response {
        Response::Metadata(metadata_resp) => {
            assert_eq!(metadata_resp.correlation_id, 3);
            assert_eq!(metadata_resp.brokers.len(), 1);
            assert_eq!(metadata_resp.brokers[0].node_id, 0);
            assert_eq!(metadata_resp.topics.len(), 1);
            assert_eq!(metadata_resp.topics[0].name, "test-topic");
            assert_eq!(metadata_resp.topics[0].partitions.len(), 3); // Default partition count
            assert_eq!(metadata_resp.topics[0].partitions[0].id, 0);
        }
        _ => panic!("Expected metadata response"),
    }
}

#[tokio::test]
async fn test_message_handler_empty_metadata() {
    let handler = MessageHandler::new().unwrap();

    // Create a metadata request for all topics (empty topics list)
    let metadata_req = MetadataRequest {
        correlation_id: 4,
        topics: vec![],
        api_version: 7,
        allow_auto_topic_creation: true,
    };

    // Handle metadata request
    let response = handler
        .handle_request(Request::Metadata(metadata_req))
        .await
        .expect("Failed to handle metadata request");

    // Verify metadata response (should be empty since no topics exist)
    match response {
        Response::Metadata(metadata_resp) => {
            assert_eq!(metadata_resp.correlation_id, 4);
            assert_eq!(metadata_resp.brokers.len(), 1);
            assert_eq!(metadata_resp.topics.len(), 0);
        }
        _ => panic!("Expected metadata response"),
    }
}

#[tokio::test]
async fn test_message_handler_fetch_nonexistent_topic() {
    let handler = MessageHandler::new().unwrap();

    // Create a fetch request for a non-existent topic
    let fetch_req = FetchRequest {
        correlation_id: 5,
        topic: "nonexistent-topic".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 1024,
        timeout_ms: 3000,
    };

    // Handle fetch request
    let response = handler
        .handle_request(Request::Fetch(fetch_req))
        .await
        .expect("Failed to handle fetch request");

    // Verify fetch response (should return error for nonexistent topic)
    match response {
        Response::Fetch(fetch_resp) => {
            assert_eq!(fetch_resp.correlation_id, 5);
            assert_eq!(fetch_resp.topic, "nonexistent-topic");
            assert_eq!(fetch_resp.partition, 0);
            assert_eq!(fetch_resp.messages.len(), 0);
            assert_eq!(fetch_resp.error_code, 3); // Unknown topic error
        }
        _ => panic!("Expected fetch response"),
    }
}

#[tokio::test]
async fn test_multiple_partitions() {
    let handler = MessageHandler::new().unwrap();

    // Produce messages to different partitions
    let message1 = Message::new("partition 0 message");
    let produce_req1 = ProduceRequest {
        correlation_id: 1,
        topic: "multi-partition-topic".to_string(),
        partition: 0,
        messages: vec![message1],
        acks: 1,
        timeout_ms: 5000,
    };

    let message2 = Message::new("partition 1 message");
    let produce_req2 = ProduceRequest {
        correlation_id: 2,
        topic: "multi-partition-topic".to_string(),
        partition: 1,
        messages: vec![message2],
        acks: 1,
        timeout_ms: 5000,
    };

    handler
        .handle_request(Request::Produce(produce_req1))
        .await
        .expect("Failed to produce to partition 0");

    handler
        .handle_request(Request::Produce(produce_req2))
        .await
        .expect("Failed to produce to partition 1");

    // Fetch from partition 0
    let fetch_req1 = FetchRequest {
        correlation_id: 3,
        topic: "multi-partition-topic".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 1024,
        timeout_ms: 3000,
    };

    let response1 = handler
        .handle_request(Request::Fetch(fetch_req1))
        .await
        .expect("Failed to fetch from partition 0");

    match response1 {
        Response::Fetch(fetch_resp) => {
            assert_eq!(fetch_resp.messages.len(), 1);
            assert_eq!(
                fetch_resp.messages[0].1.value,
                Bytes::from("partition 0 message")
            );
        }
        _ => panic!("Expected fetch response"),
    }

    // Fetch from partition 1
    let fetch_req2 = FetchRequest {
        correlation_id: 4,
        topic: "multi-partition-topic".to_string(),
        partition: 1,
        offset: 0,
        max_bytes: 1024,
        timeout_ms: 3000,
    };

    let response2 = handler
        .handle_request(Request::Fetch(fetch_req2))
        .await
        .expect("Failed to fetch from partition 1");

    match response2 {
        Response::Fetch(fetch_resp) => {
            assert_eq!(fetch_resp.messages.len(), 1);
            assert_eq!(
                fetch_resp.messages[0].1.value,
                Bytes::from("partition 1 message")
            );
        }
        _ => panic!("Expected fetch response"),
    }
}

#[tokio::test]
async fn test_batch_produce() {
    let handler = MessageHandler::new().unwrap();

    // Produce a batch of messages
    let messages = vec![
        Message::new("message 1"),
        Message::new("message 2"),
        Message::new("message 3"),
    ];

    let produce_req = ProduceRequest {
        correlation_id: 1,
        topic: "batch-topic".to_string(),
        partition: 0,
        messages,
        acks: 1,
        timeout_ms: 5000,
    };

    let response = handler
        .handle_request(Request::Produce(produce_req))
        .await
        .expect("Failed to handle batch produce request");

    // Verify produce response
    match response {
        Response::Produce(produce_resp) => {
            assert_eq!(produce_resp.base_offset, 0);
        }
        _ => panic!("Expected produce response"),
    }

    // Fetch all messages
    let fetch_req = FetchRequest {
        correlation_id: 2,
        topic: "batch-topic".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 4096,
        timeout_ms: 3000,
    };

    let response = handler
        .handle_request(Request::Fetch(fetch_req))
        .await
        .expect("Failed to handle fetch request");

    // Verify all messages were stored and can be fetched
    match response {
        Response::Fetch(fetch_resp) => {
            assert_eq!(fetch_resp.messages.len(), 3);
            assert_eq!(fetch_resp.messages[0].0, 0);
            assert_eq!(fetch_resp.messages[0].1.value, Bytes::from("message 1"));
            assert_eq!(fetch_resp.messages[1].0, 1);
            assert_eq!(fetch_resp.messages[1].1.value, Bytes::from("message 2"));
            assert_eq!(fetch_resp.messages[2].0, 2);
            assert_eq!(fetch_resp.messages[2].1.value, Bytes::from("message 3"));
        }
        _ => panic!("Expected fetch response"),
    }
}

// Test TCP server startup and shutdown (simplified test without actual network connections)
#[tokio::test]
async fn test_broker_server_creation() {
    let config = BrokerConfig {
        host: "127.0.0.1".to_string(),
        port: 0, // Use port 0 to let OS assign a free port
        ..Default::default()
    };

    // Test that broker server can be created without errors
    let server = BrokerServer::new(config);
    assert!(server.is_ok());
}

#[tokio::test]
async fn test_message_offset_ordering() {
    let handler = MessageHandler::new().unwrap();

    // Produce messages in sequence
    for i in 0..5 {
        let message = Message::new(format!("message {}", i));
        let produce_req = ProduceRequest {
            correlation_id: i as i32,
            topic: "ordering-topic".to_string(),
            partition: 0,
            messages: vec![message],
            acks: 1,
            timeout_ms: 5000,
        };

        let response = handler
            .handle_request(Request::Produce(produce_req))
            .await
            .expect("Failed to produce message");

        // Verify offset increments correctly
        match response {
            Response::Produce(produce_resp) => {
                assert_eq!(produce_resp.base_offset, i);
            }
            _ => panic!("Expected produce response"),
        }
    }

    // Fetch all messages and verify ordering
    let fetch_req = FetchRequest {
        correlation_id: 10,
        topic: "ordering-topic".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 4096,
        timeout_ms: 3000,
    };

    let response = handler
        .handle_request(Request::Fetch(fetch_req))
        .await
        .expect("Failed to fetch messages");

    match response {
        Response::Fetch(fetch_resp) => {
            assert_eq!(fetch_resp.messages.len(), 5);
            for (i, (offset, message)) in fetch_resp.messages.iter().enumerate() {
                assert_eq!(*offset, i as u64);
                assert_eq!(message.value, Bytes::from(format!("message {}", i)));
            }
        }
        _ => panic!("Expected fetch response"),
    }
}
