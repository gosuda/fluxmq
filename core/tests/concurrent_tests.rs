use fluxmq::{
    broker::MessageHandler,
    protocol::{FetchRequest, Message, ProduceRequest, Request, Response},
};
use std::sync::Arc;
use tokio::task::JoinSet;

#[tokio::test]
async fn test_concurrent_producers() {
    let handler = Arc::new(MessageHandler::new().unwrap());
    let num_producers = 10;
    let messages_per_producer = 100;

    let mut tasks = JoinSet::new();

    // Spawn multiple producer tasks
    for producer_id in 0..num_producers {
        let handler = Arc::clone(&handler);
        tasks.spawn(async move {
            for msg_id in 0..messages_per_producer {
                let message = Message::new(format!("producer_{}_message_{}", producer_id, msg_id));
                let produce_req = ProduceRequest {
                    correlation_id: (producer_id * 1000 + msg_id) as i32,
                    topic: "concurrent-topic".to_string(),
                    partition: 0,
                    messages: vec![message],
                    acks: 1,
                    timeout_ms: 5000,
                };

                let response = handler
                    .handle_request(Request::Produce(produce_req))
                    .await
                    .expect("Failed to produce message");

                // Verify the response is a produce response
                match response {
                    Response::Produce(resp) => {
                        assert_eq!(resp.error_code, 0);
                        assert_eq!(resp.topic, "concurrent-topic");
                        assert_eq!(resp.partition, 0);
                    }
                    _ => panic!("Expected produce response"),
                }
            }
            producer_id
        });
    }

    // Wait for all producers to complete
    let mut completed_producers = Vec::new();
    while let Some(result) = tasks.join_next().await {
        let producer_id = result.expect("Producer task failed");
        completed_producers.push(producer_id);
    }

    assert_eq!(completed_producers.len(), num_producers);

    // Verify all messages were stored
    let fetch_req = FetchRequest {
        correlation_id: 9999,
        topic: "concurrent-topic".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 1024 * 1024, // 1MB
        timeout_ms: 3000,
    };

    let response = handler
        .handle_request(Request::Fetch(fetch_req))
        .await
        .expect("Failed to fetch messages");

    match response {
        Response::Fetch(fetch_resp) => {
            assert_eq!(
                fetch_resp.messages.len(),
                num_producers * messages_per_producer
            );
            assert_eq!(fetch_resp.error_code, 0);

            // Verify messages are in order by offset
            for (i, (offset, _)) in fetch_resp.messages.iter().enumerate() {
                assert_eq!(*offset, i as u64);
            }
        }
        _ => panic!("Expected fetch response"),
    }
}

#[tokio::test]
async fn test_concurrent_consumers() {
    let handler = Arc::new(MessageHandler::new().unwrap());
    let num_messages = 100;

    // First, produce a batch of messages
    let mut messages = Vec::new();
    for i in 0..num_messages {
        messages.push(Message::new(format!("message_{}", i)));
    }

    let produce_req = ProduceRequest {
        correlation_id: 1,
        topic: "consumer-topic".to_string(),
        partition: 0,
        messages,
        acks: 1,
        timeout_ms: 5000,
    };

    let response = handler
        .handle_request(Request::Produce(produce_req))
        .await
        .expect("Failed to produce batch");

    match response {
        Response::Produce(resp) => assert_eq!(resp.base_offset, 0),
        _ => panic!("Expected produce response"),
    }

    let num_consumers = 5;
    let messages_per_consumer = num_messages / num_consumers;
    let mut tasks = JoinSet::new();

    // Spawn multiple consumer tasks, each reading different offset ranges
    for consumer_id in 0..num_consumers {
        let handler = Arc::clone(&handler);
        let start_offset = consumer_id * messages_per_consumer;

        tasks.spawn(async move {
            let fetch_req = FetchRequest {
                correlation_id: (1000 + consumer_id) as i32,
                topic: "consumer-topic".to_string(),
                partition: 0,
                offset: start_offset as u64,
                max_bytes: 1024 * 10, // 10KB
                timeout_ms: 3000,
            };

            let response = handler
                .handle_request(Request::Fetch(fetch_req))
                .await
                .expect("Failed to fetch messages");

            match response {
                Response::Fetch(fetch_resp) => {
                    assert_eq!(fetch_resp.error_code, 0);

                    // Each consumer should get messages_per_consumer messages
                    let expected_count = if consumer_id == num_consumers - 1 {
                        // Last consumer gets remaining messages
                        num_messages - start_offset
                    } else {
                        messages_per_consumer
                    };

                    assert!(fetch_resp.messages.len() >= expected_count.min(10)); // Limited by max_bytes
                    fetch_resp.messages.len()
                }
                _ => panic!("Expected fetch response"),
            }
        });
    }

    // Wait for all consumers to complete
    let mut total_fetched = 0;
    while let Some(result) = tasks.join_next().await {
        let fetched_count = result.expect("Consumer task failed");
        total_fetched += fetched_count;
    }

    // Verify that some messages were fetched by consumers
    assert!(total_fetched > 0);
}

#[tokio::test]
async fn test_concurrent_mixed_operations() {
    let handler = Arc::new(MessageHandler::new().unwrap());
    let mut tasks = JoinSet::new();

    // Spawn producer task
    let handler_clone = Arc::clone(&handler);
    tasks.spawn(async move {
        for i in 0..50 {
            let message = Message::new(format!("mixed_message_{}", i));
            let produce_req = ProduceRequest {
                correlation_id: i,
                topic: "mixed-topic".to_string(),
                partition: 0,
                messages: vec![message],
                acks: 1,
                timeout_ms: 5000,
            };

            handler_clone
                .handle_request(Request::Produce(produce_req))
                .await
                .expect("Failed to produce message");

            // Small delay to allow interleaving
            tokio::task::yield_now().await;
        }
        "producer"
    });

    // Spawn consumer task
    let handler_clone = Arc::clone(&handler);
    tasks.spawn(async move {
        let mut last_offset = 0;
        let mut _total_fetched = 0;

        for _ in 0..20 {
            let fetch_req = FetchRequest {
                correlation_id: 2000,
                topic: "mixed-topic".to_string(),
                partition: 0,
                offset: last_offset,
                max_bytes: 1024,
                timeout_ms: 1000,
            };

            if let Ok(Response::Fetch(fetch_resp)) = handler_clone
                .handle_request(Request::Fetch(fetch_req))
                .await
            {
                _total_fetched += fetch_resp.messages.len();
                if !fetch_resp.messages.is_empty() {
                    last_offset = fetch_resp.messages.last().unwrap().0 + 1;
                }
            }

            // Small delay between fetch attempts
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        "consumer"
    });

    // Wait for both tasks to complete
    let mut results = Vec::new();
    while let Some(result) = tasks.join_next().await {
        let task_result = result.expect("Task failed");
        results.push(task_result);
    }

    assert_eq!(results.len(), 2);
    assert!(results.contains(&"producer"));
    assert!(results.contains(&"consumer"));
}

#[tokio::test]
async fn test_concurrent_different_partitions() {
    let handler = Arc::new(MessageHandler::new().unwrap());
    let num_partitions = 3; // Match default partition count
    let messages_per_partition = 20;
    let mut tasks = JoinSet::new();

    // Spawn tasks for different partitions
    for partition_id in 0..num_partitions {
        let handler = Arc::clone(&handler);
        tasks.spawn(async move {
            let mut produced_offsets = Vec::new();

            for msg_id in 0..messages_per_partition {
                let message = Message::new(format!("partition_{}_msg_{}", partition_id, msg_id));
                let produce_req = ProduceRequest {
                    correlation_id: (partition_id * 100 + msg_id) as i32,
                    topic: "multi-partition".to_string(),
                    partition: partition_id as u32,
                    messages: vec![message],
                    acks: 1,
                    timeout_ms: 5000,
                };

                let response = handler
                    .handle_request(Request::Produce(produce_req))
                    .await
                    .expect("Failed to produce message");

                match response {
                    Response::Produce(resp) => {
                        produced_offsets.push(resp.base_offset);
                    }
                    _ => panic!("Expected produce response"),
                }
            }

            (partition_id, produced_offsets)
        });
    }

    // Collect results
    let mut partition_results = Vec::new();
    while let Some(result) = tasks.join_next().await {
        let (partition_id, offsets) = result.expect("Partition task failed");
        partition_results.push((partition_id, offsets));
    }

    assert_eq!(partition_results.len(), num_partitions);

    // Verify each partition has its own offset sequence starting from 0
    for (partition_id, offsets) in partition_results {
        assert_eq!(offsets.len(), messages_per_partition);
        for (i, offset) in offsets.iter().enumerate() {
            assert_eq!(
                *offset, i as u64,
                "Partition {} offset mismatch",
                partition_id
            );
        }
    }
}

#[tokio::test]
async fn test_high_throughput_stress() {
    let handler = Arc::new(MessageHandler::new().unwrap());
    let num_concurrent_producers = 20;
    let messages_per_producer = 50;
    let mut tasks = JoinSet::new();

    let start_time = std::time::Instant::now();

    // Spawn many concurrent producers
    for producer_id in 0..num_concurrent_producers {
        let handler = Arc::clone(&handler);
        tasks.spawn(async move {
            let mut successful_produces = 0;

            for msg_id in 0..messages_per_producer {
                let message = Message::new(format!("stress_{}_{}", producer_id, msg_id));
                let produce_req = ProduceRequest {
                    correlation_id: (producer_id * 1000 + msg_id) as i32,
                    topic: "stress-topic".to_string(),
                    partition: (producer_id % 3) as u32, // Distribute across 3 partitions
                    messages: vec![message],
                    acks: 1,
                    timeout_ms: 5000,
                };

                if handler
                    .handle_request(Request::Produce(produce_req))
                    .await
                    .is_ok()
                {
                    successful_produces += 1;
                }
            }

            successful_produces
        });
    }

    // Wait for all producers and count successful operations
    let mut total_successful = 0;
    while let Some(result) = tasks.join_next().await {
        let successful_produces = result.expect("Producer task failed");
        total_successful += successful_produces;
    }

    let duration = start_time.elapsed();
    let expected_total = num_concurrent_producers * messages_per_producer;

    assert_eq!(total_successful, expected_total);

    // Log performance metrics (for manual verification)
    let throughput = total_successful as f64 / duration.as_secs_f64();
    println!(
        "Stress test completed: {} messages in {:?} ({:.0} msg/sec)",
        total_successful, duration, throughput
    );

    // Verify we can still read back the data
    let fetch_req = FetchRequest {
        correlation_id: 99999,
        topic: "stress-topic".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 1024 * 1024, // 1MB
        timeout_ms: 3000,
    };

    let response = handler
        .handle_request(Request::Fetch(fetch_req))
        .await
        .expect("Failed to fetch after stress test");

    match response {
        Response::Fetch(fetch_resp) => {
            assert!(fetch_resp.messages.len() > 0);
            assert_eq!(fetch_resp.error_code, 0);
        }
        _ => panic!("Expected fetch response"),
    }
}
