#!/usr/bin/env python3
"""
Test script to verify the new Kafka record batch format implementation using an existing topic
"""

from kafka import KafkaConsumer, KafkaProducer
import json
import time
import sys

def test_consumer_with_existing_topic():
    print("Testing consumer with existing topic...")
    
    # Test with an existing topic: python-test-topic
    test_topic = "python-test-topic"
    
    # Test 1: Producer adds new messages to existing topic
    print(f"\n1. Adding messages to existing topic: {test_topic}")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    
    # Send a few test messages
    for i in range(3):
        key = f"new-key-{i}"
        value = {"message": f"New test message {i}", "timestamp": time.time()}
        print(f"Sending message {i}: key={key}, value={value}")
        
        future = producer.send(test_topic, key=key, value=value)
        result = future.get(timeout=10)
        print(f"  -> Sent to partition {result.partition}, offset {result.offset}")
    
    producer.flush()
    producer.close()
    print("Producer completed successfully!")
    
    # Test 2: Consumer reads messages (both existing and new)
    print(f"\n2. Testing consumer with new record batch format on {test_topic}...")
    consumer = KafkaConsumer(
        test_topic,
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000  # 5 second timeout
    )
    
    messages_received = 0
    
    try:
        print("Polling for messages...")
        for message in consumer:
            messages_received += 1
            print(f"Received message {messages_received}:")
            print(f"  Topic: {message.topic}")
            print(f"  Partition: {message.partition}")
            print(f"  Offset: {message.offset}")
            print(f"  Key: {message.key}")
            print(f"  Value: {message.value}")
            print(f"  Timestamp: {message.timestamp}")
            print()
            
            # Stop after receiving 10 messages to avoid too much output
            if messages_received >= 10:
                print("Received enough messages for testing!")
                break
                
    except Exception as e:
        print(f"Error during message consumption: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        consumer.close()
    
    if messages_received > 0:
        print(f"✅ SUCCESS: Consumer successfully parsed {messages_received} messages with new record batch format!")
        return True
    else:
        print(f"❌ FAILURE: Expected at least 1 message, got {messages_received}")
        return False

if __name__ == "__main__":
    success = test_consumer_with_existing_topic()
    sys.exit(0 if success else 1)