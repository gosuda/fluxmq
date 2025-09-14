#!/usr/bin/env python3
"""
Simple consumer test to check record batch format issues
"""

from kafka import KafkaConsumer
import json
import time
import sys

def test_simple_consumer():
    print("Testing simple consumer on existing topic...")
    
    # Use an existing topic that has data
    consumer = KafkaConsumer(
        'python-test-topic',  # This topic already exists with messages
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        consumer_timeout_ms=3000,  # 3 second timeout
        value_deserializer=lambda m: m.decode('utf-8') if m else None,
        key_deserializer=lambda k: k.decode('utf-8') if k else None
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
            print(f"  Value Type: {type(message.value)}")
            print(f"  Value Raw: {repr(message.value)}")
            print()
            
            if messages_received >= 3:
                print("Received 3 messages, stopping.")
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
        print(f"✅ SUCCESS: Consumer received {messages_received} messages!")
        return True
    else:
        print(f"❌ FAILURE: Consumer received 0 messages (expected some)")
        return False

if __name__ == "__main__":
    success = test_simple_consumer()
    sys.exit(0 if success else 1)