#!/usr/bin/env python3
"""
Debug fetch response parsing issue
"""

from kafka import KafkaConsumer
import time

def test_fetch_from_topic():
    print("=== Testing Fetch from topic with messages ===")
    
    try:
        # Create consumer for topic that has messages
        consumer = KafkaConsumer(
            'comprehensive-test',  # Topic that has messages from producer test
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000  # Short timeout
        )
        
        print("✓ Consumer created")
        
        # Try to consume messages
        message_count = 0
        for message in consumer:
            print(f"✓ Message {message_count}: partition={message.partition}, offset={message.offset}")
            print(f"  Key: {message.key}")
            print(f"  Value: {message.value}")
            message_count += 1
            
            # Only read a few messages
            if message_count >= 3:
                break
                
        print(f"✓ Successfully consumed {message_count} messages")
        consumer.close()
        return True
        
    except Exception as e:
        print(f"✗ Fetch test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_fetch_from_empty_topic():
    print("\n=== Testing Fetch from empty topic ===")
    
    try:
        # Create consumer for topic that might be empty
        consumer = KafkaConsumer(
            'empty-topic-test',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=2000  # Short timeout for empty topic
        )
        
        print("✓ Consumer created for empty topic")
        
        # Try to consume (should timeout)
        message_count = 0
        for message in consumer:
            message_count += 1
            if message_count >= 1:  # Just test one message
                break
                
        if message_count == 0:
            print("✓ No messages (expected for empty topic)")
        else:
            print(f"✓ Found {message_count} messages in supposedly empty topic")
            
        consumer.close()
        return True
        
    except Exception as e:
        print(f"✗ Empty topic fetch test failed: {e}")
        return False

if __name__ == "__main__":
    print("🔍 FluxMQ Fetch Debug Test")
    print("=" * 40)
    
    results = []
    results.append(test_fetch_from_topic())
    results.append(test_fetch_from_empty_topic())
    
    print(f"\n=== Summary: {sum(results)}/2 tests passed ===")