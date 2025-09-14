#!/usr/bin/env python3
"""
Quick consumer test to verify FluxMQ consumer functionality after fixes
"""

import time
from kafka import KafkaProducer, KafkaConsumer

def test_consumer_functionality():
    """Test basic producer-consumer flow"""
    print("🧪 Quick Consumer Test")
    print("=" * 30)
    
    # Create a unique topic for this test
    topic_name = f"test-{int(time.time())}"
    
    # Producer: Send 10 test messages
    print(f"📤 Producing 10 messages to topic '{topic_name}'...")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 1),
        value_serializer=lambda x: x.encode('utf-8')
    )
    
    for i in range(10):
        message = f"test-message-{i}"
        producer.send(topic_name, message)
        print(f"  Sent: {message}")
    
    producer.flush()
    producer.close()
    print("✅ Producer completed")
    
    # Wait a moment for messages to be written
    time.sleep(1)
    
    # Consumer: Read the messages
    print(f"\n📥 Consuming messages from topic '{topic_name}'...")
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 1),
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000  # 5 second timeout
    )
    
    messages_received = 0
    start_time = time.time()
    
    try:
        for message in consumer:
            messages_received += 1
            decoded_value = message.value.decode('utf-8')
            print(f"  Received: {decoded_value} (offset: {message.offset})")
            
            # Stop after receiving all 10 messages
            if messages_received >= 10:
                break
                
    except Exception as e:
        print(f"  Consumer exception: {e}")
    
    end_time = time.time()
    duration = end_time - start_time
    
    consumer.close()
    
    # Results
    print(f"\n📊 Test Results:")
    print(f"  Messages sent: 10")
    print(f"  Messages received: {messages_received}")
    print(f"  Duration: {duration:.2f}s")
    
    if messages_received == 10:
        print(f"  ✅ SUCCESS: Consumer functionality is working!")
        return True
    else:
        print(f"  ❌ FAILED: Consumer only received {messages_received}/10 messages")
        return False

if __name__ == "__main__":
    success = test_consumer_functionality()
    exit(0 if success else 1)