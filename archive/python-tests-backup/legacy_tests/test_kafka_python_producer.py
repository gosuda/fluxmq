#!/usr/bin/env python3
"""
FluxMQ Kafka Compatibility Test - Python Producer
Testing FluxMQ with standard kafka-python client
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
import sys

def test_producer():
    print("🐍 FluxMQ Kafka Python Producer Test")
    print("=" * 50)
    
    try:
        # Connect to FluxMQ using standard Kafka client
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None,
            retries=3,
            acks='all'  # Wait for all replicas
        )
        
        print("✅ Successfully connected to FluxMQ broker")
        
        # Test 1: Send message without key
        topic = 'python-test-topic'
        message1 = {'message': 'Hello from Python!', 'timestamp': time.time()}
        
        print(f"\n📤 Sending message to topic '{topic}'...")
        future = producer.send(topic, value=message1)
        
        # Get metadata
        record_metadata = future.get(timeout=10)
        print(f"✅ Message sent successfully!")
        print(f"   📍 Topic: {record_metadata.topic}")
        print(f"   📍 Partition: {record_metadata.partition}")
        print(f"   📍 Offset: {record_metadata.offset}")
        
        # Test 2: Send message with key (for partition assignment)
        print(f"\n📤 Sending keyed message...")
        message2 = {'message': 'Hello with key!', 'user_id': 'user-123'}
        
        future2 = producer.send(topic, key='user-123', value=message2)
        record_metadata2 = future2.get(timeout=10)
        print(f"✅ Keyed message sent successfully!")
        print(f"   📍 Topic: {record_metadata2.topic}")
        print(f"   📍 Partition: {record_metadata2.partition}")
        print(f"   📍 Offset: {record_metadata2.offset}")
        
        # Test 3: Send multiple messages
        print(f"\n📤 Sending batch of messages...")
        for i in range(5):
            message = {
                'message': f'Batch message {i}',
                'batch_id': 'batch-1',
                'sequence': i
            }
            producer.send(topic, key=f'batch-key-{i}', value=message)
        
        # Flush to ensure all messages are sent
        producer.flush()
        print("✅ Batch messages sent successfully!")
        
        print(f"\n🎉 Python kafka-python client test completed successfully!")
        print(f"   FluxMQ successfully handled Kafka protocol requests")
        
    except KafkaError as e:
        print(f"❌ Kafka error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)
    finally:
        if 'producer' in locals():
            producer.close()

if __name__ == "__main__":
    test_producer()