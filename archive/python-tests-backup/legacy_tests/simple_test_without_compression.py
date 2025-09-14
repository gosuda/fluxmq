#!/usr/bin/env python3
"""
Simple Kafka test without compression to check FindCoordinator fix
"""

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
import time

def test_simple_producer():
    print("🚀 Simple Producer Test (No Compression)")
    
    try:
        # Create producer without compression
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=5,
            # Remove compression_type to use default (no compression)
        )
        
        print("✅ Producer created successfully")
        
        # Send test message
        message = {"test": "FindCoordinator working", "timestamp": time.time()}
        future = producer.send('simple-test', key='test-key', value=message)
        record_metadata = future.get(timeout=10)
        
        print(f"✅ Message sent successfully:")
        print(f"   Topic: {record_metadata.topic}")
        print(f"   Partition: {record_metadata.partition}")
        print(f"   Offset: {record_metadata.offset}")
        
        producer.flush()
        producer.close()
        print("🎉 Producer test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Producer error: {e}")
        return False

def test_simple_consumer():
    print("\n🔍 Simple Consumer Test")
    
    try:
        consumer = KafkaConsumer(
            'simple-test',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: m.decode('utf-8') if m else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='simple-test-group',
            consumer_timeout_ms=5000
        )
        
        print("✅ Consumer created successfully")
        
        message_count = 0
        for message in consumer:
            message_count += 1
            print(f"📥 Received message: {message.value}")
            print(f"   Key: {message.key}, Partition: {message.partition}, Offset: {message.offset}")
            break  # Just get one message
        
        consumer.close()
        
        if message_count > 0:
            print("🎉 Consumer test completed successfully!")
            return True
        else:
            print("⚠️  No messages received")
            return False
            
    except Exception as e:
        print(f"❌ Consumer error: {e}")
        return False

def main():
    print("📊 Simple FluxMQ Kafka Test (No Compression)")
    print("=" * 50)
    
    producer_ok = test_simple_producer()
    consumer_ok = test_simple_consumer()
    
    print("\n" + "=" * 50)
    if producer_ok and consumer_ok:
        print("✅ ALL TESTS PASSED! FindCoordinator fix is working!")
    else:
        print(f"❌ Tests failed - Producer: {producer_ok}, Consumer: {consumer_ok}")

if __name__ == "__main__":
    main()