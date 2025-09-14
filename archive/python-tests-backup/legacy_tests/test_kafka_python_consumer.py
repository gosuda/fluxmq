#!/usr/bin/env python3
"""
FluxMQ Kafka Compatibility Test - Python Consumer
Testing FluxMQ with standard kafka-python client
"""

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import sys
import signal
import time

def test_consumer():
    print("🐍 FluxMQ Kafka Python Consumer Test")
    print("=" * 50)
    
    consumer = None
    
    def signal_handler(sig, frame):
        print("\n🛑 Shutting down consumer...")
        if consumer:
            consumer.close()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Connect to FluxMQ using standard Kafka client
        consumer = KafkaConsumer(
            'python-test-topic',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
            key_deserializer=lambda m: m.decode('utf-8') if m else None,
            auto_offset_reset='earliest',  # Start from beginning
            enable_auto_commit=True,
            group_id='python-test-group',
            consumer_timeout_ms=5000  # Timeout after 5 seconds
        )
        
        print("✅ Successfully connected to FluxMQ broker")
        print("📖 Starting to consume messages...")
        print("   (Press Ctrl+C to stop)\n")
        
        message_count = 0
        start_time = time.time()
        
        for message in consumer:
            message_count += 1
            print(f"📨 Message #{message_count}:")
            print(f"   📍 Topic: {message.topic}")
            print(f"   📍 Partition: {message.partition}")
            print(f"   📍 Offset: {message.offset}")
            print(f"   🔑 Key: {message.key}")
            print(f"   📄 Value: {message.value}")
            print(f"   🕒 Timestamp: {message.timestamp}")
            print("-" * 40)
            
            # Stop after receiving some messages or timeout
            if message_count >= 10:
                print(f"\n✅ Successfully consumed {message_count} messages!")
                break
        
        elapsed_time = time.time() - start_time
        if message_count > 0:
            print(f"\n🎉 Python kafka-python consumer test completed!")
            print(f"   📊 Messages consumed: {message_count}")
            print(f"   ⏱️  Time elapsed: {elapsed_time:.2f}s")
            print(f"   FluxMQ successfully handled Kafka consume requests")
        else:
            print(f"\n⚠️  No messages found in topic (this is normal if producer hasn't run)")
            
    except KafkaError as e:
        print(f"❌ Kafka error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)
    finally:
        if consumer:
            consumer.close()

if __name__ == "__main__":
    test_consumer()