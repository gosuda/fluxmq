#!/usr/bin/env python3
"""
Simple Kafka connectivity test
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

def simple_test():
    print("🧪 Simple Kafka Connectivity Test")
    print("=" * 40)
    
    try:
        # Very simple connection test
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=str.encode,
            request_timeout_ms=5000,
            retries=1
        )
        
        print("✅ Producer created successfully")
        
        # Send a simple message
        topic = 'test'
        message = f"Hello FluxMQ! Time: {time.time()}"
        
        print(f"📤 Sending message to topic '{topic}': {message}")
        
        future = producer.send(topic, message)
        result = future.get(timeout=10)
        
        print(f"✅ Message sent successfully!")
        print(f"   Topic: {result.topic}")
        print(f"   Partition: {result.partition}")
        print(f"   Offset: {result.offset}")
        
        producer.close()
        print("🎉 Test completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print(f"   Error type: {type(e)}")

if __name__ == "__main__":
    simple_test()