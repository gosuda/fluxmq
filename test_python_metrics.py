#!/usr/bin/env python3
"""
Test Python kafka client to verify metrics recording
"""
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

def test_metrics_recording():
    print("🐍 Testing Python kafka-python client for metrics recording...")
    
    try:
        # Create producer with minimal config
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            request_timeout_ms=10000,
            api_version_auto_timeout_ms=5000
        )
        
        print("✅ Producer created successfully")
        
        # Send test messages
        for i in range(5):
            message = {"test_message": f"message_{i}", "timestamp": time.time()}
            future = producer.send('test-topic', message)
            
            try:
                record_metadata = future.get(timeout=10)
                print(f"✅ Message {i} sent: topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}")
            except KafkaError as e:
                print(f"❌ Message {i} failed: {e}")
        
        producer.flush()
        producer.close()
        print("🎉 Python client test completed")
        
    except Exception as e:
        print(f"❌ Python client error: {e}")

if __name__ == "__main__":
    test_metrics_recording()