#!/usr/bin/env python3
"""
Simple producer test to debug codec error
"""

import time
from kafka import KafkaProducer
import json

def test_producer_no_compression():
    print("=== Testing Producer without compression ===")
    
    try:
        # Try without compression_type parameter
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        print("✓ Producer created without compression_type")
        
        # Send test message
        future = producer.send('test-topic-simple', value={"test": "message", "timestamp": time.time()})
        result = future.get(timeout=10)
        print(f"✓ Message sent: partition={result.partition}, offset={result.offset}")
        
        producer.close()
        return True
        
    except Exception as e:
        print(f"✗ Producer without compression_type failed: {e}")
        return False

def test_producer_with_none_compression():
    print("\n=== Testing Producer with compression_type='none' ===")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            compression_type='none'
        )
        
        print("✓ Producer created with compression_type='none'")
        
        # Send test message
        future = producer.send('test-topic-none', value={"test": "message", "timestamp": time.time()})
        result = future.get(timeout=10)
        print(f"✓ Message sent: partition={result.partition}, offset={result.offset}")
        
        producer.close()
        return True
        
    except Exception as e:
        print(f"✗ Producer with compression_type='none' failed: {e}")
        return False

def test_producer_with_gzip_compression():
    print("\n=== Testing Producer with compression_type='gzip' ===")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            compression_type='gzip'
        )
        
        print("✓ Producer created with compression_type='gzip'")
        
        # Send test message
        future = producer.send('test-topic-gzip', value={"test": "message", "timestamp": time.time()})
        result = future.get(timeout=10)
        print(f"✓ Message sent: partition={result.partition}, offset={result.offset}")
        
        producer.close()
        return True
        
    except Exception as e:
        print(f"✗ Producer with compression_type='gzip' failed: {e}")
        return False

if __name__ == "__main__":
    results = []
    results.append(test_producer_no_compression())
    results.append(test_producer_with_none_compression())
    results.append(test_producer_with_gzip_compression())
    
    print(f"\n=== Summary: {sum(results)}/3 tests passed ===")