#!/usr/bin/env python3

import time
from kafka import KafkaConsumer
from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor

def test_range_consumer():
    """Test consumer with explicit range assignor"""
    try:
        consumer = KafkaConsumer(
            'test-topic',
            bootstrap_servers=['localhost:9092'],
            group_id='test-group-range',
            partition_assignment_strategy=[RangePartitionAssignor],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        
        print("✓ Range consumer created successfully")
        consumer.close()
        
    except Exception as e:
        print(f"✗ Range consumer error: {e}")

def test_roundrobin_consumer():
    """Test consumer with explicit roundrobin assignor"""
    try:
        consumer = KafkaConsumer(
            'test-topic',
            bootstrap_servers=['localhost:9092'],
            group_id='test-group-rr',
            partition_assignment_strategy=[RoundRobinPartitionAssignor],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        
        print("✓ RoundRobin consumer created successfully")
        consumer.close()
        
    except Exception as e:
        print(f"✗ RoundRobin consumer error: {e}")

def test_default_consumer():
    """Test consumer with default assignor (should be range)"""
    try:
        consumer = KafkaConsumer(
            'test-topic',
            bootstrap_servers=['localhost:9092'],
            group_id='test-group-default',
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        
        print("✓ Default consumer created successfully")
        consumer.close()
        
    except Exception as e:
        print(f"✗ Default consumer error: {e}")

def main():
    print("=== FluxMQ Protocol Debug Test ===")
    
    print("\n1. Testing Range assignor:")
    test_range_consumer()
    
    print("\n2. Testing RoundRobin assignor:")
    test_roundrobin_consumer()
    
    print("\n3. Testing Default assignor:")  
    test_default_consumer()
    
    print("\n=== Test Complete ===")

if __name__ == "__main__":
    main()