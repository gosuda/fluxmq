#!/usr/bin/env python3

import time
from kafka import KafkaConsumer, TopicPartition

def test_direct_offsetfetch():
    """Test the direct OffsetFetch API issue"""
    print("=== Testing Direct OffsetFetch API Issue ===")
    
    try:
        # Create consumer with minimal setup
        consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            group_id='offset-test-group'
        )
        
        # Create topic partition
        tp = TopicPartition('offset-test-topic', 0)
        
        # This should trigger the OffsetFetch API call
        print("Calling committed() which uses OffsetFetch API...")
        committed_offsets = consumer.committed(tp)
        print(f"✓ Committed offset for partition 0: {committed_offsets}")
        
        consumer.close()
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()

def test_api_versions_check():
    """Check what API versions are being reported"""
    print("\n=== Testing API Versions Response ===")
    
    try:
        consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
        print("✓ Consumer created successfully - API versions negotiated")
        consumer.close()
        
    except Exception as e:
        print(f"✗ Error during API versions: {e}")

if __name__ == "__main__":
    test_api_versions_check()
    test_direct_offsetfetch()