#!/usr/bin/env python3

"""
Simple test script for FluxMQ ListTopics (Metadata API) functionality
"""

from kafka import KafkaConsumer
import time

def test_list_topics():
    print("🔍 Testing ListTopics functionality...")
    
    # Test ListTopics by requesting metadata for all topics
    try:
        consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], consumer_timeout_ms=1000)
        
        # This should trigger a metadata request with topics=None (ListTopics)
        print("📋 Requesting topic metadata...")
        topics = consumer.topics()  # This calls metadata API with topics=None
        
        print(f"✅ Found {len(topics)} topics:")
        for topic in sorted(topics):
            partitions = consumer.partitions_for_topic(topic)
            print(f"  - {topic}: {len(partitions) if partitions else 0} partitions")
        
        consumer.close()
        return True
        
    except Exception as e:
        print(f"❌ ListTopics test failed: {e}")
        return False

if __name__ == "__main__":
    if test_list_topics():
        print("\n✅ ListTopics test completed successfully!")
    else:
        print("\n❌ ListTopics test failed!")