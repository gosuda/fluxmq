#!/usr/bin/env python3

"""
Test script for FluxMQ Enhanced DescribeTopics functionality
"""

from kafka import KafkaConsumer
import time

def test_describe_topics():
    print("🔍 Testing Enhanced DescribeTopics functionality...")
    
    try:
        consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], consumer_timeout_ms=1000)
        
        # Test 1: Describe all topics (List all with metadata)
        print("\n📋 Test 1: Describe all topics")
        topics = consumer.topics()
        print(f"✅ Found {len(topics)} topics:")
        
        for topic in sorted(topics):
            partitions = consumer.partitions_for_topic(topic)
            if partitions:
                print(f"  📁 {topic}: {len(partitions)} partitions {list(partitions)}")
            else:
                print(f"  📁 {topic}: no partitions available")
        
        # Test 2: Describe specific topics
        print("\n📋 Test 2: Describe specific topics")
        specific_topics = ['test-topic', 'python-test-topic', 'offset-test-topic']
        
        for topic in specific_topics:
            partitions = consumer.partitions_for_topic(topic)
            if partitions:
                print(f"  ✅ {topic}: {len(partitions)} partitions")
                # Try to get more detailed info
                try:
                    # Test beginning and end offsets if possible
                    for partition in partitions:
                        beginning_offsets = consumer.beginning_offsets({consumer.TopicPartition(topic, partition): None})
                        end_offsets = consumer.end_offsets({consumer.TopicPartition(topic, partition): None})
                        print(f"    📊 Partition {partition}: offsets {list(beginning_offsets.values())[0]}-{list(end_offsets.values())[0]}")
                except Exception as e:
                    print(f"    ⚠️  Could not get offset info: {e}")
            else:
                print(f"  ❌ {topic}: not found or no partitions")
        
        # Test 3: Describe non-existent topic
        print("\n📋 Test 3: Describe non-existent topic")
        fake_topic = 'non-existent-topic'
        partitions = consumer.partitions_for_topic(fake_topic)
        if partitions:
            print(f"  ❌ Unexpected: {fake_topic} found with {len(partitions)} partitions")
        else:
            print(f"  ✅ {fake_topic}: correctly returned as not found")

        consumer.close()
        return True
        
    except Exception as e:
        print(f"❌ DescribeTopics test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if test_describe_topics():
        print("\n✅ Enhanced DescribeTopics test completed successfully!")
    else:
        print("\n❌ Enhanced DescribeTopics test failed!")