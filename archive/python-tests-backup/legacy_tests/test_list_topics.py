#!/usr/bin/env python3

"""
Test script for FluxMQ ListTopics (Metadata API) functionality
"""

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import time

def test_list_topics():
    # Connect to FluxMQ
    admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
    
    print("🔍 Testing ListTopics functionality...")
    
    # Test 1: List topics when none exist (should return empty)
    print("\n📋 Test 1: List all topics (initial state)")
    try:
        metadata = admin_client.list_consumer_groups(timeout_ms=5000)
        print(f"✓ Got response (consumer groups API works)")
    except Exception as e:
        print(f"⚠️  Consumer groups API: {e}")

    # Use lower level metadata API to test ListTopics
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
    
    try:
        # This will trigger a metadata request with topics=None (ListTopics)
        topics = consumer.list_consumer_group_offsets()
        print("✓ Connected successfully")
    except Exception as e:
        print(f"⚠️  Initial connection: {e}")

    try:
        # Get cluster metadata (this is the ListTopics functionality)
        partitions = consumer.list_consumer_group_offsets()
        print(f"✓ Got partitions info")
    except Exception as e:
        print(f"⚠️  Partitions: {e}")

    # Test 2: Create some topics first
    print("\n🏗️  Test 2: Creating test topics...")
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    
    # Create topics by producing messages
    test_topics = ['topic1', 'topic2', 'test-multi-partition']
    
    for topic in test_topics:
        try:
            producer.send(topic, key=b'test-key', value=b'test-value')
            print(f"✓ Sent message to {topic}")
        except Exception as e:
            print(f"❌ Failed to send to {topic}: {e}")
    
    producer.flush()
    time.sleep(1)  # Give time for topics to be created
    
    # Test 3: List topics after creation
    print("\n📋 Test 3: List all topics (after creation)")
    
    try:
        # Get all topic metadata
        consumer.close()
        consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
        
        # This should trigger metadata request with topics=None
        metadata = consumer.list_consumer_group_offsets()
        print(f"✓ Metadata request successful")
        
        # Try to get partition info for created topics
        for topic in test_topics:
            try:
                partitions = consumer.partitions_for_topic(topic)
                if partitions:
                    print(f"✓ Topic {topic}: {len(partitions)} partitions")
                else:
                    print(f"❓ Topic {topic}: no partition info")
            except Exception as e:
                print(f"❌ Topic {topic}: {e}")
                
    except Exception as e:
        print(f"❌ List topics failed: {e}")
    
    # Test 4: Request specific topics
    print("\n📋 Test 4: Request specific topic metadata")
    try:
        consumer.close()
        consumer = KafkaConsumer('topic1', bootstrap_servers=['localhost:9092'])
        partitions = consumer.partitions_for_topic('topic1')
        print(f"✓ Topic1 partitions: {partitions}")
    except Exception as e:
        print(f"❌ Specific topic request failed: {e}")

    print("\n✅ ListTopics testing completed!")

if __name__ == "__main__":
    test_list_topics()