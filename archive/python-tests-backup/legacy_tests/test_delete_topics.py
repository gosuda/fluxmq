#!/usr/bin/env python3

import sys
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType

def test_delete_topics():
    print("🧪 Testing DeleteTopics API implementation...")
    
    try:
        # Connect to FluxMQ server
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='delete-topics-test'
        )
        print("✅ Connected to FluxMQ server")
        
        # Create a test topic first
        test_topic = "delete-test-topic"
        topic_list = [NewTopic(name=test_topic, num_partitions=1, replication_factor=1)]
        
        print(f"📝 Creating topic: {test_topic}")
        create_result = admin_client.create_topics(new_topics=topic_list, validate_only=False)
        
        # Wait for topic creation
        for topic, future in create_result.items():
            try:
                future.result()
                print(f"✅ Topic '{topic}' created successfully")
            except Exception as e:
                if "already exists" in str(e):
                    print(f"ℹ️ Topic '{topic}' already exists")
                else:
                    print(f"❌ Failed to create topic '{topic}': {e}")
        
        # List topics to confirm it exists
        metadata = admin_client.describe_topics([test_topic])
        print(f"🔍 Topic exists: {list(metadata.keys())}")
        
        # Now test DeleteTopics API
        print(f"🗑️ Deleting topic: {test_topic}")
        delete_result = admin_client.delete_topics(topics=[test_topic])
        
        # Check delete results
        for topic, future in delete_result.items():
            try:
                future.result()
                print(f"✅ Topic '{topic}' deleted successfully")
            except Exception as e:
                print(f"❌ Failed to delete topic '{topic}': {e}")
                return False
                
        print("🎉 DeleteTopics API test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        return False
    finally:
        admin_client.close()

if __name__ == "__main__":
    success = test_delete_topics()
    sys.exit(0 if success else 1)