#!/usr/bin/env python3
"""
Debug Admin API functionality
"""

from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import KafkaError
import time

def test_admin_list_topics():
    print("=== Testing Admin list_topics ===")
    
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='admin-test-client'
        )
        
        print("✓ AdminClient created")
        
        # List topics
        topics = admin.list_topics()
        print(f"✓ Topics found: {topics}")
        print(f"✓ Topics type: {type(topics)}")
        
        if hasattr(topics, 'topics'):
            topic_names = list(topics.topics.keys())
        else:
            topic_names = topics  # If it's already a list
        print(f"✓ Topic names: {topic_names}")
        
        admin.close()
        return True
        
    except Exception as e:
        print(f"✗ Admin list_topics failed: {e}")
        return False

def test_admin_create_topics():
    print("\n=== Testing Admin create_topics ===")
    
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='admin-create-test'
        )
        
        print("✓ AdminClient created")
        
        # Create a new topic
        topic_name = f"admin-test-topic-{int(time.time())}"
        new_topic = NewTopic(
            name=topic_name,
            num_partitions=1,
            replication_factor=1
        )
        
        print(f"📤 Creating topic: {topic_name}")
        result = admin.create_topics([new_topic])
        
        # Wait for result
        for topic, future in result.items():
            try:
                future.result(timeout=10)  # Block until operation completes
                print(f"✓ Topic '{topic}' created successfully")
            except Exception as e:
                print(f"✗ Topic '{topic}' creation failed: {e}")
                admin.close()
                return False
        
        admin.close()
        return True
        
    except Exception as e:
        print(f"✗ Admin create_topics failed: {e}")
        return False

def test_admin_describe_topics():
    print("\n=== Testing Admin describe_topics ===") 
    
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='admin-describe-test'
        )
        
        print("✓ AdminClient created")
        
        # Describe existing topics
        topics = admin.list_topics()
        if hasattr(topics, 'topics'):
            topic_list = list(topics.topics.keys())
        else:
            topic_list = topics
            
        if topic_list:
            topic_name = topic_list[0]
            print(f"📋 Describing topic: {topic_name}")
            
            result = admin.describe_topics([topic_name])
            for topic, future in result.items():
                try:
                    topic_metadata = future.result(timeout=10)
                    print(f"✓ Topic '{topic}' described: {len(topic_metadata.partitions)} partitions")
                except Exception as e:
                    print(f"✗ Topic '{topic}' describe failed: {e}")
                    admin.close()
                    return False
        else:
            print("⚠️ No topics to describe")
        
        admin.close()
        return True
        
    except Exception as e:
        print(f"✗ Admin describe_topics failed: {e}")
        return False

if __name__ == "__main__":
    print("🔍 FluxMQ Admin API Debug Test")
    print("=" * 40)
    
    results = []
    results.append(test_admin_list_topics())
    results.append(test_admin_create_topics())
    results.append(test_admin_describe_topics())
    
    print(f"\n=== Summary: {sum(results)}/3 tests passed ===")