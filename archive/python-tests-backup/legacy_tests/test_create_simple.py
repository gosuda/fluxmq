#!/usr/bin/env python3
"""
Simple CreateTopics test using direct kafka-python calls
"""

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
import time

def test_direct_topic_creation():
    print("=== Testing Direct Topic Creation ===")
    
    try:
        # Use a producer to trigger topic creation first
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            api_version=(0, 10, 1),
            value_serializer=lambda x: x.encode('utf-8')
        )
        
        test_topic = f"auto-created-{int(time.time())}"
        print(f"Creating topic via producer: {test_topic}")
        
        # Send a message to trigger auto topic creation
        future = producer.send(test_topic, 'test message')
        producer.flush()
        
        result = future.get(timeout=10)
        print(f"✓ Message sent to topic {test_topic}: partition={result.partition}, offset={result.offset}")
        
        producer.close()
        return True
        
    except Exception as e:
        print(f"✗ Direct topic creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_admin_create_with_bootstrap():
    print("\n=== Testing Admin CreateTopics with Bootstrap Discovery ===")
    
    try:
        # Try a simpler admin client setup
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            connections_max_idle_ms=60000,
            request_timeout_ms=30000,
            api_version=(0, 10, 1)  # Try with explicit API version
        )
        
        test_topic = f"admin-created-{int(time.time())}"
        print(f"Creating topic via admin client: {test_topic}")
        
        # Create topic with explicit configuration
        topic_list = [NewTopic(
            name=test_topic,
            num_partitions=1,
            replication_factor=1
        )]
        
        # Request creation
        fs = admin_client.create_topics(new_topics=topic_list, validate_only=False)
        
        # Wait for results
        for topic, f in fs.items():
            try:
                f.result(timeout=30)  # Wait up to 30 seconds
                print(f"✓ Topic '{topic}' created successfully via admin client")
                return True
                
            except TopicAlreadyExistsError:
                print(f"! Topic '{topic}' already exists (OK)")
                return True
                
            except Exception as e:
                print(f"✗ Failed to create topic '{topic}': {e}")
                return False
                
    except Exception as e:
        print(f"✗ Admin CreateTopics with bootstrap failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🔍 Simple FluxMQ CreateTopics Test")
    print("=" * 40)
    
    results = []
    results.append(test_direct_topic_creation())
    results.append(test_admin_create_with_bootstrap())
    
    print(f"\n=== Summary: {sum(results)}/2 tests passed ===")