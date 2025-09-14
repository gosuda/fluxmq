#!/usr/bin/env python3
"""
Test CreateTopics API functionality
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import time

def test_create_topics():
    print("=== Testing CreateTopics API ===" )
    
    try:
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            api_version=(0, 10, 1),
            request_timeout_ms=10000
        )
        
        print("✓ Admin client created")
        
        # Create a new topic
        new_topic_name = f"test-topic-{int(time.time())}"
        topic_list = [NewTopic(
            name=new_topic_name,
            num_partitions=2,
            replication_factor=1
        )]
        
        print(f"Creating topic: {new_topic_name}")
        
        # Create the topic
        fs = admin_client.create_topics(
            new_topics=topic_list, 
            validate_only=False,
            timeout_ms=10000
        )
        
        # Wait for operation to complete
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print(f"✓ Topic '{topic}' created successfully")
                return True
                
            except TopicAlreadyExistsError:
                print(f"! Topic '{topic}' already exists")
                return True
                
            except Exception as e:
                print(f"✗ Failed to create topic '{topic}': {e}")
                return False
                
    except Exception as e:
        print(f"✗ CreateTopics test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_create_topic_validation_only():
    print("\n=== Testing CreateTopics Validation Only ===" )
    
    try:
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            api_version=(0, 10, 1),
            request_timeout_ms=10000
        )
        
        # Create a validation-only request
        validation_topic_name = f"validation-test-{int(time.time())}"
        topic_list = [NewTopic(
            name=validation_topic_name,
            num_partitions=3,
            replication_factor=1
        )]
        
        print(f"Validating topic creation: {validation_topic_name}")
        
        # Validate topic creation (should not actually create)
        fs = admin_client.create_topics(
            new_topics=topic_list, 
            validate_only=True,  # This should only validate, not create
            timeout_ms=10000
        )
        
        # Wait for operation to complete
        for topic, f in fs.items():
            try:
                f.result()
                print(f"✓ Topic '{topic}' validation successful")
                return True
                
            except Exception as e:
                print(f"✗ Topic '{topic}' validation failed: {e}")
                return False
                
    except Exception as e:
        print(f"✗ CreateTopics validation test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🔍 FluxMQ CreateTopics API Test")
    print("=" * 40)
    
    results = []
    results.append(test_create_topics())
    results.append(test_create_topic_validation_only())
    
    print(f"\n=== Summary: {sum(results)}/2 tests passed ===")