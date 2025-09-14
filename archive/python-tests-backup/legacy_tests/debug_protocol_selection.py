#!/usr/bin/env python3

"""
Simple test to check what protocol gets selected in JoinGroup response
"""

import time
from kafka import KafkaConsumer
from kafka.coordinator.assignors.range import RangePartitionAssignor

def test_joingroup_protocol():
    """Test what protocol gets returned in JoinGroup response"""
    print("=== Testing JoinGroup Protocol Selection ===")
    
    try:
        # Create consumer that will trigger JoinGroup
        consumer = KafkaConsumer(
            'test-topic-for-protocol',
            bootstrap_servers=['localhost:9092'],
            group_id='protocol-test-group-new',  # Use new group name
            partition_assignment_strategy=[RangePartitionAssignor],
            auto_offset_reset='earliest',
            consumer_timeout_ms=2000  # Short timeout
        )
        
        print("✓ Consumer created - JoinGroup succeeded")
        
        # Try to consume (this should trigger the protocol assignment)
        try:
            messages = []
            for message in consumer:
                messages.append(message)
                if len(messages) >= 1:  # Just try one message
                    break
        except Exception as e:
            print(f"Consume error (expected): {e}")
            
        consumer.close()
        
    except Exception as e:
        print(f"✗ Consumer creation failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_joingroup_protocol()