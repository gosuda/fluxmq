#!/usr/bin/env python3

import time
import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.errors import KafkaError

def test_offset_commit_fetch():
    """Test consumer offset commit and fetch operations"""
    
    # First, produce some messages
    print("=== Step 1: Producing Test Messages ===")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None
    )
    
    # Send 5 test messages
    for i in range(5):
        message = {
            'id': i,
            'message': f'Offset test message {i}',
            'timestamp': time.time()
        }
        
        future = producer.send('offset-test-topic', key=f'key-{i}', value=message)
        record_metadata = future.get(timeout=10)
        print(f"✓ Sent message {i}: offset={record_metadata.offset}")
        
    producer.close()
    print("✓ All messages produced successfully")
    
    # Now test consumer with offset operations
    print("\n=== Step 2: Testing Consumer Offset Management ===")
    
    # Consumer 1: Read first 3 messages and commit offset
    print("\n--- Consumer 1: Reading and committing offsets ---")
    consumer1 = KafkaConsumer(
        'offset-test-topic',
        bootstrap_servers=['localhost:9092'],
        group_id='offset-test-group',
        partition_assignment_strategy=[RangePartitionAssignor],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        auto_offset_reset='earliest',
        enable_auto_commit=False,  # Manual commit for testing
        consumer_timeout_ms=5000
    )
    
    try:
        messages_consumed = []
        for message in consumer1:
            print(f"✓ Consumer1 read: key={message.key}, offset={message.offset}")
            messages_consumed.append(message)
            
            if len(messages_consumed) >= 3:
                # Commit after reading 3 messages
                consumer1.commit()
                print(f"✓ Committed offset after reading {len(messages_consumed)} messages")
                break
                
    except Exception as e:
        print(f"✗ Consumer1 error: {e}")
    finally:
        consumer1.close()
    
    # Consumer 2: Start from committed offset (should read remaining messages)
    print("\n--- Consumer 2: Reading from committed offset ---")
    consumer2 = KafkaConsumer(
        'offset-test-topic',
        bootstrap_servers=['localhost:9092'],
        group_id='offset-test-group',  # Same group
        partition_assignment_strategy=[RangePartitionAssignor],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=5000
    )
    
    try:
        messages_consumed_2 = []
        for message in consumer2:
            print(f"✓ Consumer2 read: key={message.key}, offset={message.offset}")
            messages_consumed_2.append(message)
            
            if len(messages_consumed_2) >= 5:  # Safety limit
                break
                
        if len(messages_consumed_2) == 0:
            print("✗ Consumer2 read no messages - possible offset fetch issue")
        elif len(messages_consumed_2) == 2:
            print("✓ Consumer2 correctly read remaining 2 messages from committed offset")
        else:
            print(f"? Consumer2 read {len(messages_consumed_2)} messages (expected 2)")
            
    except Exception as e:
        print(f"✗ Consumer2 error: {e}")
    finally:
        consumer2.close()
    
    print("\n=== Offset Management Test Complete ===")

def test_offset_fetch_api():
    """Test offset fetch API directly"""
    print("\n=== Step 3: Testing Direct Offset Fetch ===")
    
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        group_id='offset-test-group',
        partition_assignment_strategy=[RangePartitionAssignor]
    )
    
    try:
        # Get committed offsets for the group
        from kafka import TopicPartition
        tp = TopicPartition('offset-test-topic', 0)
        
        committed_offsets = consumer.committed(tp)
        print(f"✓ Committed offset for partition 0: {committed_offsets}")
        
    except Exception as e:
        print(f"✗ Offset fetch error: {e}")
    finally:
        consumer.close()

def main():
    print("=== FluxMQ Offset Management Test ===")
    
    try:
        test_offset_commit_fetch()
        test_offset_fetch_api()
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        
    print("\n=== Test Complete ===")

if __name__ == "__main__":
    main()