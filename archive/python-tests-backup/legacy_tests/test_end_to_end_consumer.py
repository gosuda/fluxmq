#!/usr/bin/env python3

import time
import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import threading

def test_producer():
    """Producer to send test messages"""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None
    )
    
    try:
        # Send 10 test messages
        for i in range(10):
            message = {
                'id': i,
                'message': f'Test message {i}',
                'timestamp': time.time()
            }
            
            future = producer.send('test-topic', key=f'key-{i}', value=message)
            record_metadata = future.get(timeout=10)
            
            print(f"✓ Sent message {i}: topic={record_metadata.topic}, "
                  f"partition={record_metadata.partition}, offset={record_metadata.offset}")
            time.sleep(0.1)
            
        producer.flush()
        print("✓ Producer completed successfully")
        
    except Exception as e:
        print(f"✗ Producer error: {e}")
    finally:
        producer.close()

def test_consumer():
    """Consumer to receive test messages"""
    consumer = KafkaConsumer(
        'test-topic',
        bootstrap_servers=['localhost:9092'],
        group_id='test-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=10000  # 10 second timeout
    )
    
    try:
        print("✓ Consumer started, waiting for messages...")
        message_count = 0
        
        for message in consumer:
            print(f"✓ Received: key={message.key}, value={message.value}, "
                  f"partition={message.partition}, offset={message.offset}")
            message_count += 1
            
            # Exit after receiving 10 messages or timeout
            if message_count >= 10:
                break
                
        print(f"✓ Consumer received {message_count} messages")
        
    except Exception as e:
        print(f"✗ Consumer error: {e}")
    finally:
        consumer.close()

def main():
    print("=== FluxMQ End-to-End Consumer Test ===")
    
    # Start consumer in background thread
    consumer_thread = threading.Thread(target=test_consumer)
    consumer_thread.daemon = True
    consumer_thread.start()
    
    # Give consumer time to start
    time.sleep(2)
    
    # Start producer
    test_producer()
    
    # Wait for consumer to finish
    consumer_thread.join(timeout=15)
    
    print("=== Test Complete ===")

if __name__ == "__main__":
    main()