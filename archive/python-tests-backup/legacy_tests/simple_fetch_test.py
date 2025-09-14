#!/usr/bin/env python3
"""
Simple Kafka Fetch test to verify direct message consumption without consumer groups
"""

from kafka import KafkaProducer, KafkaConsumer
import json
import time

def test_simple_fetch():
    print("🔍 Simple Fetch Test (No Consumer Groups)")
    
    try:
        # First produce a message
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=3,
        )
        
        # Send test message
        message = {"test": "Direct fetch working", "timestamp": time.time()}
        future = producer.send('fetch-test', key='test-key', value=message)
        record_metadata = future.get(timeout=10)
        
        print(f"✅ Message sent: topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}")
        
        producer.flush()
        producer.close()
        
        # Now try to fetch directly using manual assignment
        print("🔄 Attempting manual partition assignment fetch...")
        
        consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: m.decode('utf-8') if m else None,
            auto_offset_reset='earliest',
            enable_auto_commit=False,  # Disable consumer groups
            consumer_timeout_ms=5000
        )
        
        # Manually assign partitions (no consumer group)
        from kafka import TopicPartition
        partition = TopicPartition('fetch-test', 0)
        consumer.assign([partition])
        print("✅ Partition manually assigned")
        
        # Seek to beginning
        consumer.seek_to_beginning()
        print("✅ Seeking to beginning")
        
        message_count = 0
        for message in consumer:
            message_count += 1
            print(f"📥 Received: {message.value}")
            print(f"   Key: {message.key}, Partition: {message.partition}, Offset: {message.offset}")
            break  # Just get one message
        
        consumer.close()
        
        if message_count > 0:
            print("✅ Manual assignment fetch PASSED!")
            return True
        else:
            print("⚠️ No messages received with manual assignment")
            return False
        
    except Exception as e:
        print(f"❌ Direct fetch error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("📊 Simple FluxMQ Fetch API Test")
    print("=" * 40)
    
    success = test_simple_fetch()
    
    print("\n" + "=" * 40)
    if success:
        print("✅ Direct fetch test PASSED!")
    else:
        print("❌ Direct fetch test FAILED!")

if __name__ == "__main__":
    main()