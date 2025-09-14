#!/usr/bin/env python3

"""
Quick Performance Test for FluxMQ
간단한 Producer/Consumer 성능 테스트
"""

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import time
import threading
import json

def create_test_topic():
    """테스트 토픽 생성"""
    admin_client = KafkaAdminClient(
        bootstrap_servers=['localhost:9092'],
        client_id='perf-test-admin'
    )
    
    topic_list = [NewTopic(name="perf-test-topic", num_partitions=1, replication_factor=1)]
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print("✅ Created topic: perf-test-topic")
    except TopicAlreadyExistsError:
        print("✅ Topic already exists: perf-test-topic")
    except Exception as e:
        print(f"⚠️ Topic creation error: {e}")
    
    admin_client.close()

def producer_test(num_messages=10000):
    """Producer 성능 테스트"""
    print(f"\n🚀 Producer Test: {num_messages:,} messages")
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        client_id='perf-test-producer'
    )
    
    start_time = time.time()
    
    for i in range(num_messages):
        message = {
            'id': i,
            'timestamp': time.time(),
            'data': f'test-message-{i}'
        }
        producer.send('perf-test-topic', message)
        
        if i % 1000 == 0 and i > 0:
            print(f"  Sent: {i:,} messages...")
    
    producer.flush()
    producer.close()
    
    end_time = time.time()
    duration = end_time - start_time
    throughput = num_messages / duration
    
    print(f"✅ Producer Results:")
    print(f"   Messages: {num_messages:,}")
    print(f"   Duration: {duration:.2f} seconds")
    print(f"   Throughput: {throughput:,.0f} msg/sec")
    
    return throughput

def consumer_test(num_messages=10000):
    """Consumer 성능 테스트"""
    print(f"\n📥 Consumer Test: Reading {num_messages:,} messages")
    
    consumer = KafkaConsumer(
        'perf-test-topic',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        client_id='perf-test-consumer',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=10000  # 10초 타임아웃
    )
    
    start_time = time.time()
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            
            if message_count % 1000 == 0:
                print(f"  Consumed: {message_count:,} messages...")
            
            if message_count >= num_messages:
                break
    except Exception as e:
        print(f"⚠️ Consumer error: {e}")
    
    consumer.close()
    
    end_time = time.time()
    duration = end_time - start_time
    
    if message_count > 0:
        throughput = message_count / duration
        print(f"✅ Consumer Results:")
        print(f"   Messages: {message_count:,}")
        print(f"   Duration: {duration:.2f} seconds")  
        print(f"   Throughput: {throughput:,.0f} msg/sec")
    else:
        print("⚠️ No messages consumed")
        throughput = 0
    
    return throughput, message_count

def main():
    """메인 성능 테스트"""
    print("🔥 FluxMQ Performance Test")
    print("=" * 50)
    
    # 토픽 생성
    create_test_topic()
    time.sleep(1)  # 토픽 생성 대기
    
    # Producer 테스트
    num_messages = 5000  # 작은 수로 시작
    producer_throughput = producer_test(num_messages)
    
    time.sleep(2)  # 메시지 처리 대기
    
    # Consumer 테스트
    consumer_throughput, consumed_count = consumer_test(num_messages)
    
    # 결과 요약
    print("\n📊 Performance Summary")
    print("=" * 50)
    print(f"Producer Throughput: {producer_throughput:,.0f} msg/sec")
    print(f"Consumer Throughput: {consumer_throughput:,.0f} msg/sec")
    print(f"Messages Produced: {num_messages:,}")
    print(f"Messages Consumed: {consumed_count:,}")
    
    if consumed_count == num_messages:
        print("✅ All messages successfully produced and consumed!")
    else:
        print(f"⚠️ Message mismatch: {num_messages - consumed_count} messages lost")

if __name__ == "__main__":
    main()
