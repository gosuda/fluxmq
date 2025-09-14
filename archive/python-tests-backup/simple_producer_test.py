#!/usr/bin/env python3

"""
Simple Producer Test for FluxMQ
Direct producer test without admin client
"""

from kafka import KafkaProducer
import time
import json

def simple_producer_test():
    """간단한 Producer 테스트"""
    print("🚀 Simple Producer Test")
    print("=" * 50)
    
    try:
        # Producer 생성
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            client_id='simple-producer'
        )
        
        topic_name = 'test-topic'  # 이미 존재하는 토픽 사용
        num_messages = 1000
        
        print(f"Sending {num_messages} messages to topic: {topic_name}")
        
        start_time = time.time()
        
        # 메시지 전송
        for i in range(num_messages):
            message = {
                'id': i,
                'timestamp': time.time(),
                'data': f'test-message-{i}'
            }
            future = producer.send(topic_name, message)
            
            if i % 100 == 0 and i > 0:
                print(f"  Sent: {i} messages...")
        
        # 모든 메시지 전송 완료 대기
        producer.flush()
        producer.close()
        
        end_time = time.time()
        duration = end_time - start_time
        throughput = num_messages / duration
        
        print(f"\n✅ Results:")
        print(f"   Messages: {num_messages:,}")
        print(f"   Duration: {duration:.2f} seconds")
        print(f"   Throughput: {throughput:,.0f} msg/sec")
        
        return throughput
        
    except Exception as e:
        print(f"⚠️ Error: {e}")
        return 0

if __name__ == "__main__":
    simple_producer_test()
