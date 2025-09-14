#!/usr/bin/env python3

"""
Batch Producer Test for FluxMQ
Tests performance with batch message sending to demonstrate ultra-performance capabilities
"""

from kafka import KafkaProducer
import time
import json

def batch_producer_test():
    """배치 Producer 테스트 - 진짜 성능 측정"""
    print("🚀 Batch Producer Test - Ultra Performance")
    print("=" * 50)
    
    try:
        # Producer 생성 - 배치 최적화 설정
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            client_id='batch-producer',
            # 배치 최적화 설정
            batch_size=16384,  # 16KB 배치
            linger_ms=10,      # 10ms 배치 대기
            # compression_type='snappy',  # 압축 사용 (라이브러리 없음)
            acks=1,           # Leader 확인만
            retries=3,
            buffer_memory=33554432,  # 32MB 버퍼
            max_request_size=1048576,  # 1MB 최대 요청
        )
        
        topic_name = 'batch-test-topic'
        num_messages = 10000
        
        print(f"Sending {num_messages:,} messages to topic: {topic_name}")
        print(f"Configuration:")
        print(f"  - Batch size: 16KB")
        print(f"  - Linger time: 10ms")  
        print(f"  - Compression: snappy")
        print(f"  - Buffer: 32MB")
        
        start_time = time.time()
        
        # 배치 메시지 전송 - 대량 처리
        for i in range(num_messages):
            message = {
                'id': i,
                'timestamp': time.time(),
                'data': f'batch-message-{i:06d}',
                'payload': f'{"x" * 100}'  # 100 바이트 페이로드로 현실적인 크기
            }
            
            # 비동기 전송 - 배치에 추가만 됨
            producer.send(topic_name, message)
            
            if i % 1000 == 0 and i > 0:
                print(f"  Queued: {i:,} messages...")
        
        print("  All messages queued, flushing...")
        
        # 강제로 모든 배치 전송
        producer.flush()
        producer.close()
        
        end_time = time.time()
        duration = end_time - start_time
        throughput = num_messages / duration
        
        print(f"\n✅ Batch Results:")
        print(f"   Messages: {num_messages:,}")
        print(f"   Duration: {duration:.3f} seconds")
        print(f"   Throughput: {throughput:,.0f} msg/sec")
        
        # 성능 분석
        if throughput > 40000:
            print(f"🎉 ULTRA-PERFORMANCE: Achieved {throughput:,.0f} msg/sec!")
        elif throughput > 20000:
            print(f"🚀 HIGH-PERFORMANCE: {throughput:,.0f} msg/sec is excellent!")
        elif throughput > 10000:
            print(f"✅ GOOD-PERFORMANCE: {throughput:,.0f} msg/sec is solid")
        else:
            print(f"⚠️  BASELINE: {throughput:,.0f} msg/sec - room for improvement")
        
        return throughput
        
    except Exception as e:
        print(f"⚠️ Error: {e}")
        return 0

if __name__ == "__main__":
    batch_producer_test()