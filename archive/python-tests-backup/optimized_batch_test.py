#!/usr/bin/env python3

"""
Optimized Batch Producer Test for FluxMQ
Aggressive batching configuration to achieve true batch processing
"""

from kafka import KafkaProducer
import time
import json

def optimized_batch_test():
    """최적화된 배치 Producer 테스트 - 49k+ msg/sec 목표"""
    print("🚀 Optimized Batch Producer Test - Target: 49k+ msg/sec")
    print("=" * 60)
    
    try:
        # Producer 생성 - 극도로 공격적인 배치 설정
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            client_id='optimized-batch-producer',
            
            # 🚀 극도의 배치 최적화 설정
            batch_size=65536,         # 64KB 배치 (기본값의 4배)
            linger_ms=100,           # 100ms 대기 (기본값의 10배) - 배치 누적 시간
            buffer_memory=134217728, # 128MB 버퍼 (기본값의 4배)
            max_request_size=2097152,# 2MB 최대 요청 (기본값의 2배)
            
            # 🔥 성능 극대화 설정
            acks=1,                  # Leader만 확인 (all=모든 replica 대기 vs 1=leader만)
            retries=1,               # 재시도 최소화
            max_in_flight_requests_per_connection=10,  # 병렬 요청 증가
            
            # 📊 압축 및 네트워크 최적화
            # compression_type=None,   # 압축 비활성화로 CPU 절약
            
            # 🎯 배치 강제 설정
            send_buffer_bytes=262144,    # 256KB 송신 버퍼
            receive_buffer_bytes=262144, # 256KB 수신 버퍼
        )
        
        topic_name = 'ultra-batch-topic'
        num_messages = 20000  # 메시지 수 증가
        
        print(f"Sending {num_messages:,} messages to topic: {topic_name}")
        print(f"Batch Configuration:")
        print(f"  - Batch size: 64KB")
        print(f"  - Linger time: 100ms (배치 누적 대기)")
        print(f"  - Buffer memory: 128MB")
        print(f"  - Max request: 2MB")
        print(f"  - In-flight requests: 10")
        
        start_time = time.time()
        
        # 🚀 대량 메시지 전송 - 배치 누적을 위해 빠르게 전송
        print("Phase 1: Rapid message queuing for batch accumulation...")
        for i in range(num_messages):
            message = {
                'id': i,
                'timestamp': time.time(),
                'batch_test': True,
                'data': f'ultra-batch-msg-{i:06d}',
                'payload': 'x' * 200  # 200바이트 페이로드 (현실적인 크기)
            }
            
            # 비동기 전송 - linger_ms 동안 배치에 누적됨
            future = producer.send(topic_name, message)
            
            if i % 2000 == 0 and i > 0:
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                print(f"  Queued: {i:,} messages... ({rate:,.0f} msg/sec so far)")
        
        queued_time = time.time()
        print(f"Phase 2: Force flushing all batches...")
        
        # 🔥 강제로 모든 배치 전송 및 완료 대기
        producer.flush()
        
        flushed_time = time.time()
        producer.close()
        
        end_time = time.time()
        
        # 📊 상세한 성능 분석
        total_duration = end_time - start_time
        queue_duration = queued_time - start_time
        flush_duration = flushed_time - queued_time
        
        total_throughput = num_messages / total_duration
        queue_throughput = num_messages / queue_duration if queue_duration > 0 else float('inf')
        
        print(f"\n🎯 Optimized Batch Results:")
        print(f"   Messages: {num_messages:,}")
        print(f"   Total duration: {total_duration:.3f} seconds")
        print(f"   Queue phase: {queue_duration:.3f} seconds ({queue_throughput:,.0f} msg/sec)")
        print(f"   Flush phase: {flush_duration:.3f} seconds")
        print(f"   Overall throughput: {total_throughput:,.0f} msg/sec")
        
        # 🎉 성과 분석
        if total_throughput >= 49000:
            print(f"🎉 SUCCESS! Achieved Phase 1 target: {total_throughput:,.0f} msg/sec!")
            if total_throughput >= 100000:
                print(f"🚀 ULTRA SUCCESS! Approaching Phase 2 levels!")
        elif total_throughput >= 30000:
            print(f"🔥 EXCELLENT! {total_throughput:,.0f} msg/sec - Very close to target!")
        elif total_throughput >= 15000:
            print(f"✅ GOOD PROGRESS! {total_throughput:,.0f} msg/sec - 2x improvement!")
        else:
            print(f"📊 BASELINE: {total_throughput:,.0f} msg/sec - Need more optimization")
        
        # 배치 효율성 분석
        estimated_batches = max(1, int(total_duration / 0.1))  # 100ms linger 기준
        avg_batch_size = num_messages / estimated_batches if estimated_batches > 0 else 0
        print(f"   Estimated batches: {estimated_batches}")
        print(f"   Avg messages per batch: {avg_batch_size:.1f}")
        
        return total_throughput
        
    except Exception as e:
        print(f"⚠️ Error: {e}")
        return 0

if __name__ == "__main__":
    optimized_batch_test()