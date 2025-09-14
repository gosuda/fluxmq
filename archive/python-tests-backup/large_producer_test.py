#!/usr/bin/env python3

"""
Large Scale Producer Test for FluxMQ
대규모 성능 테스트
"""

from kafka import KafkaProducer
import time
import json
import threading
from concurrent.futures import ThreadPoolExecutor

def producer_worker(thread_id, messages_per_thread):
    """개별 Producer 워커"""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        client_id=f'producer-{thread_id}',
        batch_size=16384,  # 배치 크기 증가
        linger_ms=1,       # 배치 대기 시간
        acks=1            # 빠른 응답을 위해 acks=1
    )
    
    topic_name = 'test-topic'  # 기존 토픽 사용
    
    start_time = time.time()
    
    for i in range(messages_per_thread):
        message = {
            'thread_id': thread_id,
            'message_id': i,
            'timestamp': time.time(),
            'data': f'thread-{thread_id}-message-{i}'
        }
        producer.send(topic_name, message)
    
    producer.flush()
    producer.close()
    
    end_time = time.time()
    duration = end_time - start_time
    throughput = messages_per_thread / duration
    
    return thread_id, messages_per_thread, duration, throughput

def large_scale_test():
    """대규모 성능 테스트"""
    print("🚀 Large Scale Producer Test")
    print("=" * 60)
    
    # 테스트 설정
    num_threads = 4        # Producer 스레드 수
    messages_per_thread = 5000  # 스레드당 메시지 수
    total_messages = num_threads * messages_per_thread
    
    print(f"Configuration:")
    print(f"  Producer threads: {num_threads}")
    print(f"  Messages per thread: {messages_per_thread:,}")
    print(f"  Total messages: {total_messages:,}")
    print(f"  Target: >400k msg/sec")
    print()
    
    start_time = time.time()
    
    # 멀티스레드 Producer 실행
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for thread_id in range(num_threads):
            future = executor.submit(producer_worker, thread_id, messages_per_thread)
            futures.append(future)
        
        print("Running producer threads...")
        results = []
        for future in futures:
            thread_id, messages, duration, throughput = future.result()
            results.append((thread_id, messages, duration, throughput))
            print(f"  Thread {thread_id}: {messages:,} msgs in {duration:.2f}s = {throughput:,.0f} msg/sec")
    
    end_time = time.time()
    total_duration = end_time - start_time
    total_throughput = total_messages / total_duration
    
    # 결과 출력
    print("\n📊 Results Summary")
    print("=" * 60)
    print(f"Total Messages: {total_messages:,}")
    print(f"Total Duration: {total_duration:.2f} seconds")
    print(f"Overall Throughput: {total_throughput:,.0f} msg/sec")
    
    # 개별 스레드 통계
    thread_throughputs = [result[3] for result in results]
    avg_thread_throughput = sum(thread_throughputs) / len(thread_throughputs)
    max_thread_throughput = max(thread_throughputs)
    min_thread_throughput = min(thread_throughputs)
    
    print(f"\nPer-Thread Statistics:")
    print(f"  Average: {avg_thread_throughput:,.0f} msg/sec")
    print(f"  Maximum: {max_thread_throughput:,.0f} msg/sec")
    print(f"  Minimum: {min_thread_throughput:,.0f} msg/sec")
    
    # 성능 평가
    print(f"\n🎯 Performance Assessment:")
    if total_throughput > 400000:
        print(f"  ✅ EXCELLENT: {total_throughput:,.0f} msg/sec > 400k target!")
    elif total_throughput > 100000:
        print(f"  ✅ GOOD: {total_throughput:,.0f} msg/sec > 100k")
    elif total_throughput > 50000:
        print(f"  🔶 FAIR: {total_throughput:,.0f} msg/sec > 50k")
    else:
        print(f"  ⚠️ NEEDS OPTIMIZATION: {total_throughput:,.0f} msg/sec")
    
    return total_throughput

if __name__ == "__main__":
    large_scale_test()
