#!/usr/bin/env python3
"""
Simple throughput benchmark for FluxMQ
"""

import time
import threading
from kafka import KafkaProducer, KafkaConsumer
from concurrent.futures import ThreadPoolExecutor, as_completed
import statistics

def benchmark_producer(topic_name, messages_per_thread, thread_id):
    """Benchmark producer throughput in a single thread"""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 1),
        value_serializer=lambda x: x.encode('utf-8'),
        batch_size=16384,  # 16KB batch size for better throughput
        linger_ms=5,       # Wait 5ms for batching
        compression_type=None  # No compression for pure throughput test
    )
    
    start_time = time.time()
    
    for i in range(messages_per_thread):
        message = f"benchmark-message-{thread_id}-{i}"
        producer.send(topic_name, message)
    
    producer.flush()
    end_time = time.time()
    
    duration = end_time - start_time
    throughput = messages_per_thread / duration
    
    producer.close()
    return throughput, messages_per_thread, duration

def benchmark_consumer(topic_name, expected_messages, timeout_seconds=30):
    """Benchmark consumer throughput"""
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 1),
        auto_offset_reset='earliest',
        consumer_timeout_ms=timeout_seconds * 1000,
        fetch_max_bytes=1024*1024,  # 1MB fetch size
        max_partition_fetch_bytes=1024*1024  # 1MB per partition
    )
    
    start_time = time.time()
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            if message_count >= expected_messages:
                break
    except Exception as e:
        print(f"Consumer stopped: {e}")
    
    end_time = time.time()
    duration = end_time - start_time
    throughput = message_count / duration if duration > 0 else 0
    
    consumer.close()
    return throughput, message_count, duration

def run_producer_benchmark():
    """Run multi-threaded producer benchmark"""
    print("\n🚀 Producer Throughput Benchmark")
    print("=" * 50)
    
    topic_name = f"benchmark-{int(time.time())}"
    threads = [1, 2, 4, 8]  # Test different thread counts
    messages_per_thread = 10000  # 10k messages per thread
    
    results = {}
    
    for thread_count in threads:
        print(f"\nTesting with {thread_count} producer threads...")
        
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            futures = []
            
            for thread_id in range(thread_count):
                future = executor.submit(
                    benchmark_producer, 
                    topic_name, 
                    messages_per_thread, 
                    thread_id
                )
                futures.append(future)
            
            thread_results = []
            total_messages = 0
            total_duration = 0
            
            for future in as_completed(futures):
                throughput, msg_count, duration = future.result()
                thread_results.append(throughput)
                total_messages += msg_count
                total_duration = max(total_duration, duration)
            
            # Calculate aggregate throughput
            aggregate_throughput = total_messages / total_duration
            avg_thread_throughput = statistics.mean(thread_results)
            
            results[thread_count] = {
                'total_messages': total_messages,
                'duration': total_duration,
                'aggregate_throughput': aggregate_throughput,
                'avg_thread_throughput': avg_thread_throughput,
                'thread_results': thread_results
            }
            
            print(f"  Total messages: {total_messages:,}")
            print(f"  Duration: {total_duration:.2f}s")
            print(f"  Aggregate throughput: {aggregate_throughput:,.0f} msg/sec")
            print(f"  Average per thread: {avg_thread_throughput:,.0f} msg/sec")
    
    return results, topic_name

def run_consumer_benchmark(topic_name, expected_messages):
    """Run consumer throughput benchmark"""
    print(f"\n📥 Consumer Throughput Benchmark")
    print("=" * 50)
    
    print(f"Consuming {expected_messages:,} messages...")
    
    throughput, consumed, duration = benchmark_consumer(topic_name, expected_messages)
    
    print(f"  Consumed messages: {consumed:,}")
    print(f"  Duration: {duration:.2f}s")
    print(f"  Consumer throughput: {throughput:,.0f} msg/sec")
    
    return throughput, consumed, duration

def main():
    print("🔥 FluxMQ Performance Benchmark")
    print("=" * 50)
    print("Target: Optimize from 400k+ to 1M+ msg/sec")
    
    # Run producer benchmark
    producer_results, topic_name = run_producer_benchmark()
    
    # Wait a bit for messages to be fully written
    time.sleep(2)
    
    # Get total messages for consumer test
    max_thread_count = max(producer_results.keys())
    total_messages = producer_results[max_thread_count]['total_messages']
    
    # Run consumer benchmark
    consumer_throughput, consumed, duration = run_consumer_benchmark(topic_name, total_messages)
    
    # Summary
    print(f"\n📊 Performance Summary")
    print("=" * 50)
    
    best_producer_result = max(producer_results.values(), key=lambda x: x['aggregate_throughput'])
    best_threads = [k for k, v in producer_results.items() if v == best_producer_result][0]
    
    print(f"Best Producer Performance:")
    print(f"  Threads: {best_threads}")
    print(f"  Throughput: {best_producer_result['aggregate_throughput']:,.0f} msg/sec")
    
    print(f"\nConsumer Performance:")
    print(f"  Throughput: {consumer_throughput:,.0f} msg/sec")
    
    # Performance analysis
    current_max = best_producer_result['aggregate_throughput']
    target = 1_000_000  # 1M msg/sec target
    
    if current_max >= target:
        print(f"\n🎉 TARGET ACHIEVED! Current: {current_max:,.0f} msg/sec >= Target: {target:,.0f} msg/sec")
    else:
        improvement_needed = target / current_max
        print(f"\n🎯 OPTIMIZATION NEEDED:")
        print(f"  Current: {current_max:,.0f} msg/sec")
        print(f"  Target: {target:,.0f} msg/sec") 
        print(f"  Improvement needed: {improvement_needed:.1f}x")
    
    return producer_results, consumer_throughput

if __name__ == "__main__":
    main()