#!/usr/bin/env python3
"""
Simple throughput benchmark for FluxMQ on port 9093
"""

import time
import threading
from kafka import KafkaProducer, KafkaConsumer
from concurrent.futures import ThreadPoolExecutor, as_completed
import statistics

def benchmark_producer(topic_name, messages_per_thread, thread_id):
    """Benchmark producer throughput in a single thread"""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9093'],
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
        bootstrap_servers=['localhost:9093'],
        api_version=(0, 10, 1),
        auto_offset_reset='earliest',
        consumer_timeout_ms=timeout_seconds * 1000,
        fetch_max_bytes=1024*1024,  # 1MB fetch size
        max_partition_fetch_bytes=1024*1024  # 1MB per partition
    )
    
    start_time = time.time()
    messages_consumed = 0
    
    try:
        for message in consumer:
            messages_consumed += 1
            if messages_consumed >= expected_messages:
                break
    except:
        pass
    
    end_time = time.time()
    duration = end_time - start_time
    throughput = messages_consumed / duration if duration > 0 else 0
    
    consumer.close()
    return throughput, messages_consumed, duration

def run_producer_benchmark():
    """Run multi-threaded producer benchmark"""
    print("\n🚀 Producer Throughput Benchmark")
    print("="*50)
    
    # Test with different thread counts
    thread_counts = [1, 2, 4, 8]
    messages_per_thread = 10000
    
    results = {}
    
    for num_threads in thread_counts:
        print(f"\nTesting with {num_threads} producer threads...")
        topic_name = f"benchmark-topic-{num_threads}-{int(time.time())}"
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for thread_id in range(num_threads):
                future = executor.submit(
                    benchmark_producer, 
                    topic_name, 
                    messages_per_thread, 
                    thread_id
                )
                futures.append(future)
            
            thread_throughputs = []
            total_messages = 0
            total_duration = 0
            
            for future in as_completed(futures):
                throughput, msg_count, duration = future.result()
                thread_throughputs.append(throughput)
                total_messages += msg_count
                total_duration = max(total_duration, duration)
            
            aggregate_throughput = total_messages / total_duration
            avg_thread_throughput = statistics.mean(thread_throughputs)
            
            results[num_threads] = {
                'aggregate': aggregate_throughput,
                'average_per_thread': avg_thread_throughput,
                'total_messages': total_messages,
                'duration': total_duration
            }
            
            print(f"  ✅ Aggregate: {aggregate_throughput:,.0f} msg/sec")
            print(f"  ✅ Per thread avg: {avg_thread_throughput:,.0f} msg/sec")
            print(f"  ✅ Total messages: {total_messages:,}")
            print(f"  ✅ Duration: {total_duration:.2f}s")
    
    return results, topic_name

def run_consumer_benchmark(topic_name, expected_messages):
    """Run consumer benchmark"""
    print("\n📥 Consumer Throughput Benchmark")
    print("="*50)
    
    throughput, messages, duration = benchmark_consumer(topic_name, expected_messages)
    
    print(f"  ✅ Throughput: {throughput:,.0f} msg/sec")
    print(f"  ✅ Messages consumed: {messages:,}")
    print(f"  ✅ Duration: {duration:.2f}s")
    
    return throughput

def main():
    print("\n🔥 FluxMQ Performance Benchmark")
    print("="*50)
    print("Target: Optimize from 400k+ to 1M+ msg/sec")
    
    # Run producer benchmark
    producer_results, topic_name = run_producer_benchmark()
    
    # Find best producer result
    best_threads = max(producer_results.keys(), 
                      key=lambda k: producer_results[k]['aggregate'])
    best_throughput = producer_results[best_threads]['aggregate']
    
    # Run consumer benchmark on the topic with most messages
    total_messages = producer_results[best_threads]['total_messages']
    consumer_throughput = run_consumer_benchmark(topic_name, total_messages)
    
    # Summary
    print("\n📊 Benchmark Summary")
    print("="*50)
    print(f"🏆 Best Producer Throughput: {best_throughput:,.0f} msg/sec ({best_threads} threads)")
    print(f"🏆 Consumer Throughput: {consumer_throughput:,.0f} msg/sec")
    
    # Performance rating
    if best_throughput >= 1000000:
        print("\n🌟 ULTRA PERFORMANCE: 1M+ msg/sec achieved!")
    elif best_throughput >= 400000:
        print("\n⚡ HIGH PERFORMANCE: 400k+ msg/sec achieved!")
    elif best_throughput >= 100000:
        print("\n🚀 GOOD PERFORMANCE: 100k+ msg/sec")
    elif best_throughput >= 50000:
        print("\n✅ MODERATE PERFORMANCE: 50k+ msg/sec")
    else:
        print(f"\n⚠️  NEEDS OPTIMIZATION: {best_throughput:,.0f} msg/sec")
        print("   Target: 400k+ msg/sec (current gap: {:.0f}x)".format(400000/best_throughput))

if __name__ == "__main__":
    main()