#!/usr/bin/env python3
"""
Quick performance analysis and bottleneck identification
"""

import time
import threading
from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor, as_completed

def producer_thread_test(thread_id, messages_per_thread=5000):
    """Test producer performance in single thread"""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 1),
        value_serializer=lambda x: x.encode('utf-8'),
        batch_size=16384,
        linger_ms=1,  # Reduced for speed
        acks=1  # Faster acknowledgment
    )
    
    topic_name = f"perf-test-{thread_id}"
    start_time = time.time()
    
    for i in range(messages_per_thread):
        producer.send(topic_name, f"msg-{thread_id}-{i}")
    
    producer.flush()
    end_time = time.time()
    producer.close()
    
    duration = end_time - start_time
    throughput = messages_per_thread / duration
    return throughput, duration, messages_per_thread

def run_performance_analysis():
    """Run quick performance analysis"""
    print("🔥 FluxMQ Performance Analysis")
    print("=" * 50)
    
    # Test configurations: [threads, messages_per_thread]
    configs = [
        (1, 5000),   # Single thread baseline
        (2, 5000),   # Dual thread  
        (4, 5000),   # Quad thread
        (8, 5000),   # Octa thread
    ]
    
    results = {}
    
    for threads, msgs_per_thread in configs:
        print(f"\n📊 Testing {threads} threads × {msgs_per_thread} msgs/thread")
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(producer_thread_test, i, msgs_per_thread) 
                      for i in range(threads)]
            
            thread_results = []
            total_messages = 0
            
            for future in as_completed(futures):
                throughput, duration, msg_count = future.result()
                thread_results.append(throughput)
                total_messages += msg_count
        
        end_time = time.time()
        total_duration = end_time - start_time
        aggregate_throughput = total_messages / total_duration
        avg_thread_throughput = sum(thread_results) / len(thread_results)
        
        results[threads] = {
            'total_messages': total_messages,
            'duration': total_duration,
            'aggregate_throughput': aggregate_throughput,
            'avg_per_thread': avg_thread_throughput,
            'efficiency': (aggregate_throughput / (threads * 7220)) * 100  # vs single baseline
        }
        
        print(f"  Total: {total_messages:,} msgs in {total_duration:.2f}s")
        print(f"  Aggregate: {aggregate_throughput:,.0f} msg/sec")
        print(f"  Per thread: {avg_thread_throughput:,.0f} msg/sec")
        print(f"  Efficiency: {results[threads]['efficiency']:.1f}% of ideal scaling")
    
    # Analysis
    print(f"\n🎯 Performance Analysis Summary")
    print("=" * 50)
    
    best_config = max(results.keys(), key=lambda k: results[k]['aggregate_throughput'])
    best_perf = results[best_config]['aggregate_throughput']
    
    print(f"Best Configuration: {best_config} threads")
    print(f"Peak Performance: {best_perf:,.0f} msg/sec") 
    print(f"Target (400k): {(best_perf/400_000)*100:.1f}% achieved")
    
    # Bottleneck analysis
    single_thread = results[1]['aggregate_throughput']
    scaling_efficiency = {}
    
    for threads in results.keys():
        if threads > 1:
            expected = single_thread * threads
            actual = results[threads]['aggregate_throughput']
            efficiency = (actual / expected) * 100
            scaling_efficiency[threads] = efficiency
            print(f"{threads} threads: {efficiency:.1f}% scaling efficiency")
    
    # Recommendations
    print(f"\n💡 Optimization Recommendations:")
    avg_efficiency = sum(scaling_efficiency.values()) / len(scaling_efficiency)
    
    if avg_efficiency > 80:
        print("✅ Good thread scaling - focus on single-thread optimizations")
        print("  → Protocol parsing optimization")
        print("  → Zero-copy message handling")
        print("  → Memory pool optimizations")
    elif avg_efficiency > 60:
        print("⚠️  Moderate thread scaling - mixed optimizations needed")
        print("  → Reduce lock contention") 
        print("  → Async I/O optimizations")
        print("  → Protocol batch processing")
    else:
        print("🚨 Poor thread scaling - focus on concurrency")
        print("  → Lock-free data structures")
        print("  → Thread-local storage")
        print("  → Connection pooling")
    
    return results

if __name__ == "__main__":
    results = run_performance_analysis()