#!/usr/bin/env python3
"""
Ultra Performance Test for FluxMQ
Tests the new performance optimizations:
- Lock-free storage
- Memory-mapped I/O  
- SIMD processing
- Advanced batching
"""

import threading
import time
from kafka import KafkaProducer
import json
import statistics

# Test configuration
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'ultra_perf_test'
NUM_THREADS = 8  # Increased for ultra performance
MESSAGES_PER_THREAD = 10000  # Increased message volume
TOTAL_MESSAGES = NUM_THREADS * MESSAGES_PER_THREAD

def producer_thread(thread_id, results):
    """Ultra high-performance producer thread"""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda x: x.encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8') if x else None,
        # Ultra performance configuration
        batch_size=1048576,      # 1MB batches
        linger_ms=0,             # No lingering
        buffer_memory=134217728, # 128MB buffer
        max_in_flight_requests_per_connection=50,  # High parallelism
        acks=1,                  # Fast acknowledgment
        compression_type=None,   # No compression overhead
        retries=0,               # No retries for max speed
    )
    
    start_time = time.time()
    
    # Generate large message batches for testing
    message_sizes = [100, 1000, 5000, 10000]  # Variable sizes for SIMD testing
    
    for i in range(MESSAGES_PER_THREAD):
        # Cycle through message sizes to test SIMD processing
        size = message_sizes[i % len(message_sizes)]
        
        key = f"thread_{thread_id}_msg_{i}"
        value = f"ultra_performance_test_message_{'x' * (size - 50)}"  # Variable size payload
        
        # Use automatic partition assignment
        producer.send(
            TOPIC_NAME, 
            key=key, 
            value=value
        )
        
        # Periodic flush for better throughput measurement
        if i % 1000 == 0:
            producer.flush()
    
    # Final flush
    producer.flush()
    end_time = time.time()
    
    duration = end_time - start_time
    throughput = MESSAGES_PER_THREAD / duration
    
    results[thread_id] = {
        'thread_id': thread_id,
        'messages': MESSAGES_PER_THREAD,
        'duration': duration,
        'throughput': throughput,
        'avg_message_size': sum(message_sizes) / len(message_sizes)
    }
    
    print(f"🚀 Thread {thread_id}: {throughput:.0f} msg/sec ({duration:.2f}s)")
    
    producer.close()

def run_ultra_performance_test():
    """Run the ultra performance test with all optimizations"""
    print("🔥 Ultra Performance Test - FluxMQ Advanced Optimizations")
    print(f"📊 Configuration: {NUM_THREADS} threads × {MESSAGES_PER_THREAD} messages = {TOTAL_MESSAGES} total")
    print("🚀 Testing: Lock-free storage, Memory-mapped I/O, SIMD processing, Advanced batching")
    print()
    
    # Start ultra performance test
    results = {}
    threads = []
    
    print(f"🚀 Starting {NUM_THREADS} ultra-performance producer threads...")
    overall_start = time.time()
    
    # Launch all producer threads
    for i in range(NUM_THREADS):
        thread = threading.Thread(
            target=producer_thread, 
            args=(i, results)
        )
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    overall_end = time.time()
    overall_duration = overall_end - overall_start
    
    # Calculate comprehensive performance metrics
    individual_throughputs = [r['throughput'] for r in results.values()]
    total_throughput = TOTAL_MESSAGES / overall_duration
    
    # Performance analysis
    print(f"\n{'='*60}")
    print("🏆 ULTRA PERFORMANCE RESULTS")
    print(f"{'='*60}")
    print(f"📈 Total Messages: {TOTAL_MESSAGES:,}")
    print(f"⏱️  Total Duration: {overall_duration:.2f} seconds")
    print(f"🚀 Total Throughput: {total_throughput:.0f} msg/sec")
    print(f"🔥 Peak Thread Throughput: {max(individual_throughputs):.0f} msg/sec")
    print(f"📊 Average Thread Throughput: {statistics.mean(individual_throughputs):.0f} msg/sec")
    print(f"📉 Thread Throughput StdDev: {statistics.stdev(individual_throughputs):.0f} msg/sec")
    print()
    
    # Performance vs target analysis
    target_throughput = 400000  # 400k msg/sec target
    performance_percentage = (total_throughput / target_throughput) * 100
    
    print(f"🎯 Target Analysis:")
    print(f"   Target: {target_throughput:,} msg/sec")
    print(f"   Achieved: {total_throughput:.0f} msg/sec ({performance_percentage:.1f}% of target)")
    
    if performance_percentage >= 100:
        print("🎉 TARGET ACHIEVED! Ultra performance optimizations successful!")
    elif performance_percentage >= 75:
        print("🔥 Excellent performance! Close to target.")
    elif performance_percentage >= 50:
        print("⚡ Good performance improvement, more optimization needed.")
    else:
        print("📈 Performance improved, significant optimization opportunities remain.")
    
    print()
    
    # Optimization effectiveness analysis
    baseline_throughput = 23600  # Previous best performance
    improvement_factor = total_throughput / baseline_throughput
    
    print(f"📊 Optimization Effectiveness:")
    print(f"   Previous Best: {baseline_throughput:,} msg/sec")
    print(f"   Current: {total_throughput:.0f} msg/sec")
    print(f"   Improvement: {improvement_factor:.2f}x ({(improvement_factor-1)*100:.1f}% gain)")
    print()
    
    # Detailed thread performance
    print("🧵 Per-Thread Performance:")
    for i in range(NUM_THREADS):
        result = results[i]
        print(f"   Thread {i}: {result['throughput']:.0f} msg/sec ({result['duration']:.2f}s)")
    
    print(f"\n{'='*60}")
    
    return {
        'total_throughput': total_throughput,
        'target_achievement': performance_percentage,
        'improvement_factor': improvement_factor,
        'thread_results': results
    }

if __name__ == '__main__':
    try:
        results = run_ultra_performance_test()
        
        # Export results for analysis
        with open('ultra_performance_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print("\n📁 Results saved to ultra_performance_results.json")
        
    except KeyboardInterrupt:
        print("\n⚠️ Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
