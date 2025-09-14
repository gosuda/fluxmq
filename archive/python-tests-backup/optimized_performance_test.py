#!/usr/bin/env python3
"""
FluxMQ Ultra Performance Test
Tests the new optimizations: Lock-free storage, Memory-mapped I/O, SIMD processing
"""

import threading
import time
from kafka import KafkaProducer
import json
import statistics

# Test configuration
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'ultra_test'
NUM_THREADS = 4  
MESSAGES_PER_THREAD = 5000  
TOTAL_MESSAGES = NUM_THREADS * MESSAGES_PER_THREAD

def producer_thread(thread_id, results):
    """High-performance producer thread"""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda x: x.encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8') if x else None,
        batch_size=16384,      # 16KB batches
        linger_ms=1,           # Small linger
        buffer_memory=33554432, # 32MB buffer
        max_in_flight_requests_per_connection=5,
        acks=1,
        retries=3,
    )
    
    start_time = time.time()
    
    # Variable message sizes for SIMD testing
    message_sizes = [50, 200, 500, 1000]
    
    for i in range(MESSAGES_PER_THREAD):
        size = message_sizes[i % len(message_sizes)]
        
        key = f"t{thread_id}_m{i}"
        value = f"ultra_perf_msg_{'A' * (size - 20)}"
        
        producer.send(TOPIC_NAME, key=key, value=value)
        
        if i % 1000 == 0:
            producer.flush()
    
    producer.flush()
    end_time = time.time()
    
    duration = end_time - start_time
    throughput = MESSAGES_PER_THREAD / duration
    
    results[thread_id] = {
        'messages': MESSAGES_PER_THREAD,
        'duration': duration,
        'throughput': throughput
    }
    
    print(f"✅ Thread {thread_id}: {throughput:.0f} msg/sec ({duration:.2f}s)")
    producer.close()

def run_performance_test():
    """Run the ultra performance test"""
    print("🚀 FluxMQ Ultra Performance Test")
    print(f"📊 Testing: {NUM_THREADS} threads × {MESSAGES_PER_THREAD} messages = {TOTAL_MESSAGES} total")
    print("🔥 New optimizations: Lock-free + Memory-mapped + SIMD + Zero-copy")
    print()
    
    results = {}
    threads = []
    
    print(f"🚀 Starting {NUM_THREADS} producer threads...")
    overall_start = time.time()
    
    # Launch producer threads
    for i in range(NUM_THREADS):
        thread = threading.Thread(target=producer_thread, args=(i, results))
        threads.append(thread)
        thread.start()
    
    # Wait for completion
    for thread in threads:
        thread.join()
    
    overall_end = time.time()
    overall_duration = overall_end - overall_start
    
    # Calculate metrics
    individual_throughputs = [r['throughput'] for r in results.values()]
    total_throughput = TOTAL_MESSAGES / overall_duration
    
    # Results
    print(f"\n{'='*50}")
    print("🏆 ULTRA PERFORMANCE RESULTS")
    print(f"{'='*50}")
    print(f"📈 Total Messages: {TOTAL_MESSAGES:,}")
    print(f"⏱️  Total Duration: {overall_duration:.2f} seconds")
    print(f"🚀 Total Throughput: {total_throughput:.0f} msg/sec")
    print(f"📊 Peak Thread: {max(individual_throughputs):.0f} msg/sec")
    print(f"📊 Average Thread: {statistics.mean(individual_throughputs):.0f} msg/sec")
    print()
    
    # Comparison with previous performance
    baseline_performance = 23600
    improvement = total_throughput / baseline_performance
    
    print(f"📊 Performance Analysis:")
    print(f"   Previous Best: {baseline_performance:,} msg/sec")
    print(f"   Current: {total_throughput:.0f} msg/sec")
    print(f"   Improvement: {improvement:.2f}x ({(improvement-1)*100:.1f}% gain)")
    
    # Target analysis
    target = 400000
    target_progress = (total_throughput / target) * 100
    print(f"   Target Progress: {target_progress:.1f}% of 400k msg/sec goal")
    
    if improvement > 1.5:
        print("🎉 Significant improvement achieved!")
    elif improvement > 1.1:
        print("✅ Good performance improvement")
    else:
        print("📈 Marginal improvement, more optimization needed")
    
    print(f"\n{'='*50}")
    
    return {
        'total_throughput': total_throughput,
        'improvement_factor': improvement,
        'target_progress': target_progress
    }

if __name__ == '__main__':
    try:
        results = run_performance_test()
        
        # Save results
        with open('ultra_perf_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print("\n📁 Results saved to ultra_perf_results.json")
        
    except KeyboardInterrupt:
        print("\n⚠️ Test interrupted")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
