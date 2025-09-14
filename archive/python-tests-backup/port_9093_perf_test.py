#!/usr/bin/env python3
import time
import threading
from kafka import KafkaProducer
import json

def producer_thread(thread_id, messages_per_thread, results):
    start_time = time.time()
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9093'],  # Use optimized port
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_block_ms=30000
        )
        
        messages_sent = 0
        for i in range(messages_per_thread):
            message = {"thread": thread_id, "message": i, "timestamp": time.time()}
            producer.send(f'ultra-perf-topic', message)
            messages_sent += 1
            
        producer.flush()
        duration = time.time() - start_time
        throughput = messages_sent / duration if duration > 0 else 0
        results[thread_id] = (messages_sent, duration, throughput)
        
        print(f"✅ Thread-{thread_id}: {messages_sent} messages in {duration:.2f}s = {throughput:.0f} msg/sec")
        producer.close()
        
    except Exception as e:
        print(f"❌ Thread-{thread_id} error: {e}")
        results[thread_id] = (0, 0, 0)

def run_performance_test():
    print("🚀 FluxMQ Ultra Performance Test (Port 9093)")
    
    num_threads = 4
    messages_per_thread = 5000
    total_messages = num_threads * messages_per_thread
    
    print(f"📊 Testing: {num_threads} threads × {messages_per_thread} messages = {total_messages} total")
    print("🔥 New optimizations: Lock-free + Memory-mapped + SIMD + Zero-copy")
    print()
    
    results = {}
    threads = []
    
    print(f"🚀 Starting {num_threads} producer threads...")
    print()
    
    test_start = time.time()
    
    for i in range(num_threads):
        t = threading.Thread(target=producer_thread, args=(i+1, messages_per_thread, results))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
    
    test_duration = time.time() - test_start
    
    # Aggregate results
    total_sent = sum(r[0] for r in results.values())
    individual_throughputs = [r[2] for r in results.values() if r[2] > 0]
    
    total_throughput = total_sent / test_duration if test_duration > 0 else 0
    
    print()
    print("=" * 50)
    print("🏆 ULTRA PERFORMANCE RESULTS")
    print("=" * 50)
    print(f"📈 Total Messages: {total_sent:,}")
    print(f"⏱️  Total Duration: {test_duration:.2f} seconds")
    print(f"🚀 Total Throughput: {total_throughput:.0f} msg/sec")
    
    if individual_throughputs:
        print(f"📊 Peak Thread: {max(individual_throughputs):.0f} msg/sec")
        print(f"📉 Min Thread: {min(individual_throughputs):.0f} msg/sec")
        print(f"📊 Avg Thread: {sum(individual_throughputs)/len(individual_throughputs):.0f} msg/sec")
    
    print()
    print("🎯 Target: 400,000+ msg/sec")
    progress = (total_throughput / 400000) * 100
    print(f"📈 Progress: {progress:.1f}% of target")
    
    return total_throughput

if __name__ == "__main__":
    try:
        throughput = run_performance_test()
        if throughput > 50000:
            print("🎉 EXCELLENT: >50k msg/sec achieved!")
        elif throughput > 20000:
            print("✅ GOOD: >20k msg/sec achieved")
        elif throughput > 10000:
            print("📈 PROGRESS: >10k msg/sec achieved")
        else:
            print("⚠️  Need optimization: <10k msg/sec")
    except Exception as e:
        print(f"❌ Test failed: {e}")