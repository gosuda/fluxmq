#!/usr/bin/env python3

"""
Multi-threaded Producer Test for FluxMQ
Tests concurrent producer performance to maximize throughput.
Goal: Achieve 49k+ msg/sec by parallelizing individual requests.
"""

from kafka import KafkaProducer
import time
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue

def single_producer_worker(worker_id, num_messages, results_queue, topic_name):
    """Single producer worker thread"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            client_id=f'multi-producer-{worker_id}',
            
            # Optimized settings for individual message performance
            acks=1,
            retries=1,
            max_in_flight_requests_per_connection=5,
            request_timeout_ms=5000,
            api_version=(0, 10, 1),
        )
        
        messages_sent = 0
        start_time = time.time()
        
        for i in range(num_messages):
            message = {
                'worker_id': worker_id,
                'message_id': i,
                'timestamp': time.time(),
                'data': f'multi-thread-msg-{worker_id}-{i:06d}',
                'payload': 'x' * 100  # 100 byte payload
            }
            
            # Send message
            producer.send(topic_name, message)
            messages_sent += 1
        
        # Flush remaining messages
        producer.flush()
        producer.close()
        
        end_time = time.time()
        duration = end_time - start_time
        throughput = messages_sent / duration if duration > 0 else 0
        
        results_queue.put({
            'worker_id': worker_id,
            'messages_sent': messages_sent,
            'duration': duration,
            'throughput': throughput
        })
        
        return throughput
        
    except Exception as e:
        print(f"⚠️ Worker {worker_id} error: {e}")
        results_queue.put({
            'worker_id': worker_id,
            'messages_sent': 0,
            'duration': 0,
            'throughput': 0,
            'error': str(e)
        })
        return 0

def multi_thread_producer_test():
    """Multi-threaded producer test - maximize parallelism"""
    print("🚀 Multi-threaded Producer Test for FluxMQ")
    print("===========================================")
    
    # Test configuration
    num_workers = 8           # 8 concurrent producers
    messages_per_worker = 1250  # 1250 * 8 = 10,000 total messages
    topic_name = 'multi-thread-topic'
    
    print(f"Configuration:")
    print(f"  - Workers: {num_workers}")
    print(f"  - Messages per worker: {messages_per_worker:,}")
    print(f"  - Total messages: {num_workers * messages_per_worker:,}")
    print(f"  - Topic: {topic_name}")
    print()
    
    results_queue = queue.Queue()
    start_time = time.time()
    
    # Launch producer workers
    print("Launching producer workers...")
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(single_producer_worker, worker_id, messages_per_worker, results_queue, topic_name)
            for worker_id in range(num_workers)
        ]
        
        # Wait for all workers to complete
        completed_workers = 0
        for future in as_completed(futures):
            try:
                worker_throughput = future.result()
                completed_workers += 1
                print(f"  Worker {completed_workers}/{num_workers} completed: {worker_throughput:,.0f} msg/sec")
            except Exception as e:
                print(f"  Worker failed: {e}")
    
    end_time = time.time()
    total_duration = end_time - start_time
    
    # Collect results
    results = []
    total_messages = 0
    total_errors = 0
    
    while not results_queue.empty():
        result = results_queue.get()
        results.append(result)
        if 'error' not in result:
            total_messages += result['messages_sent']
        else:
            total_errors += 1
    
    # Calculate overall throughput
    overall_throughput = total_messages / total_duration if total_duration > 0 else 0
    
    print()
    print("🎯 Multi-threaded Producer Results:")
    print(f"   Total messages: {total_messages:,}")
    print(f"   Total duration: {total_duration:.3f} seconds")
    print(f"   Overall throughput: {overall_throughput:,.0f} msg/sec")
    print(f"   Workers completed: {len(results) - total_errors}/{num_workers}")
    
    if total_errors > 0:
        print(f"   Errors: {total_errors}")
    
    print()
    print("Per-worker performance:")
    for result in results:
        if 'error' not in result:
            print(f"   Worker {result['worker_id']}: {result['messages_sent']:,} msgs in {result['duration']:.3f}s = {result['throughput']:,.0f} msg/sec")
        else:
            print(f"   Worker {result['worker_id']}: ERROR - {result['error']}")
    
    print()
    
    # Performance analysis
    if overall_throughput >= 49000:
        print(f"🎉 SUCCESS! Achieved Phase 1 target: {overall_throughput:,.0f} msg/sec!")
        if overall_throughput >= 100000:
            print(f"🚀 ULTRA SUCCESS! Approaching Phase 2 levels!")
    elif overall_throughput >= 30000:
        print(f"🔥 EXCELLENT! {overall_throughput:,.0f} msg/sec - Very close to target!")
    elif overall_throughput >= 15000:
        print(f"✅ SIGNIFICANT IMPROVEMENT! {overall_throughput:,.0f} msg/sec - 2x over single-thread!")
    elif overall_throughput >= 10000:
        print(f"📈 GOOD IMPROVEMENT! {overall_throughput:,.0f} msg/sec - Notable gain!")
    else:
        print(f"📊 BASELINE: {overall_throughput:,.0f} msg/sec - Need more optimization")
    
    # Scaling analysis
    single_thread_estimate = 7300  # Based on previous tests
    scaling_factor = overall_throughput / single_thread_estimate if single_thread_estimate > 0 else 0
    print(f"   Scaling factor: {scaling_factor:.2f}x (vs single-thread ~{single_thread_estimate:,} msg/sec)")
    
    # Efficiency analysis
    theoretical_max = single_thread_estimate * num_workers
    efficiency = (overall_throughput / theoretical_max * 100) if theoretical_max > 0 else 0
    print(f"   Parallelization efficiency: {efficiency:.1f}% of theoretical maximum")
    
    return overall_throughput

if __name__ == "__main__":
    multi_thread_producer_test()