#!/usr/bin/env python3
import time
import threading
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import json

def create_topic():
    """Create the test topic first"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='topic_creator',
            request_timeout_ms=10000
        )
        
        topic_list = [NewTopic(name='small-multi-topic', num_partitions=3, replication_factor=1)]
        
        try:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print("✅ Topic 'small-multi-topic' created successfully")
        except Exception as e:
            if "already exists" in str(e) or "TopicExistsException" in str(e):
                print("✅ Topic 'small-multi-topic' already exists")
            else:
                print(f"⚠️  Topic creation error: {e}")
        
        admin_client.close()
        time.sleep(1)  # Let topic propagate
        return True
        
    except Exception as e:
        print(f"❌ Failed to create topic: {e}")
        return False

def producer_thread(thread_id, messages_per_thread, results):
    start_time = time.time()
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_block_ms=5000,
            retries=3,
            batch_size=8192,
            linger_ms=1,
            compression_type=None
        )
        
        messages_sent = 0
        for i in range(messages_per_thread):
            message = {"thread": thread_id, "message": i, "timestamp": time.time()}
            producer.send('small-multi-topic', message)
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

def run_small_multi_test():
    print("🚀 FluxMQ Small Multi-threaded Test (Port 9092)")
    print("🏗️  Creating topic first...")
    
    if not create_topic():
        print("❌ Failed to create topic, aborting test")
        return 0
        
    num_threads = 2
    messages_per_thread = 1000
    total_messages = num_threads * messages_per_thread
    
    print(f"📊 Testing: {num_threads} threads × {messages_per_thread} messages = {total_messages} total")
    print("🔥 I/O Optimized FluxMQ")
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
    print("🏆 SMALL MULTI-THREADED RESULTS")
    print("=" * 50)
    print(f"📈 Total Messages: {total_sent:,}")
    print(f"⏱️  Total Duration: {test_duration:.2f} seconds")
    print(f"🚀 Total Throughput: {total_throughput:.0f} msg/sec")
    
    if individual_throughputs:
        print(f"📊 Peak Thread: {max(individual_throughputs):.0f} msg/sec")
        print(f"📉 Min Thread: {min(individual_throughputs):.0f} msg/sec")
        print(f"📊 Avg Thread: {sum(individual_throughputs)/len(individual_throughputs):.0f} msg/sec")
        
        # Thread consistency check
        consistency = (min(individual_throughputs) / max(individual_throughputs)) * 100
        print(f"🎯 Thread Consistency: {consistency:.1f}%")
    
    return total_throughput

if __name__ == "__main__":
    try:
        throughput = run_small_multi_test()
        
        print("\n" + "=" * 50)
        if throughput > 10000:
            print("🎉 EXCELLENT: >10k msg/sec achieved!")
        elif throughput > 5000:
            print("✅ VERY GOOD: >5k msg/sec achieved")
        elif throughput > 3000:
            print("✅ GOOD: >3k msg/sec achieved")
        else:
            print("⚠️  Need optimization: <3k msg/sec")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")