#!/usr/bin/env python3
"""
Quick FluxMQ Performance Benchmark
Tests current throughput and latency with Python kafka client
"""
import time
import threading
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

# Configuration
BROKER = 'localhost:9092'
TOPIC = 'performance-test'
NUM_MESSAGES = 10000
THREADS = 4

def producer_test():
    """Test producer performance"""
    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: v.encode('utf-8'),
        batch_size=16384,
        linger_ms=10,
        compression_type='lz4'
    )

    start_time = time.time()

    for i in range(NUM_MESSAGES):
        message = f"Performance test message {i}"
        producer.send(TOPIC, value=message)

    producer.flush()
    end_time = time.time()

    elapsed = end_time - start_time
    throughput = NUM_MESSAGES / elapsed

    print(f"✅ Producer Performance:")
    print(f"   Messages: {NUM_MESSAGES}")
    print(f"   Time: {elapsed:.2f}s")
    print(f"   Throughput: {throughput:.0f} msg/sec")
    print(f"   Latency: {elapsed/NUM_MESSAGES*1000:.2f} ms/msg")

    producer.close()
    return throughput

def multithreaded_producer_test():
    """Test multi-threaded producer performance"""
    results = []
    threads = []

    def thread_producer(thread_id):
        producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            value_serializer=lambda v: v.encode('utf-8'),
            batch_size=16384,
            linger_ms=5,
            compression_type='lz4'
        )

        messages_per_thread = NUM_MESSAGES // THREADS
        start_time = time.time()

        for i in range(messages_per_thread):
            message = f"Thread-{thread_id} message {i}"
            producer.send(f"{TOPIC}-mt", value=message)

        producer.flush()
        end_time = time.time()

        elapsed = end_time - start_time
        throughput = messages_per_thread / elapsed
        results.append(throughput)

        print(f"   Thread {thread_id}: {throughput:.0f} msg/sec")
        producer.close()

    print(f"\n🚀 Multi-threaded Producer Test ({THREADS} threads):")

    start_time = time.time()

    for i in range(THREADS):
        thread = threading.Thread(target=thread_producer, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    end_time = time.time()

    total_elapsed = end_time - start_time
    total_throughput = sum(results)

    print(f"✅ Multi-threaded Results:")
    print(f"   Total throughput: {total_throughput:.0f} msg/sec")
    print(f"   Average per thread: {total_throughput/THREADS:.0f} msg/sec")
    print(f"   Total time: {total_elapsed:.2f}s")

    return total_throughput

if __name__ == "__main__":
    print("🔥 FluxMQ Performance Benchmark Starting...")
    print(f"Server: {BROKER}")
    print(f"Topic: {TOPIC}")
    print("-" * 50)

    try:
        # Single-threaded test
        single_throughput = producer_test()

        # Multi-threaded test
        multi_throughput = multithreaded_producer_test()

        print("\n📊 Summary:")
        print(f"Single-threaded: {single_throughput:.0f} msg/sec")
        print(f"Multi-threaded: {multi_throughput:.0f} msg/sec")
        print(f"Improvement: {multi_throughput/single_throughput:.1f}x")

        if multi_throughput > 50000:
            print("🎉 Excellent performance! (50k+ msg/sec)")
        elif multi_throughput > 20000:
            print("✅ Good performance! (20k+ msg/sec)")
        else:
            print("⚠️ Below target performance (<20k msg/sec)")

    except KafkaError as e:
        print(f"❌ Kafka error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")