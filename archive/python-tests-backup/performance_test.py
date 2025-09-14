#!/usr/bin/env python3
"""
Performance test comparing original vs optimized FluxMQ storage
"""
import time
import subprocess
import sys
from kafka import KafkaProducer
import threading

def create_test_messages(count=1000):
    """Create test messages for benchmarking"""
    messages = []
    for i in range(count):
        message = {
            'key': f'key_{i}',
            'value': f'message_value_{i}_{"x" * 100}'  # ~110 bytes per message
        }
        messages.append(message)
    return messages

def run_producer_test(message_count=10000, threads=4):
    """Run high-performance producer test"""
    print(f"🚀 Running producer performance test:")
    print(f"   Messages: {message_count}")
    print(f"   Threads: {threads}")
    print(f"   Expected data: ~{(message_count * 110) / 1024 / 1024:.1f} MB")
    
    start_time = time.time()
    
    def producer_worker(thread_id, messages_per_thread):
        try:
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                batch_size=16384,  # Large batch size for performance
                linger_ms=10,      # Batch for 10ms
                buffer_memory=33554432,  # 32MB buffer
                max_request_size=1048576,  # 1MB max request
                acks=1             # Leader acknowledgment only
            )
            
            for i in range(messages_per_thread):
                key = f'thread_{thread_id}_key_{i}'
                value = f'thread_{thread_id}_message_{i}_' + 'x' * 100
                
                producer.send('performance-test', key=key.encode(), value=value.encode())
                
                # Periodic flush for batching
                if i % 1000 == 0:
                    producer.flush()
            
            producer.flush()
            producer.close()
            print(f"✅ Thread {thread_id} completed {messages_per_thread} messages")
            
        except Exception as e:
            print(f"❌ Thread {thread_id} failed: {e}")
    
    # Start producer threads
    messages_per_thread = message_count // threads
    thread_list = []
    
    for thread_id in range(threads):
        thread = threading.Thread(
            target=producer_worker, 
            args=(thread_id, messages_per_thread)
        )
        thread.start()
        thread_list.append(thread)
    
    # Wait for all threads to complete
    for thread in thread_list:
        thread.join()
    
    end_time = time.time()
    duration = end_time - start_time
    
    total_messages = messages_per_thread * threads
    messages_per_second = total_messages / duration
    
    print(f"\n📊 Performance Results:")
    print(f"   Duration: {duration:.2f} seconds")
    print(f"   Messages sent: {total_messages}")
    print(f"   Throughput: {messages_per_second:.1f} msg/sec")
    print(f"   Data rate: {(total_messages * 110) / duration / 1024 / 1024:.1f} MB/sec")
    
    return {
        'duration': duration,
        'messages': total_messages,
        'throughput': messages_per_second,
        'data_rate_mb_per_sec': (total_messages * 110) / duration / 1024 / 1024
    }

def wait_for_server(max_retries=30):
    """Wait for FluxMQ server to be ready"""
    import socket
    
    for i in range(max_retries):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(('localhost', 9092))
            sock.close()
            
            if result == 0:
                print("✅ FluxMQ server is ready")
                return True
        except:
            pass
        
        print(f"⏳ Waiting for server... ({i+1}/{max_retries})")
        time.sleep(1)
    
    print("❌ Server not ready after 30 seconds")
    return False

def main():
    print("🧪 FluxMQ Performance Test Suite")
    print("=" * 50)
    
    # Check if server is running
    if not wait_for_server(5):
        print("❌ FluxMQ server is not running on localhost:9092")
        print("Please start FluxMQ server first:")
        print("cargo run --release -- --port 9092 --enable-consumer-groups")
        return 1
    
    try:
        # Warm-up test
        print("\n🔥 Warm-up test (1000 messages)...")
        warmup_result = run_producer_test(message_count=1000, threads=1)
        
        # Main performance test
        print("\n🚀 Main performance test (20000 messages, 4 threads)...")
        main_result = run_producer_test(message_count=20000, threads=4)
        
        # High-load test
        print("\n⚡ High-load test (50000 messages, 8 threads)...")
        high_load_result = run_producer_test(message_count=50000, threads=8)
        
        # Summary
        print("\n" + "=" * 60)
        print("📈 PERFORMANCE SUMMARY")
        print("=" * 60)
        print(f"Warm-up:   {warmup_result['throughput']:>10.1f} msg/sec")
        print(f"Main test: {main_result['throughput']:>10.1f} msg/sec")
        print(f"High-load: {high_load_result['throughput']:>10.1f} msg/sec")
        print(f"Peak data rate: {max(warmup_result['data_rate_mb_per_sec'], main_result['data_rate_mb_per_sec'], high_load_result['data_rate_mb_per_sec']):.1f} MB/sec")
        
        # Performance assessment
        peak_throughput = max(warmup_result['throughput'], main_result['throughput'], high_load_result['throughput'])
        target_throughput = 400000  # Target: 400k msg/sec
        
        print(f"\n🎯 Performance Assessment:")
        print(f"   Current peak: {peak_throughput:.1f} msg/sec")
        print(f"   Target:       {target_throughput:,} msg/sec")
        print(f"   Progress:     {(peak_throughput / target_throughput * 100):.1f}% of target")
        
        if peak_throughput >= target_throughput:
            print("🎉 TARGET ACHIEVED! 🎉")
        else:
            improvement_needed = target_throughput / peak_throughput
            print(f"📊 Need {improvement_needed:.1f}x improvement to reach target")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⚠️ Test interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())