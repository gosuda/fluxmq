#!/usr/bin/env python3

"""
Fast Producer Test for FluxMQ
Optimized kafka-python settings for maximum individual request throughput.
Focus on minimizing per-request overhead.
"""

from kafka import KafkaProducer
import time
import json

def fast_producer_test():
    """Fast producer test with optimized settings"""
    print("⚡ Fast Producer Test - Minimal Overhead")
    print("======================================")
    
    try:
        # Ultra-fast producer configuration - minimize overhead
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            client_id='fast-producer',
            
            # 🚀 Speed optimizations
            acks=1,                    # Leader only (fastest)
            retries=0,                 # No retries (fastest)
            max_in_flight_requests_per_connection=10,  # Max parallelism
            request_timeout_ms=3000,   # Quick timeout
            api_version=(0, 10, 1),    # Explicit version
            
            # 📦 Buffer settings for speed
            buffer_memory=16777216,    # 16MB buffer (smaller, faster)
            send_buffer_bytes=131072,  # 128KB send buffer
            receive_buffer_bytes=32768, # 32KB receive buffer
            
            # ⏱️ No batching delay
            batch_size=1,              # Minimal batching
            linger_ms=0,              # No waiting
            
            # 🔧 Connection optimizations
            connections_max_idle_ms=9 * 60 * 1000,  # Keep connections
            reconnect_backoff_ms=50,   # Fast reconnect
            retry_backoff_ms=100,      # Quick retry
        )
        
        topic_name = 'fast-test-topic'
        num_messages = 2000
        
        print(f"Sending {num_messages:,} messages to topic: {topic_name}")
        print(f"Optimizations:")
        print(f"  - acks=1 (leader only)")
        print(f"  - retries=0 (no retries)")
        print(f"  - batch_size=1 (minimal batch)")
        print(f"  - linger_ms=0 (no wait)")
        print(f"  - max_in_flight=10 (parallel)")
        print()
        
        start_time = time.time()
        
        # Send messages as fast as possible
        for i in range(num_messages):
            message = {
                'id': i,
                'timestamp': time.time(),
                'data': f'fast-msg-{i:06d}',
                'payload': 'x' * 50  # Small 50-byte payload
            }
            
            # Fire and forget - maximum speed
            producer.send(topic_name, message)
            
            if i % 250 == 0 and i > 0:
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                print(f"  Sent: {i:,} messages... ({rate:,.0f} msg/sec so far)")
        
        # Flush remaining messages
        print("  Flushing remaining messages...")
        producer.flush()
        producer.close()
        
        end_time = time.time()
        duration = end_time - start_time
        throughput = num_messages / duration
        
        print()
        print("⚡ Fast Producer Results:")
        print(f"   Messages: {num_messages:,}")
        print(f"   Duration: {duration:.3f} seconds")
        print(f"   Throughput: {throughput:,.0f} msg/sec")
        print()
        
        # Performance analysis
        baseline = 7305  # Previous baseline
        improvement = (throughput / baseline - 1) * 100 if baseline > 0 else 0
        
        if throughput >= 49000:
            print(f"🎉 SUCCESS! Achieved Phase 1 target: {throughput:,.0f} msg/sec!")
        elif throughput >= 30000:
            print(f"🔥 EXCELLENT! {throughput:,.0f} msg/sec - Very close to target!")
        elif throughput >= 15000:
            print(f"✅ SIGNIFICANT IMPROVEMENT! {throughput:,.0f} msg/sec - 2x over baseline!")
        elif throughput >= 10000:
            print(f"📈 GOOD IMPROVEMENT! {throughput:,.0f} msg/sec - Notable gain!")
        elif improvement > 5:
            print(f"📊 MODEST IMPROVEMENT: {throughput:,.0f} msg/sec (+{improvement:.1f}%)")
        else:
            print(f"📊 BASELINE: {throughput:,.0f} msg/sec - Similar to previous")
        
        # Speed analysis
        msg_latency = duration / num_messages * 1000  # ms per message
        print(f"   Average latency: {msg_latency:.2f} ms/msg")
        
        return throughput
        
    except Exception as e:
        print(f"⚠️ Error: {e}")
        return 0

if __name__ == "__main__":
    fast_producer_test()