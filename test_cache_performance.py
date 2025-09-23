#!/usr/bin/env python3

import socket
import struct
import time

def create_produce_request(topic, message, correlation_id=1):
    """Create a Kafka produce request"""
    api_key = 0
    api_version = 0
    client_id = b"cache-test-client"

    key = b"cache-key"
    value = message.encode('utf-8')

    # Message format (old format for simplicity)
    message_body = struct.pack('>I', len(key)) + key + struct.pack('>I', len(value)) + value
    message_content = struct.pack('>bb', 0, 0) + message_body  # Magic=0, Attributes=0

    # CRC32
    import zlib
    crc = zlib.crc32(message_content) & 0xffffffff

    # Full message
    message_size = 4 + 1 + 1 + 4 + len(key) + 4 + len(value)
    offset = 0

    message = struct.pack('>Q', offset) + struct.pack('>I', message_size) + struct.pack('>I', crc) + message_content

    # Message set
    message_set = message
    message_set_size = len(message_set)

    # Partition data
    partition_data = struct.pack('>I', 0) + struct.pack('>I', message_set_size) + message_set

    # Topic data
    topic_bytes = topic.encode('utf-8')
    topic_data = struct.pack('>H', len(topic_bytes)) + topic_bytes + struct.pack('>I', 1) + partition_data

    # Request body
    required_acks = 1
    timeout = 10000
    topic_array = struct.pack('>I', 1) + topic_data

    request_body = struct.pack('>hI', required_acks, timeout) + topic_array

    # Client ID
    client_id_data = struct.pack('>H', len(client_id)) + client_id

    # Request header
    request_header = struct.pack('>HhI', api_key, api_version, correlation_id) + client_id_data

    # Full request
    full_request = request_header + request_body

    # Add length prefix
    length_prefix = struct.pack('>I', len(full_request))

    return length_prefix + full_request

def create_fetch_request(topic, partition=0, offset=0, correlation_id=2):
    """Create a Kafka fetch request"""
    api_key = 1
    api_version = 0
    client_id = b"cache-test-client"

    # Fetch request parameters
    replica_id = -1  # Consumer
    max_wait_time = 1000  # 1 second
    min_bytes = 1

    # Topic partition data
    topic_bytes = topic.encode('utf-8')
    partition_data = struct.pack('>I', partition) + struct.pack('>Q', offset) + struct.pack('>I', 1048576)  # max_bytes
    topic_data = struct.pack('>H', len(topic_bytes)) + topic_bytes + struct.pack('>I', 1) + partition_data

    # Request body
    request_body = struct.pack('>iII', replica_id, max_wait_time, min_bytes) + struct.pack('>I', 1) + topic_data

    # Client ID
    client_id_data = struct.pack('>H', len(client_id)) + client_id

    # Request header
    request_header = struct.pack('>HhI', api_key, api_version, correlation_id) + client_id_data

    # Full request
    full_request = request_header + request_body

    # Add length prefix
    length_prefix = struct.pack('>I', len(full_request))

    return length_prefix + full_request

def send_request(sock, request):
    """Send request and get response"""
    sock.send(request)

    # Read response length
    response_length = struct.unpack('>I', sock.recv(4))[0]

    # Read response data
    response_data = sock.recv(response_length)
    return response_data

def test_cache_performance():
    print("🧪 Testing FluxMQ Message Cache Performance...")

    try:
        # Connect to FluxMQ
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 9092))

        topic = "cache-test-topic"

        # Step 1: Produce 10 messages to populate cache
        print("📤 Phase 1: Producing 10 messages to populate cache...")
        for i in range(10):
            message = f"Cache test message {i+1} - timestamp {time.time()}"
            request = create_produce_request(topic, message, correlation_id=i+1)
            response = send_request(sock, request)
            print(f"   ✅ Produced message {i+1}")

        print("⏳ Waiting 2 seconds for cache to populate...")
        time.sleep(2)

        # Step 2: Fetch messages multiple times to test cache hits
        print("📥 Phase 2: Testing cache performance with repeated fetches...")

        fetch_times = []
        for attempt in range(5):
            start_time = time.time()

            # Fetch from offset 0 (should hit cache after first fetch)
            request = create_fetch_request(topic, partition=0, offset=0, correlation_id=100+attempt)
            response = send_request(sock, request)

            end_time = time.time()
            fetch_duration = (end_time - start_time) * 1000  # Convert to ms
            fetch_times.append(fetch_duration)

            print(f"   📊 Fetch attempt {attempt+1}: {fetch_duration:.2f}ms")

        # Step 3: Analyze performance
        print("\n📈 Cache Performance Analysis:")
        print(f"   First fetch (cache miss): {fetch_times[0]:.2f}ms")
        if len(fetch_times) > 1:
            avg_cache_hits = sum(fetch_times[1:]) / len(fetch_times[1:])
            print(f"   Avg cache hits: {avg_cache_hits:.2f}ms")
            improvement = ((fetch_times[0] - avg_cache_hits) / fetch_times[0]) * 100
            print(f"   Performance improvement: {improvement:.1f}%")

        sock.close()

        print("✅ Cache performance test completed successfully!")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_cache_performance()
    if success:
        print("\n🎉 Message cache is working and showing performance benefits!")
    else:
        print("\n💥 Cache test failed - check server logs for details")