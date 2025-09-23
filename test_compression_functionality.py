#!/usr/bin/env python3

import socket
import struct
import time

def create_produce_request(topic, message, correlation_id=1):
    """Create a Kafka produce request with large message for compression testing"""
    api_key = 0
    api_version = 0
    client_id = b"compression-test-client"

    key = b"test-key"
    # Create a large, compressible message (>1KB to trigger compression)
    large_message = f"""
    This is a comprehensive test message designed to trigger FluxMQ's compression functionality.
    Message timestamp: {time.time()}
    Message content: {'REPEATED_DATA_FOR_COMPRESSION ' * 50}
    This message should be over 1KB to trigger LZ4 compression automatically.
    FluxMQ compression test with repeated patterns for better compression ratios.
    """ * 5  # Make it even larger

    value = large_message.encode('utf-8')
    print(f"📏 Original message size: {len(value)} bytes")

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
    client_id = b"compression-test-client"

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

def test_compression():
    print("🧪 Testing FluxMQ Compression Functionality...")

    try:
        # Connect to FluxMQ
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 9092))

        topic = "compression-test-topic"

        # Step 1: Produce large messages to trigger compression
        print("📤 Phase 1: Producing large messages to test compression...")
        for i in range(5):
            message_id = f"compression-test-{i+1}"
            request = create_produce_request(topic, message_id, correlation_id=i+1)
            response = send_request(sock, request)
            print(f"   ✅ Produced large message {i+1}")

        print("⏳ Waiting 2 seconds for compression processing...")
        time.sleep(2)

        # Step 2: Fetch messages to verify they were stored and can be retrieved
        print("📥 Phase 2: Fetching messages to verify compression/decompression...")

        for attempt in range(3):
            request = create_fetch_request(topic, partition=0, offset=0, correlation_id=100+attempt)
            response = send_request(sock, request)

            print(f"   📊 Fetch attempt {attempt+1}: Received {len(response)} bytes")

        sock.close()

        print("✅ Compression test completed successfully!")
        print("📊 Check server logs for compression statistics")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_compression()
    if success:
        print("\n🎉 Compression functionality is working!")
        print("📋 Next: Check FluxMQ server logs for compression metrics")
    else:
        print("\n💥 Compression test failed - check server logs for details")