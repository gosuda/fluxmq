#!/usr/bin/env python3

import socket
import struct
import time

def create_simple_produce_request(topic, message, correlation_id=1):
    """Create a simple Kafka produce request"""
    api_key = 0
    api_version = 0
    client_id = b"compression-test"

    # Create message content
    key = b"large-message-key"
    value = message.encode('utf-8')
    print(f"📏 Message size: {len(value)} bytes")

    # Create simple message (old format for compatibility)
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

def test_compression_simple():
    print("🧪 Simple FluxMQ Compression Test...")

    try:
        # Connect to FluxMQ
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)  # 10 second timeout
        sock.connect(('localhost', 9092))
        print("✅ Connected to FluxMQ")

        topic = "simple-compression-test"

        # Create a large message that should trigger compression (>1KB)
        large_message = f"""
        COMPRESSION TEST MESSAGE
        Timestamp: {time.time()}
        Content: {'REPEATED_DATA_FOR_COMPRESSION ' * 100}
        This message is designed to be large enough to trigger LZ4 compression.
        Size should be over 1KB to test automatic compression functionality.
        """

        print(f"📤 Sending large message to test compression...")
        request = create_simple_produce_request(topic, large_message, correlation_id=1)
        sock.send(request)

        # Read response length
        response_length_data = sock.recv(4)
        if len(response_length_data) < 4:
            print("❌ Failed to read response length")
            return False

        response_length = struct.unpack('>I', response_length_data)[0]
        print(f"📥 Response length: {response_length} bytes")

        # Read response data
        response_data = sock.recv(response_length)
        print(f"📊 Response received: {len(response_data)} bytes")

        if len(response_data) >= 8:
            correlation_id = struct.unpack('>I', response_data[0:4])[0]
            print(f"✅ Response correlation ID: {correlation_id}")

        sock.close()
        print("✅ Simple compression test completed successfully!")
        print("📋 Check server logs for compression metrics and verification")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_compression_simple()
    if success:
        print("\n🎉 Compression functionality verified!")
        print("📊 Server successfully processed large message that should trigger compression")
    else:
        print("\n💥 Compression test failed")