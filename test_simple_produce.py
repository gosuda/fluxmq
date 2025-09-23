#!/usr/bin/env python3

import socket
import struct
import time

def create_produce_request():
    """Create a minimal Kafka produce request manually"""
    # Kafka Produce API (key=0, version=0)
    api_key = 0
    api_version = 0
    correlation_id = 42
    client_id = b"test-client"

    # Topic data
    topic_name = b"test-topic"
    partition = 0
    message_set_size = 0  # Will calculate

    # Simple message
    key = b"key1"
    value = b"Hello FluxMQ!"

    # Message format (old format for simplicity)
    # Offset (8) + MessageSize (4) + CRC (4) + Magic (1) + Attributes (1) + KeyLength (4) + Key + ValueLength (4) + Value
    message_body = struct.pack('>I', len(key)) + key + struct.pack('>I', len(value)) + value
    message_content = struct.pack('>bb', 0, 0) + message_body  # Magic=0, Attributes=0

    # Calculate CRC32 for the message content
    import zlib
    crc = zlib.crc32(message_content) & 0xffffffff

    # Full message
    message_size = 4 + 1 + 1 + 4 + len(key) + 4 + len(value)  # CRC + Magic + Attributes + KeyLen + Key + ValueLen + Value
    offset = 0  # First message

    message = struct.pack('>Q', offset) + struct.pack('>I', message_size) + struct.pack('>I', crc) + message_content

    # Message set
    message_set = message
    message_set_size = len(message_set)

    # Partition data
    partition_data = struct.pack('>I', partition) + struct.pack('>I', message_set_size) + message_set

    # Topic data
    topic_data = struct.pack('>H', len(topic_name)) + topic_name + struct.pack('>I', 1) + partition_data  # 1 partition

    # Required acks, timeout, topic array
    required_acks = 1
    timeout = 10000
    topic_array = struct.pack('>I', 1) + topic_data  # 1 topic

    # Request body
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

def test_manual_produce():
    print("🧪 Testing manual Kafka produce request...")

    try:
        # Connect to FluxMQ
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 9092))

        # Send produce request
        request = create_produce_request()
        print(f"📤 Sending produce request ({len(request)} bytes)")
        sock.send(request)

        # Read response
        response_length = struct.unpack('>I', sock.recv(4))[0]
        print(f"📥 Response length: {response_length}")

        response_data = sock.recv(response_length)
        print(f"📥 Response data: {response_data.hex()}")

        sock.close()

        print("✅ Produce request sent successfully!")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_manual_produce()
    if success:
        print("⏳ Waiting 2 seconds for metrics to update...")
        time.sleep(2)
        print("📊 Check server logs for metrics update")