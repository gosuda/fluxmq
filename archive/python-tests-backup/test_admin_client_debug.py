#!/usr/bin/env python3
"""
AdminClient Protocol Debug Test
Isolates the exact AdminClient issue
"""

import socket
import struct
import logging

logging.basicConfig(level=logging.DEBUG)

def send_metadata_request():
    """Send a raw metadata request to see what AdminClient expects"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 9092))
    
    # ApiVersions request first (API key 18, version 2)
    api_key = 18
    api_version = 2
    correlation_id = 1
    client_id = "admin-debug"
    
    # Build request
    request = struct.pack('>h', api_key)  # API key
    request += struct.pack('>h', api_version)  # API version
    request += struct.pack('>i', correlation_id)  # Correlation ID
    request += struct.pack('>h', len(client_id))  # Client ID length
    request += client_id.encode('utf-8')  # Client ID
    
    # Send with length prefix
    message = struct.pack('>i', len(request)) + request
    sock.send(message)
    
    # Read response
    response_length = struct.unpack('>i', sock.recv(4))[0]
    response = sock.recv(response_length)
    print(f"ApiVersions response length: {response_length}")
    print(f"Response bytes: {response.hex()[:100]}...")
    
    # Now send Metadata request (API key 3, version 5)
    api_key = 3
    api_version = 5  # AdminClient typically uses v5 or higher
    correlation_id = 2
    
    request = struct.pack('>h', api_key)  # API key
    request += struct.pack('>h', api_version)  # API version
    request += struct.pack('>i', correlation_id)  # Correlation ID
    request += struct.pack('>h', len(client_id))  # Client ID length
    request += client_id.encode('utf-8')  # Client ID
    request += struct.pack('>i', 0)  # No topics (get all)
    
    # Send with length prefix
    message = struct.pack('>i', len(request)) + request
    sock.send(message)
    
    # Read response
    response_length = struct.unpack('>i', sock.recv(4))[0]
    response = sock.recv(response_length)
    print(f"\nMetadata response length: {response_length}")
    print(f"Response bytes: {response.hex()}")
    
    # Parse response
    pos = 0
    correlation_id = struct.unpack('>i', response[pos:pos+4])[0]
    pos += 4
    print(f"Correlation ID: {correlation_id}")
    
    throttle_time = struct.unpack('>i', response[pos:pos+4])[0]
    pos += 4
    print(f"Throttle time: {throttle_time}")
    
    broker_count = struct.unpack('>i', response[pos:pos+4])[0]
    pos += 4
    print(f"Broker count: {broker_count}")
    
    # Parse brokers
    for i in range(broker_count):
        node_id = struct.unpack('>i', response[pos:pos+4])[0]
        pos += 4
        host_len = struct.unpack('>h', response[pos:pos+2])[0]
        pos += 2
        host = response[pos:pos+host_len].decode('utf-8')
        pos += host_len
        port = struct.unpack('>i', response[pos:pos+4])[0]
        pos += 4
        
        # v5 has rack field
        if api_version >= 1:
            rack_len = struct.unpack('>h', response[pos:pos+2])[0]
            pos += 2
            if rack_len > 0:
                rack = response[pos:pos+rack_len].decode('utf-8')
                pos += rack_len
        
        print(f"  Broker {i}: node_id={node_id}, host={host}, port={port}")
    
    # v2+ has cluster_id
    if api_version >= 2:
        cluster_id_len = struct.unpack('>h', response[pos:pos+2])[0]
        pos += 2
        if cluster_id_len > 0:
            cluster_id = response[pos:pos+cluster_id_len].decode('utf-8')
            pos += cluster_id_len
        else:
            cluster_id = None
        print(f"Cluster ID: {cluster_id}")
    
    # v1+ has controller_id
    if api_version >= 1:
        controller_id = struct.unpack('>i', response[pos:pos+4])[0]
        pos += 4
        print(f"Controller ID: {controller_id}")
    
    # Topics array
    topic_count = struct.unpack('>i', response[pos:pos+4])[0]
    pos += 4
    print(f"Topic count: {topic_count}")
    
    print(f"\nRemaining bytes at topics: {response[pos:].hex()}")
    
    sock.close()

if __name__ == '__main__':
    try:
        send_metadata_request()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()