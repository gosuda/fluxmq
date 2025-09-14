#!/usr/bin/env python3
"""
Force metadata request test using lower level kafka-python APIs
"""

import socket
import struct
import time
from kafka import KafkaClient
from kafka.protocol.api import Request, Response
from kafka.protocol.metadata import MetadataRequest_v1, MetadataResponse_v1

def test_direct_metadata():
    print("🔍 Direct Metadata Request Test")
    print("=" * 40)
    
    try:
        # Create a basic kafka client with longer timeout
        client = KafkaClient(
            bootstrap_servers=['localhost:9092'],
            client_id='direct-metadata-test',
            request_timeout_ms=30000,  # 30 second timeout
            reconnect_backoff_ms=100,
            reconnect_backoff_max_ms=1000,
        )
        
        print("✅ KafkaClient created")
        
        # Force metadata check
        print("🔍 Checking if client is ready...")
        
        # Try to wait for the client to be ready
        ready = client.ready(node_id='bootstrap-0', timeout_ms=10000)
        print(f"Client ready status: {ready}")
        
        if ready:
            print("✅ Client is ready - this means ApiVersions worked!")
            
            # Now try to get metadata
            print("📋 Requesting cluster metadata...")
            metadata = client.cluster.metadata()
            print(f"Cluster metadata: {metadata}")
            
        else:
            print("❌ Client not ready - ApiVersions or connection issue")
            
        # Try to explicitly send metadata request
        print("📋 Attempting explicit metadata request...")
        try:
            metadata_request = MetadataRequest_v1([])  # Request all topics
            response = client.send(node_id=None, request=metadata_request)
            print(f"Metadata response: {response}")
        except Exception as e:
            print(f"Metadata request failed: {e}")
        
        client.close()
        print("🎉 Direct test completed!")
        
    except Exception as e:
        print(f"❌ Direct test error: {e}")
        print(f"   Error type: {type(e)}")
        import traceback
        traceback.print_exc()

def test_socket_connection():
    print("\n🔌 Raw Socket Connection Test")
    print("=" * 40)
    
    try:
        # Test raw socket connection
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect(('localhost', 9092))
        
        print("✅ Raw socket connection successful")
        
        # Send a simple ApiVersions request manually
        # Format: [length][api_key][api_version][correlation_id][client_id]
        correlation_id = 12345
        client_id = b"socket-test"
        
        # Build ApiVersions request (API key 18, version 0)
        request_body = struct.pack('>HHI', 18, 0, correlation_id) + \
                      struct.pack('>H', len(client_id)) + client_id
        
        # Add length prefix
        full_request = struct.pack('>I', len(request_body)) + request_body
        
        print(f"Sending {len(full_request)} bytes: {full_request.hex()}")
        sock.send(full_request)
        
        # Read response
        print("⏳ Waiting for response...")
        response_length_bytes = sock.recv(4)
        if len(response_length_bytes) == 4:
            response_length = struct.unpack('>I', response_length_bytes)[0]
            print(f"Response length: {response_length}")
            
            response_data = sock.recv(response_length)
            print(f"Response data ({len(response_data)} bytes): {response_data.hex()}")
            
            if len(response_data) >= 4:
                resp_correlation_id = struct.unpack('>I', response_data[:4])[0]
                print(f"Response correlation ID: {resp_correlation_id}")
                
                if resp_correlation_id == correlation_id:
                    print("✅ Correlation ID matches!")
                else:
                    print(f"❌ Correlation ID mismatch: expected {correlation_id}, got {resp_correlation_id}")
        
        sock.close()
        print("🎉 Raw socket test completed!")
        
    except Exception as e:
        print(f"❌ Socket test error: {e}")

if __name__ == "__main__":
    test_direct_metadata()
    test_socket_connection()