#!/usr/bin/env python3
"""
Test direct Metadata API call to FluxMQ
"""

import socket
import struct

def test_metadata_api():
    print("🔍 Direct Metadata API Test")
    print("=" * 40)
    
    try:
        # Connect to FluxMQ
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect(('localhost', 9092))
        
        print("✅ Connected to FluxMQ")
        
        # Build Metadata request (API key 3, version 0)
        correlation_id = 12346
        client_id = b"metadata-test"
        
        # Metadata request format: [api_key][api_version][correlation_id][client_id][topics]
        # For all topics, topics should be empty array (-1 for null array)
        request_body = struct.pack('>HHI', 3, 0, correlation_id) + \
                      struct.pack('>H', len(client_id)) + client_id + \
                      struct.pack('>i', -1)  # All topics
        
        # Add length prefix
        full_request = struct.pack('>I', len(request_body)) + request_body
        
        print(f"Sending Metadata request ({len(full_request)} bytes): {full_request.hex()}")
        sock.send(full_request)
        
        # Read response
        print("⏳ Waiting for Metadata response...")
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
                    print("✅ Metadata API working! Correlation ID matches!")
                    # Try to decode basic response structure
                    if len(response_data) >= 8:
                        throttle_time = struct.unpack('>I', response_data[4:8])[0]
                        print(f"Throttle time: {throttle_time}ms")
                else:
                    print(f"❌ Correlation ID mismatch: expected {correlation_id}, got {resp_correlation_id}")
        
        sock.close()
        print("🎉 Metadata test completed!")
        
    except Exception as e:
        print(f"❌ Metadata test error: {e}")

if __name__ == "__main__":
    test_metadata_api()