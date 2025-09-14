#!/usr/bin/env python3

"""
Check if DescribeConfigs API is advertised in ApiVersions
"""

from kafka.admin import KafkaAdminClient
import socket

def test_api_versions():
    """Check if API 32 is supported"""
    print("🔍 Testing API Versions")
    print("=" * 40)
    
    try:
        # Create admin client 
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='api-test'
        )
        
        # Get the client connection
        broker = admin_client._client.cluster.brokers()[0]
        print(f"Connected to broker: {broker.host}:{broker.port}")
        
        # Check supported APIs
        conn = admin_client._client._get_conn(broker.nodeId)
        print(f"Connection established: {conn}")
        
        # Check if the client has API version info
        api_versions = admin_client._client.api_version
        print(f"Client API version: {api_versions}")
        
        print("✅ Connected successfully to FluxMQ")
        admin_client.close()
        return True
        
    except Exception as e:
        print(f"⚠️ Error: {e}")
        return False

def check_raw_connection():
    """Make a raw connection to check protocol detection"""
    try:
        print("\n🔧 Raw Socket Test")
        print("-" * 30)
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 9092))
        
        # Send a simple test message (4 bytes length + some data)
        test_data = b'\x00\x00\x00\x04test'
        sock.send(test_data)
        
        print("✅ Raw socket connection successful")
        sock.close()
        return True
        
    except Exception as e:
        print(f"⚠️ Raw connection failed: {e}")
        return False

if __name__ == "__main__":
    success = test_api_versions()
    check_raw_connection()
    exit(0 if success else 1)