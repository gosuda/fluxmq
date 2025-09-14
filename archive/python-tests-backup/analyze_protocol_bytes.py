#!/usr/bin/env python3
"""
Comprehensive Kafka Protocol Byte Analysis Tool
Analyzes the exact byte structure differences between FluxMQ and Apache Kafka
"""

import socket
import struct
import io
import sys
import logging

logging.basicConfig(level=logging.INFO)

class KafkaProtocolAnalyzer:
    def __init__(self, host='localhost', port=9092):
        self.host = host
        self.port = port
    
    def send_and_capture_response(self, api_key, api_version, correlation_id, payload=b''):
        """Send a request and capture the raw response bytes"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.host, self.port))
        
        try:
            # Build request header
            client_id = "protocol-analyzer"
            request = struct.pack('>h', api_key)          # API key (2 bytes)
            request += struct.pack('>h', api_version)     # API version (2 bytes)
            request += struct.pack('>i', correlation_id)  # Correlation ID (4 bytes)
            request += struct.pack('>h', len(client_id))  # Client ID length (2 bytes)
            request += client_id.encode('utf-8')          # Client ID
            request += payload                            # Request payload
            
            # Send with length prefix
            message = struct.pack('>i', len(request)) + request
            sock.send(message)
            
            # Read response
            response_length = struct.unpack('>i', sock.recv(4))[0]
            response = sock.recv(response_length)
            
            return response_length, response
            
        finally:
            sock.close()
    
    def analyze_metadata_response_v2(self, response):
        """Analyze Metadata API v2 response structure"""
        print("=== Metadata API v2 Response Analysis ===")
        print(f"Total response length: {len(response)} bytes")
        print(f"First 64 bytes (hex): {response[:64].hex()}")
        print(f"Last 64 bytes (hex): {response[-64:].hex()}")
        
        cursor = io.BytesIO(response)
        
        # Parse response
        correlation_id = struct.unpack('>i', cursor.read(4))[0]
        print(f"Correlation ID: {correlation_id}")
        
        # v2 format: correlation_id + brokers + cluster_id + controller_id + topics + throttle_time_ms
        brokers_count = struct.unpack('>i', cursor.read(4))[0]
        print(f"Brokers count: {brokers_count}")
        
        # Parse brokers
        for i in range(brokers_count):
            node_id = struct.unpack('>i', cursor.read(4))[0]
            host_len = struct.unpack('>h', cursor.read(2))[0]
            host = cursor.read(host_len).decode('utf-8')
            port = struct.unpack('>i', cursor.read(4))[0]
            
            # v1+ has rack (nullable string)
            rack_len = struct.unpack('>h', cursor.read(2))[0]
            if rack_len > 0:
                rack = cursor.read(rack_len).decode('utf-8')
            else:
                rack = None
                
            print(f"  Broker {i}: node_id={node_id}, host={host}, port={port}, rack={rack}")
        
        # v2+ has cluster_id (nullable string)
        cluster_id_len = struct.unpack('>h', cursor.read(2))[0]
        if cluster_id_len > 0:
            cluster_id = cursor.read(cluster_id_len).decode('utf-8')
        else:
            cluster_id = None
        print(f"Cluster ID: {cluster_id}")
        
        # v1+ has controller_id
        controller_id = struct.unpack('>i', cursor.read(4))[0]
        print(f"Controller ID: {controller_id}")
        
        # Topics array
        topics_count = struct.unpack('>i', cursor.read(4))[0]
        print(f"Topics count: {topics_count}")
        
        print(f"Position after topics count: {cursor.tell()}")
        
        # Skip topic parsing for now, go to end
        remaining = len(response) - cursor.tell()
        print(f"Remaining bytes: {remaining}")
        
        if remaining >= 4:
            # Try to find throttle_time_ms at the end
            cursor.seek(-4, 2)  # Go to last 4 bytes
            throttle_time = struct.unpack('>i', cursor.read(4))[0]
            print(f"Potential throttle_time_ms at end: {throttle_time}")
        
        return {
            'correlation_id': correlation_id,
            'brokers_count': brokers_count,
            'cluster_id': cluster_id,
            'controller_id': controller_id,
            'topics_count': topics_count,
            'total_length': len(response)
        }
    
    def analyze_metadata_response_v6(self, response):
        """Analyze Metadata API v6 response structure"""
        print("\n=== Metadata API v6 Response Analysis ===")
        print(f"Total response length: {len(response)} bytes")
        print(f"First 64 bytes (hex): {response[:64].hex()}")
        
        cursor = io.BytesIO(response)
        
        # Parse response
        correlation_id = struct.unpack('>i', cursor.read(4))[0]
        print(f"Correlation ID: {correlation_id}")
        
        # v3+ format: correlation_id + throttle_time_ms + brokers + cluster_id + controller_id + topics
        throttle_time = struct.unpack('>i', cursor.read(4))[0]
        print(f"Throttle time (at position 4-8): {throttle_time}")
        
        brokers_count = struct.unpack('>i', cursor.read(4))[0]
        print(f"Brokers count: {brokers_count}")
        
        return {
            'correlation_id': correlation_id,
            'throttle_time_ms': throttle_time,
            'brokers_count': brokers_count,
            'total_length': len(response)
        }

    def create_reference_response_v2(self):
        """Create a reference v2 response to compare with"""
        print("\n=== Creating Reference v2 Response ===")
        
        # Build a minimal v2 response manually
        response = b''
        response += struct.pack('>i', 1)           # correlation_id
        response += struct.pack('>i', 1)           # brokers count
        
        # Single broker
        response += struct.pack('>i', 0)           # node_id
        host = "localhost"
        response += struct.pack('>h', len(host))   # host length  
        response += host.encode('utf-8')           # host
        response += struct.pack('>i', 9092)        # port
        response += struct.pack('>h', -1)          # rack (null)
        
        # cluster_id (null)
        response += struct.pack('>h', -1)
        
        # controller_id
        response += struct.pack('>i', 0)
        
        # topics array (empty)
        response += struct.pack('>i', 0)
        
        # throttle_time_ms at end for v2
        response += struct.pack('>i', 0)
        
        print(f"Reference v2 response: {len(response)} bytes")
        print(f"Hex: {response.hex()}")
        
        return response

def test_metadata_versions():
    """Test different Metadata API versions"""
    analyzer = KafkaProtocolAnalyzer()
    
    print("Testing Kafka Metadata API versions for protocol compatibility\n")
    
    # Test v2 with empty topics list
    try:
        print("=== Testing Metadata v2 (empty topics) ===")
        payload = struct.pack('>i', 0)  # No topics (empty list)
        payload += struct.pack('?', True)  # allow_auto_topic_creation = true
        
        length, response = analyzer.send_and_capture_response(3, 2, 1, payload)
        print(f"Response received: {length} bytes")
        result_v2_empty = analyzer.analyze_metadata_response_v2(response)
        
    except Exception as e:
        print(f"v2 empty topics failed: {e}")
    
    # Test v2 with null topics (all topics)
    try:
        print("\n=== Testing Metadata v2 (all topics) ===")
        payload = struct.pack('>i', -1)  # Null topics array (all topics)
        payload += struct.pack('?', True)  # allow_auto_topic_creation = true
        
        length, response = analyzer.send_and_capture_response(3, 2, 2, payload)
        print(f"Response received: {length} bytes")
        result_v2_all = analyzer.analyze_metadata_response_v2(response)
        
    except Exception as e:
        print(f"v2 all topics failed: {e}")
    
    # Test v6
    try:
        print("\n=== Testing Metadata v6 ===")
        payload = struct.pack('>i', 0)  # No topics
        payload += struct.pack('?', False)  # allow_auto_topic_creation = false
        
        length, response = analyzer.send_and_capture_response(3, 6, 3, payload)
        print(f"Response received: {length} bytes")
        result_v6 = analyzer.analyze_metadata_response_v6(response)
        
    except Exception as e:
        print(f"v6 failed: {e}")

    # Create reference response
    analyzer.create_reference_response_v2()

def compare_with_apache_kafka():
    """Compare FluxMQ responses with expected Apache Kafka format"""
    print("\n" + "="*60)
    print("PROTOCOL COMPATIBILITY ANALYSIS")
    print("="*60)
    
    # Test kafka-python compatibility directly
    try:
        from kafka.admin.client import KafkaAdminClient
        from kafka.protocol.metadata import MetadataRequest_v2
        
        print("Testing kafka-python parsing directly...")
        
        # Try to create AdminClient with v2
        admin = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='compatibility-test',
            api_version=(0, 10, 1)  # Force older version
        )
        
        print("✅ AdminClient created successfully")
        admin.close()
        
    except Exception as e:
        print(f"❌ AdminClient failed: {e}")
        print("This confirms the protocol incompatibility")

if __name__ == '__main__':
    print("Kafka Protocol Byte-Level Analysis")
    print("=================================")
    
    test_metadata_versions()
    compare_with_apache_kafka()
    
    print("\n" + "="*60)
    print("ANALYSIS COMPLETE")
    print("Check the byte-level differences above to identify the exact protocol issue")