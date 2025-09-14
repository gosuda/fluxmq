#!/usr/bin/env python3
"""
Detailed Protocol Debug Tool
Step-by-step parsing to identify exact failure point
"""

import socket
import struct
import io

def detailed_metadata_v2_analysis():
    """Analyze exactly where v2 response parsing fails"""
    
    # Get the raw response
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 9092))
    
    # Send v2 metadata request for all topics
    api_key = 3
    api_version = 2
    correlation_id = 123
    client_id = "debug-tool"
    
    request = struct.pack('>h', api_key)
    request += struct.pack('>h', api_version)
    request += struct.pack('>i', correlation_id)
    request += struct.pack('>h', len(client_id))
    request += client_id.encode('utf-8')
    request += struct.pack('>i', -1)  # All topics (NULL)
    request += struct.pack('?', True)  # allow_auto_topic_creation
    
    message = struct.pack('>i', len(request)) + request
    sock.send(message)
    
    response_length = struct.unpack('>i', sock.recv(4))[0]
    response = sock.recv(response_length)
    sock.close()
    
    print(f"=== Detailed v2 Response Analysis ===")
    print(f"Total response: {len(response)} bytes")
    print(f"Full hex: {response.hex()}")
    
    cursor = io.BytesIO(response)
    
    # Parse step by step
    correlation_id = struct.unpack('>i', cursor.read(4))[0]
    print(f"✅ Correlation ID: {correlation_id}")
    
    brokers_count = struct.unpack('>i', cursor.read(4))[0]
    print(f"✅ Brokers count: {brokers_count}")
    
    # Parse brokers
    for i in range(brokers_count):
        node_id = struct.unpack('>i', cursor.read(4))[0]
        host_len = struct.unpack('>h', cursor.read(2))[0]
        host = cursor.read(host_len).decode('utf-8')
        port = struct.unpack('>i', cursor.read(4))[0]
        rack_len = struct.unpack('>h', cursor.read(2))[0]
        rack = cursor.read(rack_len).decode('utf-8') if rack_len > 0 else None
        print(f"✅ Broker {i}: node_id={node_id}, host={host}, port={port}, rack={rack}")
    
    # v2+ has cluster_id
    cluster_id_len = struct.unpack('>h', cursor.read(2))[0]
    cluster_id = cursor.read(cluster_id_len).decode('utf-8') if cluster_id_len > 0 else None
    print(f"✅ Cluster ID: {cluster_id} (length: {cluster_id_len})")
    
    # v1+ has controller_id  
    controller_id = struct.unpack('>i', cursor.read(4))[0]
    print(f"✅ Controller ID: {controller_id}")
    
    # Topics array
    topics_count = struct.unpack('>i', cursor.read(4))[0]
    print(f"✅ Topics count: {topics_count}")
    print(f"Position after topics count: {cursor.tell()}")
    
    # Parse each topic
    for topic_idx in range(topics_count):
        print(f"\n--- Topic {topic_idx} ---")
        topic_start_pos = cursor.tell()
        print(f"Topic start position: {topic_start_pos}")
        
        try:
            # Topic error code
            error_code = struct.unpack('>h', cursor.read(2))[0]
            print(f"✅ Error code: {error_code}")
            
            # Topic name
            name_len = struct.unpack('>h', cursor.read(2))[0]
            topic_name = cursor.read(name_len).decode('utf-8')
            print(f"✅ Topic name: '{topic_name}' (len={name_len})")
            
            # is_internal (v1+)
            is_internal = cursor.read(1)[0] != 0
            print(f"✅ Is internal: {is_internal}")
            
            # Partitions array
            partition_count = struct.unpack('>i', cursor.read(4))[0]
            print(f"✅ Partitions count: {partition_count}")
            
            # Parse partitions
            for part_idx in range(partition_count):
                print(f"  Partition {part_idx}:")
                part_error_code = struct.unpack('>h', cursor.read(2))[0]
                partition_id = struct.unpack('>i', cursor.read(4))[0]
                leader = struct.unpack('>i', cursor.read(4))[0]
                leader_epoch = struct.unpack('>i', cursor.read(4))[0]
                
                print(f"    Error: {part_error_code}, ID: {partition_id}, Leader: {leader}, Epoch: {leader_epoch}")
                
                # Replica nodes array
                replica_count = struct.unpack('>i', cursor.read(4))[0]
                print(f"    Replicas count: {replica_count}")
                for r in range(replica_count):
                    replica_id = struct.unpack('>i', cursor.read(4))[0]
                    print(f"      Replica {r}: {replica_id}")
                
                # ISR nodes array  
                isr_count = struct.unpack('>i', cursor.read(4))[0]
                print(f"    ISR count: {isr_count}")
                for r in range(isr_count):
                    isr_id = struct.unpack('>i', cursor.read(4))[0]
                    print(f"      ISR {r}: {isr_id}")
                
                # v5+ has offline replicas - but this is v2, so skip
                
        except Exception as e:
            print(f"❌ PARSING FAILED at topic {topic_idx}, position {cursor.tell()}")
            print(f"Error: {e}")
            
            # Show remaining bytes
            remaining = response[cursor.tell():]
            print(f"Remaining bytes ({len(remaining)}): {remaining[:100].hex()}...")
            break
    
    # Check for throttle_time_ms at end (v2)
    if cursor.tell() < len(response):
        remaining_bytes = len(response) - cursor.tell()
        print(f"\nRemaining bytes: {remaining_bytes}")
        if remaining_bytes >= 4:
            throttle_time = struct.unpack('>i', response[-4:])[0]
            print(f"✅ Throttle time at end: {throttle_time}")

if __name__ == '__main__':
    detailed_metadata_v2_analysis()