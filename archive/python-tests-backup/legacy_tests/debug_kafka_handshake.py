#!/usr/bin/env python3
"""
Debug kafka-python handshake behavior
"""

from kafka.client_async import KafkaClient
from kafka.protocol import api
from kafka.conn import BrokerConnection
import socket
import logging
import sys

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('kafka')
logger.setLevel(logging.DEBUG)

# Create console handler
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def debug_connection():
    print("🔍 Debug Kafka-Python Connection Behavior")
    print("=" * 60)
    
    try:
        # Direct BrokerConnection test
        print("Testing BrokerConnection directly...")
        conn = BrokerConnection('localhost', 9092, 
                              request_timeout_ms=30000,
                              api_version=(0, 10, 0))  # Try lower API version
        
        print(f"Connection state: {conn.state}")
        print(f"Connection connecting: {conn.connecting()}")
        print(f"Connection connected: {conn.connected()}")
        
        # Try to connect
        print("Attempting connection...")
        if conn.connect() in (conn.CONNECTING, conn.CONNECTED):
            print("✅ Connection initiated")
            
            # Try to get connection ready
            import time
            for i in range(10):
                print(f"Attempt {i+1}: Connection state: {conn.state}")
                if conn.connected():
                    print("✅ Connection successful!")
                    break
                time.sleep(0.5)
                conn.connect()  # Continue connecting process
            else:
                print("❌ Connection never became ready")
        else:
            print("❌ Connection failed to initiate")
            
    except Exception as e:
        print(f"❌ BrokerConnection error: {e}")
        import traceback
        traceback.print_exc()

def debug_client_minimal():
    print("\n🔍 Debug Minimal KafkaClient")
    print("=" * 60)
    
    try:
        # Minimal KafkaClient setup
        client = KafkaClient(
            bootstrap_servers=['localhost:9092'],
            api_version=(0, 10, 0),  # Try compatible version
            request_timeout_ms=30000,
            connections_max_idle_ms=600000,  # Long idle timeout
            reconnect_backoff_ms=100,
            reconnect_backoff_max_ms=1000,
        )
        
        print("Client created, checking bootstrap...")
        
        # Try bootstrap manually
        print("Calling bootstrap manually...")
        client.bootstrap()
        
        print(f"Cluster metadata: {client.cluster}")
        print(f"Cluster brokers: {list(client.cluster.brokers())}")
        
    except Exception as e:
        print(f"❌ Minimal client error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_connection()
    debug_client_minimal()