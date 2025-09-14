#!/usr/bin/env python3
"""
Minimal AdminClient test to isolate the issue
"""

from kafka.admin.client import KafkaAdminClient
from kafka.admin.config_resource import ConfigResource, ConfigResourceType
import logging
import sys

# Enable all Kafka debug logging
logging.basicConfig(level=logging.DEBUG)
kafka_logger = logging.getLogger('kafka')
kafka_logger.setLevel(logging.DEBUG)

def test_admin_minimal():
    print("=== AdminClient Minimal Test ===")
    
    try:
        print("1. Creating AdminClient...")
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='minimal-admin',
            request_timeout_ms=5000,  # Short timeout for debugging
            api_version=(0, 10, 1)  # Specify older API version
        )
        
        print("2. AdminClient created successfully!")
        
        # Try a simple operation
        print("3. Listing topics...")
        topics = admin_client.list_topics()
        print(f"Topics: {topics}")
        
        admin_client.close()
        
    except Exception as e:
        print(f"AdminClient failed with: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

def test_admin_with_different_versions():
    """Test with different API versions"""
    versions_to_test = [
        None,  # Auto-detect
        (0, 10, 1),  # Old version
        (1, 1, 0),   # Intermediate
        (2, 0, 0),   # Newer
    ]
    
    for version in versions_to_test:
        print(f"\n=== Testing with API version: {version} ===")
        try:
            config = {
                'bootstrap_servers': ['localhost:9092'],
                'client_id': f'test-admin-{version}' if version else 'test-admin-auto',
                'request_timeout_ms': 3000,
            }
            
            if version:
                config['api_version'] = version
            
            admin = KafkaAdminClient(**config)
            print(f"✅ AdminClient created successfully with version {version}")
            admin.close()
            return version
            
        except Exception as e:
            print(f"❌ Failed with version {version}: {e}")
            continue
    
    return None

if __name__ == '__main__':
    print("Testing AdminClient compatibility...")
    
    # First, find a working API version
    working_version = test_admin_with_different_versions()
    
    if working_version:
        print(f"\n✅ Found working version: {working_version}")
    else:
        print("\n❌ No working API version found")
        
    # Test minimal functionality
    print("\n" + "="*50)
    if test_admin_minimal():
        print("✅ AdminClient test passed")
    else:
        print("❌ AdminClient test failed")