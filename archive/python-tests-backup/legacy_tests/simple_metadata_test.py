#!/usr/bin/env python3
"""
Extended Kafka connectivity test focusing on metadata requests
"""

from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import KafkaError
import time

def test_admin_client():
    print("🔧 Testing KafkaAdminClient")
    print("=" * 40)
    
    try:
        # Test admin client which should make metadata requests
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            request_timeout_ms=10000,  # Longer timeout
            client_id='fluxmq-test-admin'
        )
        
        print("✅ AdminClient created successfully")
        
        # Try to list topics - this should trigger metadata request
        print("📋 Attempting to list topics...")
        topics = admin_client.list_consumer_groups(timeout_ms=5000)
        print(f"✅ Got topics: {topics}")
        
        admin_client.close()
        print("🎉 Admin test completed successfully!")
        
    except Exception as e:
        print(f"❌ Admin client error: {e}")
        print(f"   Error type: {type(e)}")

def test_producer_with_metadata():
    print("\n🚀 Testing Producer with explicit metadata")
    print("=" * 40)
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            request_timeout_ms=10000,
            metadata_max_age_ms=1000,  # Force metadata refresh
            client_id='fluxmq-test-producer'
        )
        
        print("✅ Producer created")
        
        # Check if we can get cluster metadata
        print("🔍 Getting cluster metadata...")
        metadata = producer.partitions_for('test-topic')
        print(f"✅ Metadata for 'test-topic': {metadata}")
        
        producer.close()
        print("🎉 Producer metadata test completed!")
        
    except Exception as e:
        print(f"❌ Producer error: {e}")
        print(f"   Error type: {type(e)}")

if __name__ == "__main__":
    test_admin_client()
    test_producer_with_metadata()