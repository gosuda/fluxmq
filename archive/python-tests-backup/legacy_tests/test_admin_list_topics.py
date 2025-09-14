#!/usr/bin/env python3

"""
Test AdminClient ListTopics functionality directly
"""

from kafka import KafkaAdminClient
import traceback

def test_admin_list_topics():
    print("🔍 Testing AdminClient ListTopics functionality...")
    
    try:
        # Connect using AdminClient
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='fluxmq-test-admin'
        )
        
        print("✅ AdminClient connected successfully")
        
        # List topics using AdminClient
        print("📋 Requesting topic list...")
        topic_metadata = admin_client.list_topics(timeout_ms=5000)
        
        print(f"✅ Found {len(topic_metadata)} topics:")
        for topic in sorted(topic_metadata):
            partitions = topic_metadata[topic]
            print(f"  - {topic}: {len(partitions.partitions)} partitions")
            for partition in partitions.partitions:
                print(f"    - Partition {partition.id}: leader={partition.leader}, replicas={partition.replicas}")
        
        admin_client.close()
        return True
        
    except Exception as e:
        print(f"❌ AdminClient ListTopics test failed: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if test_admin_list_topics():
        print("\n✅ AdminClient ListTopics test completed successfully!")
    else:
        print("\n❌ AdminClient ListTopics test failed!")