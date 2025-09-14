#!/usr/bin/env python3

"""
DescribeConfigs API Test for FluxMQ
Test the newly implemented DescribeConfigs API (key 32)
"""

from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from kafka.errors import KafkaError

def test_describe_configs():
    """Test DescribeConfigs API"""
    print("🔧 DescribeConfigs API Test")
    print("=" * 50)
    
    try:
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='describe-configs-test'
        )
        
        # Test topic configs
        topic_resource = ConfigResource(ConfigResourceType.TOPIC, 'test-topic')
        broker_resource = ConfigResource(ConfigResourceType.BROKER, '0')
        
        resources = [topic_resource, broker_resource]
        
        print("📋 Requesting configurations for:")
        print(f"  - Topic: test-topic")
        print(f"  - Broker: 0")
        
        # Describe configs
        configs = admin_client.describe_configs(config_resources=resources)
        
        print("\n✅ DescribeConfigs Results:")
        print("-" * 30)
        
        for resource, config in configs.items():
            print(f"\nResource: {resource.resource_type.name} '{resource.name}'")
            print(f"Config entries: {len(config.configs)}")
            
            # Show first few configs
            for i, (config_name, config_entry) in enumerate(config.configs.items()):
                if i < 3:  # Show first 3 configs
                    print(f"  {config_name}: {config_entry.value}")
                    print(f"    read_only: {config_entry.read_only}")
                    print(f"    sensitive: {config_entry.sensitive}")
                    print(f"    source: {config_entry.source}")
        
        print(f"\n🎉 SUCCESS: DescribeConfigs API working correctly!")
        print(f"   - Processed {len(configs)} resources")
        print(f"   - Total configs retrieved: {sum(len(c.configs) for c in configs.values())}")
        
        admin_client.close()
        return True
        
    except Exception as e:
        print(f"⚠️ Error: {e}")
        print(f"   Type: {type(e).__name__}")
        return False

if __name__ == "__main__":
    success = test_describe_configs()
    exit(0 if success else 1)