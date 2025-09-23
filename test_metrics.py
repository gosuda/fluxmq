#!/usr/bin/env python3

from kafka import KafkaProducer
import time
import sys

def test_producer():
    print("🧪 Testing FluxMQ metrics recording...")

    try:
        # Create producer
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: v.encode('utf-8')
        )

        # Send a batch of messages
        topic = 'metrics-test'
        message_count = 10

        print(f"📤 Sending {message_count} messages to topic '{topic}'...")

        for i in range(message_count):
            message = f"Test message {i+1}"
            producer.send(topic, value=message)

        # Flush to ensure all messages are sent
        producer.flush()
        producer.close()

        print("✅ Messages sent successfully!")
        print("⏳ Waiting 2 seconds for metrics to update...")
        time.sleep(2)

        return True

    except Exception as e:
        print(f"❌ Error sending messages: {e}")
        return False

if __name__ == "__main__":
    success = test_producer()
    sys.exit(0 if success else 1)