#!/usr/bin/env python3
"""
FluxMQ 데이터 복구 검증 테스트 (Consumer)
"""

from kafka import KafkaConsumer
import json

def test_recovery_consumer():
    print("🔍 FluxMQ 데이터 복구 검증 테스트")
    print("=" * 40)
    
    try:
        # 복구된 데이터를 확인하기 위한 Consumer
        consumer = KafkaConsumer(
            'recovery-test-1', 'recovery-test-2',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: m.decode('utf-8') if m else None,
            auto_offset_reset='earliest',  # 모든 메시지 읽기
            consumer_timeout_ms=5000  # 5초 후 타임아웃
        )
        
        print("✅ Consumer 생성 성공")
        print("📖 복구된 메시지 읽기 시작...")
        
        messages_by_topic = {}
        total_messages = 0
        
        for message in consumer:
            topic = message.topic
            if topic not in messages_by_topic:
                messages_by_topic[topic] = []
            
            messages_by_topic[topic].append({
                'key': message.key,
                'value': message.value,
                'partition': message.partition,
                'offset': message.offset
            })
            
            total_messages += 1
            print(f"📥 {topic}: key={message.key}, partition={message.partition}, offset={message.offset}")
            print(f"   값: {message.value}")
        
        consumer.close()
        
        # 결과 요약
        print("\n" + "=" * 40)
        print("📊 복구 결과 요약")
        print("=" * 40)
        
        for topic, messages in messages_by_topic.items():
            print(f"토픽 '{topic}': {len(messages)}개 메시지 복구됨")
            for msg in messages:
                print(f"  - {msg['key']}: offset={msg['offset']}")
        
        print(f"\n총 {total_messages}개 메시지가 복구되었습니다!")
        
        # 예상된 메시지 수와 비교
        expected_messages = 5  # recovery-test-1(2) + recovery-test-2(3)
        if total_messages == expected_messages:
            print("✅ 모든 메시지가 성공적으로 복구되었습니다!")
        else:
            print(f"⚠️ 예상 메시지 수({expected_messages})와 다릅니다. 확인이 필요합니다.")
        
        return total_messages > 0
        
    except Exception as e:
        print(f"❌ Consumer 에러: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_recovery_consumer()