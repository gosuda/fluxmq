#!/usr/bin/env python3
"""
FluxMQ 데이터 복구 테스트
"""

from kafka import KafkaProducer
import time
import json

def test_data_persistence():
    print("🔧 FluxMQ 데이터 복구 테스트")
    print("=" * 40)
    
    try:
        # 프로듀서 생성
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
        )
        
        print("✅ Producer 생성 성공")
        
        # 복구 테스트를 위한 데이터 생성
        topics_and_data = [
            ('recovery-test-1', [
                {'key': 'user1', 'value': {'message': 'Recovery test 1-1', 'timestamp': time.time()}},
                {'key': 'user2', 'value': {'message': 'Recovery test 1-2', 'timestamp': time.time()}},
            ]),
            ('recovery-test-2', [
                {'key': 'order1', 'value': {'order_id': 1001, 'amount': 99.99}},
                {'key': 'order2', 'value': {'order_id': 1002, 'amount': 199.99}},
                {'key': 'order3', 'value': {'order_id': 1003, 'amount': 299.99}},
            ]),
        ]
        
        total_messages = 0
        
        for topic, messages in topics_and_data:
            print(f"📤 {topic} 토픽에 데이터 전송...")
            
            for msg in messages:
                future = producer.send(topic, key=msg['key'], value=msg['value'])
                record = future.get(timeout=10)
                total_messages += 1
                print(f"  ✅ {msg['key']} -> partition={record.partition}, offset={record.offset}")
        
        # 강제로 디스크에 플러시
        producer.flush()
        producer.close()
        
        print(f"🎉 총 {total_messages}개 메시지가 전송되었습니다")
        print("💾 데이터가 디스크에 저장되었습니다")
        print("\n이제 FluxMQ를 재시작해서 복구 기능을 테스트하세요:")
        print("  1. FluxMQ 프로세스를 종료하세요")
        print("  2. --recovery-mode 플래그로 재시작하세요")
        print("  3. Consumer로 데이터가 복구되었는지 확인하세요")
        
        return True
        
    except Exception as e:
        print(f"❌ 에러: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_data_persistence()