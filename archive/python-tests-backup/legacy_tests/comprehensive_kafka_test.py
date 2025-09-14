#!/usr/bin/env python3
"""
포괄적인 kafka-python 호환성 테스트
"""

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic, NewPartitions
from kafka.errors import KafkaError, TopicAlreadyExistsError
import time
import threading
import json

def test_producer_features():
    print("🚀 Producer 기능 테스트")
    print("=" * 50)
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8'),
            acks='all',  # 모든 replica 확인
            retries=3,
            batch_size=16384,
            linger_ms=5
            # Remove compression_type='none' as it's not supported by kafka-python
        )
        
        print("✅ Producer 생성 성공")
        
        # 다양한 메시지 전송 테스트
        test_cases = [
            {"key": "user1", "value": {"message": "Hello FluxMQ", "timestamp": time.time()}},
            {"key": "user2", "value": {"message": "Multiple messages", "counter": 1}},
            {"key": None, "value": {"anonymous": "message without key"}},
            {"key": "user1", "value": {"message": "Same key again", "counter": 2}},
        ]
        
        futures = []
        for i, case in enumerate(test_cases):
            print(f"📤 메시지 {i+1} 전송: key={case['key']}, value={case['value']}")
            future = producer.send('comprehensive-test', 
                                 key=case['key'], 
                                 value=case['value'])
            futures.append(future)
        
        # 모든 메시지 전송 완료 대기
        for i, future in enumerate(futures):
            record_metadata = future.get(timeout=10)
            print(f"✅ 메시지 {i+1} 성공: topic={record_metadata.topic}, "
                  f"partition={record_metadata.partition}, offset={record_metadata.offset}")
        
        producer.flush()
        producer.close()
        print("🎉 Producer 테스트 완료!\n")
        return True
        
    except Exception as e:
        print(f"❌ Producer 에러: {e}")
        return False

def test_consumer_features():
    print("🔍 Consumer 기능 테스트")
    print("=" * 50)
    
    try:
        consumer = KafkaConsumer(
            'comprehensive-test',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: m.decode('utf-8') if m else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='test-consumer-group',
            consumer_timeout_ms=5000  # 5초 후 타임아웃
        )
        
        print("✅ Consumer 생성 성공")
        
        messages_received = 0
        for message in consumer:
            messages_received += 1
            print(f"📥 메시지 수신 {messages_received}: "
                  f"key={message.key}, value={message.value}, "
                  f"partition={message.partition}, offset={message.offset}")
            
            if messages_received >= 4:  # 앞서 전송한 4개 메시지 수신 완료
                break
        
        consumer.close()
        print(f"🎉 Consumer 테스트 완료! {messages_received}개 메시지 수신\n")
        return messages_received > 0
        
    except Exception as e:
        print(f"❌ Consumer 에러: {e}")
        return False

def test_admin_features():
    print("⚙️ Admin 기능 테스트")
    print("=" * 50)
    
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='admin-test'
        )
        
        print("✅ AdminClient 생성 성공")
        
        # 토픽 리스트 조회
        print("📋 기존 토픽 리스트 조회...")
        metadata = admin.list_topics()
        print(f"✅ 토픽 목록: {list(metadata.topics.keys())}")
        
        # 새 토픽 생성 시도
        new_topic = NewTopic(
            name='admin-created-topic',
            num_partitions=2,
            replication_factor=1
        )
        
        print("🏗️ 새 토픽 생성 시도...")
        try:
            result = admin.create_topics([new_topic], timeout_ms=10000)
            for topic, future in result.items():
                future.result()  # 결과 대기
            print(f"✅ 토픽 '{new_topic.name}' 생성 성공")
        except TopicAlreadyExistsError:
            print(f"ℹ️ 토픽 '{new_topic.name}'이 이미 존재함")
        
        admin.close()
        print("🎉 Admin 테스트 완료!\n")
        return True
        
    except Exception as e:
        print(f"❌ Admin 에러: {e}")
        return False

def test_producer_consumer_together():
    print("🔄 Producer-Consumer 동시 테스트")
    print("=" * 50)
    
    try:
        # Consumer를 별도 스레드에서 실행
        received_messages = []
        consumer_error = []
        
        def consume_messages():
            try:
                consumer = KafkaConsumer(
                    'realtime-test',
                    bootstrap_servers=['localhost:9092'],
                    value_deserializer=lambda m: m.decode('utf-8'),
                    auto_offset_reset='latest',  # 새 메시지만 수신
                    group_id='realtime-group',
                    consumer_timeout_ms=8000
                )
                
                for message in consumer:
                    received_messages.append(message.value)
                    print(f"📥 실시간 수신: {message.value}")
                    
                consumer.close()
            except Exception as e:
                consumer_error.append(str(e))
        
        # Consumer 시작
        consumer_thread = threading.Thread(target=consume_messages)
        consumer_thread.start()
        
        # 잠시 대기 후 메시지 전송
        time.sleep(1)
        
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: v.encode('utf-8')
        )
        
        # 실시간 메시지 전송
        for i in range(3):
            message = f"실시간 메시지 {i+1} - {time.time()}"
            print(f"📤 실시간 전송: {message}")
            producer.send('realtime-test', value=message)
            time.sleep(1)
        
        producer.flush()
        producer.close()
        
        # Consumer 완료 대기
        consumer_thread.join(timeout=10)
        
        if consumer_error:
            print(f"❌ Consumer 에러: {consumer_error[0]}")
            return False
        
        print(f"🎉 실시간 테스트 완료! {len(received_messages)}개 메시지 처리\n")
        return len(received_messages) > 0
        
    except Exception as e:
        print(f"❌ 실시간 테스트 에러: {e}")
        return False

def test_partition_features():
    print("📊 파티션 기능 테스트")
    print("=" * 50)
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: v.encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8')
        )
        
        # 특정 파티션에 메시지 전송
        for partition in [0, 1, 2]:
            message = f"파티션 {partition} 전용 메시지"
            future = producer.send('partition-test', value=message, partition=partition)
            record = future.get(timeout=10)
            print(f"✅ 파티션 {partition} 전송 성공: offset={record.offset}")
        
        # 키 기반 파티셔닝 테스트
        keys = ['user1', 'user2', 'user3', 'user1', 'user2']
        for key in keys:
            message = f"키 '{key}'의 메시지"
            future = producer.send('partition-test', key=key, value=message)
            record = future.get(timeout=10)
            print(f"✅ 키 '{key}' -> 파티션 {record.partition}, offset={record.offset}")
        
        producer.close()
        print("🎉 파티션 테스트 완료!\n")
        return True
        
    except Exception as e:
        print(f"❌ 파티션 테스트 에러: {e}")
        return False

def main():
    print("🧪 FluxMQ Kafka-Python 호환성 종합 테스트")
    print("=" * 60)
    print()
    
    tests = [
        ("Producer 기능", test_producer_features),
        ("Consumer 기능", test_consumer_features), 
        ("Admin 기능", test_admin_features),
        ("실시간 Producer-Consumer", test_producer_consumer_together),
        ("파티션 기능", test_partition_features),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"🔍 {test_name} 테스트 시작...")
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} 테스트 예외: {e}")
            results[test_name] = False
        
        time.sleep(2)  # 테스트 간 간격
    
    # 결과 요약
    print("=" * 60)
    print("📊 테스트 결과 요약")
    print("=" * 60)
    
    passed = 0
    total = len(results)
    
    for test_name, passed_test in results.items():
        status = "✅ 성공" if passed_test else "❌ 실패"
        print(f"{test_name}: {status}")
        if passed_test:
            passed += 1
    
    print("=" * 60)
    print(f"전체 결과: {passed}/{total} 테스트 통과 ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("🎉 모든 테스트 성공! FluxMQ는 kafka-python과 100% 호환됩니다!")
    else:
        print(f"⚠️  {total-passed}개 테스트 실패. 추가 작업이 필요합니다.")

if __name__ == "__main__":
    main()