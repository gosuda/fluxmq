#!/usr/bin/env python3
"""
간단한 kafka-python 디버그 테스트
"""

import logging
import sys
from kafka import KafkaClient
from kafka.errors import KafkaError

# 디버그 로깅 활성화
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

def test_simple_client():
    print("🔍 간단한 KafkaClient 디버그 테스트")
    print("=" * 50)
    
    try:
        # 매우 관대한 타임아웃으로 클라이언트 생성
        client = KafkaClient(
            bootstrap_servers=['localhost:9092'],
            client_id='debug-test',
            request_timeout_ms=60000,  # 60초
            api_version_auto_timeout_ms=60000,  # API 버전 협상에 60초
            connections_max_idle_ms=300000,  # 5분 유휴
        )
        
        print("✅ KafkaClient 생성됨")
        
        # 브로커 준비 상태 확인
        print("📡 브로커 준비 상태 확인 중...")
        ready = client.ready(node_id='bootstrap-0', timeout_ms=30000)
        print(f"브로커 준비 상태: {ready}")
        
        if ready:
            print("✅ 브로커가 준비됨! 메타데이터 요청 시도...")
            # 메타데이터 얻기 시도
            cluster = client.cluster
            print(f"클러스터 정보: {cluster}")
        else:
            print("❌ 브로커가 준비되지 않음")
            
        # 명시적으로 닫기
        client.close()
        print("🔄 클라이언트 닫음")
        
    except Exception as e:
        print(f"❌ 에러 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_simple_client()