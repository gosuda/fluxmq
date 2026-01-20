#!/bin/bash
# Kafka 시작 스크립트

set -e

KAFKA_HOME="$(cd "$(dirname "$0")/../.." && pwd)/kafka"

if [ ! -d "$KAFKA_HOME" ]; then
    echo "❌ Kafka가 설치되지 않았습니다"
    exit 1
fi

echo "🚀 Apache Kafka 시작"
echo "======================================"
echo ""

# ZooKeeper 시작
echo "1️⃣  ZooKeeper 시작 중..."
"$KAFKA_HOME/bin/zookeeper-server-start.sh" -daemon "$KAFKA_HOME/config/zookeeper.properties" > /dev/null 2>&1
sleep 3
echo "✅ ZooKeeper 시작 완료"

# Kafka 브로커 시작
echo ""
echo "2️⃣  Kafka 브로커 시작 중..."
"$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$KAFKA_HOME/config/server.properties" > /dev/null 2>&1
sleep 5
echo "✅ Kafka 브로커 시작 완료"

# 프로세스 확인
echo ""
echo "3️⃣  프로세스 확인 중..."
sleep 2

ZOOKEEPER_PID=$(pgrep -f "org.apache.zookeeper.server.quorum.QuorumPeerMain" || echo "")
KAFKA_PID=$(pgrep -f "kafka.Kafka" || echo "")

if [ -n "$ZOOKEEPER_PID" ]; then
    echo "✅ ZooKeeper 실행 중 (PID: $ZOOKEEPER_PID)"
else
    echo "❌ ZooKeeper 시작 실패"
    exit 1
fi

if [ -n "$KAFKA_PID" ]; then
    echo "✅ Kafka 브로커 실행 중 (PID: $KAFKA_PID)"
else
    echo "❌ Kafka 브로커 시작 실패"
    exit 1
fi

# 토픽 생성
echo ""
echo "4️⃣  벤치마크 토픽 생성 중..."
sleep 2

"$KAFKA_HOME/bin/kafka-topics.sh" --create \
    --bootstrap-server localhost:9093 \
    --topic benchmark-topic \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists 2>/dev/null || true

echo "✅ 토픽 생성 완료"

echo ""
echo "✅ Kafka 준비 완료!"
echo ""
echo "=========================================="
echo "Kafka 연결 정보:"
echo "  - Bootstrap Server: localhost:9093"
echo "  - ZooKeeper: localhost:2181"
echo "  - 토픽: benchmark-topic (3 파티션)"
echo ""
echo "PID 정보:"
echo "  - ZooKeeper: $ZOOKEEPER_PID"
echo "  - Kafka: $KAFKA_PID"
echo ""
echo "로그 확인:"
echo "  - tail -f $KAFKA_HOME/logs/server.log"
echo ""
echo "종료 방법:"
echo "  - ./benchmark-suite/setup/stop_kafka.sh"
echo "=========================================="
