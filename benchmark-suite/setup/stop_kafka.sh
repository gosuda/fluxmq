#!/bin/bash
# Kafka 종료 스크립트

KAFKA_HOME="$(cd "$(dirname "$0")/../.." && pwd)/kafka"

echo "🛑 Apache Kafka 종료"
echo "======================================"

# Kafka 브로커 종료
if [ -f "$KAFKA_HOME/bin/kafka-server-stop.sh" ]; then
    echo "Kafka 브로커 종료 중..."
    "$KAFKA_HOME/bin/kafka-server-stop.sh" > /dev/null 2>&1
    sleep 3
fi

# ZooKeeper 종료
if [ -f "$KAFKA_HOME/bin/zookeeper-server-stop.sh" ]; then
    echo "ZooKeeper 종료 중..."
    "$KAFKA_HOME/bin/zookeeper-server-stop.sh" > /dev/null 2>&1
    sleep 2
fi

# 강제 종료 (남아있는 프로세스)
echo "남은 프로세스 확인 및 종료..."
pkill -9 -f "kafka.Kafka" > /dev/null 2>&1 || true
pkill -9 -f "org.apache.zookeeper.server.quorum.QuorumPeerMain" > /dev/null 2>&1 || true

sleep 1

echo "✅ Kafka 종료 완료"
