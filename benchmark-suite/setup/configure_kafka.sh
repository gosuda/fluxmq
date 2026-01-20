#!/bin/bash
# Kafka 성능 최적화 설정 스크립트

set -e

KAFKA_HOME="$(cd "$(dirname "$0")/../.." && pwd)/kafka"

if [ ! -d "$KAFKA_HOME" ]; then
    echo "❌ Kafka가 설치되지 않았습니다"
    echo "   먼저 설치 스크립트를 실행하세요: ./benchmark-suite/setup/install_kafka.sh"
    exit 1
fi

echo "⚙️  Kafka 성능 최적화 설정"
echo "======================================"
echo ""

# server.properties 백업
if [ ! -f "$KAFKA_HOME/config/server.properties.bak" ]; then
    cp "$KAFKA_HOME/config/server.properties" "$KAFKA_HOME/config/server.properties.bak"
    echo "✅ 원본 설정 백업 완료"
fi

# 최적화된 server.properties 생성
cat > "$KAFKA_HOME/config/server.properties" << 'EOF'
# Broker ID
broker.id=0

# 리스너 설정
listeners=PLAINTEXT://:9093
advertised.listeners=PLAINTEXT://localhost:9093

# 로그 디렉토리
log.dirs=/tmp/kafka-logs

# 파티션 설정
num.network.threads=8
num.io.threads=16
num.partitions=3
default.replication.factor=1

# 소켓 버퍼 크기 (성능 최적화)
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# 로그 설정
log.segment.bytes=268435456
log.retention.hours=1
log.retention.check.interval.ms=300000

# 로그 정리 설정
log.cleaner.enable=false

# ZooKeeper 연결
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000

# 그룹 코디네이터 설정
group.initial.rebalance.delay.ms=0

# 성능 최적화
num.replica.fetchers=4
replica.fetch.max.bytes=1048576
replica.socket.receive.buffer.bytes=65536
replica.socket.timeout.ms=30000
compression.type=uncompressed

# 메시지 크기 제한
message.max.bytes=1000012
replica.fetch.max.bytes=1048576
EOF

echo "✅ server.properties 최적화 완료"

# log4j 로깅 최소화 설정
cat > "$KAFKA_HOME/config/log4j.properties" << 'EOF'
# Root logger option - ERROR 레벨로 최소화
log4j.rootLogger=ERROR, stdout

# Console appender
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=[%d] %p %m (%c)%n

# Kafka 관련 로거 - ERROR만
log4j.logger.kafka=ERROR
log4j.logger.org.apache.kafka=ERROR
log4j.logger.kafka.controller=ERROR
log4j.logger.kafka.log.LogCleaner=ERROR
log4j.logger.state.change.logger=ERROR
log4j.logger.kafka.producer.async.DefaultEventHandler=ERROR
log4j.logger.kafka.request.logger=ERROR
log4j.logger.kafka.network.RequestChannel=ERROR
log4j.logger.kafka.network.Processor=ERROR
log4j.logger.kafka.server.KafkaApis=ERROR
log4j.logger.kafka.network.ConnectionQuotas=ERROR
log4j.logger.kafka.coordinator.transaction=ERROR
log4j.logger.kafka.authorizer.logger=ERROR

# ZooKeeper 관련 로거 - ERROR만
log4j.logger.org.apache.zookeeper=ERROR
log4j.logger.org.I0Itec.zkclient=ERROR
EOF

echo "✅ log4j.properties 로깅 최소화 완료"

# ZooKeeper 설정 (기본값 사용)
echo "✅ ZooKeeper 설정 유지 (기본값)"

echo ""
echo "✅ Kafka 설정 완료!"
echo ""
echo "설정 파일: $KAFKA_HOME/config/server.properties"
echo ""
echo "주요 최적화 항목:"
echo "- 포트: 9093 (FluxMQ 9092와 충돌 방지)"
echo "- 네트워크 스레드: 8개"
echo "- I/O 스레드: 16개"
echo "- 파티션 수: 3개 (FluxMQ와 동일)"
echo "- 압축: 없음 (FluxMQ와 동일)"
echo ""
echo "다음 단계:"
echo "./benchmark-suite/setup/start_kafka.sh"
