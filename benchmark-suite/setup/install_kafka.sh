#!/bin/bash
# Kafka 다운로드 및 설치 스크립트

set -e

KAFKA_VERSION="3.9.0"
SCALA_VERSION="2.13"
KAFKA_DIR="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
KAFKA_ARCHIVE="${KAFKA_DIR}.tgz"
KAFKA_URL="https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}"

echo "🚀 Apache Kafka ${KAFKA_VERSION} 설치 시작"
echo "============================================"
echo ""

# 작업 디렉토리로 이동
cd "$(dirname "$0")/../.."

# 이미 설치되어 있는지 확인
if [ -d "kafka" ]; then
    echo "✅ Kafka가 이미 설치되어 있습니다: ./kafka"
    echo "   삭제 후 재설치하려면: rm -rf kafka"
    exit 0
fi

# Kafka 다운로드
echo "📥 Kafka 다운로드 중..."
if [ ! -f "$KAFKA_ARCHIVE" ]; then
    curl -o "$KAFKA_ARCHIVE" "$KAFKA_URL"
    echo "✅ 다운로드 완료"
else
    echo "✅ 아카이브 파일이 이미 존재합니다"
fi

# 압축 해제
echo ""
echo "📦 압축 해제 중..."
tar -xzf "$KAFKA_ARCHIVE"
mv "$KAFKA_DIR" kafka
echo "✅ 압축 해제 완료"

# 다운로드 파일 정리
rm -f "$KAFKA_ARCHIVE"

echo ""
echo "✅ Kafka 설치 완료!"
echo ""
echo "설치 경로: $(pwd)/kafka"
echo "버전: Apache Kafka ${KAFKA_VERSION}"
echo ""
echo "다음 단계:"
echo "1. Kafka 설정: ./benchmark-suite/setup/configure_kafka.sh"
echo "2. Kafka 시작: ./benchmark-suite/setup/start_kafka.sh"
