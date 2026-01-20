#!/bin/bash
# FluxMQ vs Kafka 자동 비교 벤치마크 실행 스크립트

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "🚀 FluxMQ vs Kafka 종합 비교 벤치마크"
echo "=============================================="
echo ""

# 결과 디렉토리 생성
mkdir -p "$PROJECT_ROOT/benchmark-suite/results/fluxmq"
mkdir -p "$PROJECT_ROOT/benchmark-suite/results/kafka"

# ========================================
# Phase 1: FluxMQ 벤치마크
# ========================================

echo "📊 Phase 1: FluxMQ 벤치마크"
echo "=============================================="
echo ""

# FluxMQ 시작
echo "1️⃣  FluxMQ 시작 중..."
cd "$PROJECT_ROOT"

# 기존 FluxMQ 프로세스 종료
pkill -9 -f "target/release/fluxmq" > /dev/null 2>&1 || true
sleep 2

# FluxMQ 시작 (로그 최소화 - error 레벨만)
./target/release/fluxmq --log-level error > /dev/null 2>&1 &
FLUXMQ_PID=$!
sleep 3

echo "✅ FluxMQ 시작 완료 (PID: $FLUXMQ_PID)"

# FluxMQ 리소스 모니터링 시작 (백그라운드)
echo ""
echo "2️⃣  FluxMQ 리소스 모니터링 시작..."
"$PROJECT_ROOT/benchmark-suite/monitors/resource_monitor.sh" \
    $FLUXMQ_PID \
    "$PROJECT_ROOT/benchmark-suite/results/fluxmq/resource_usage.csv" \
    120 > /dev/null 2>&1 &
FLUXMQ_MONITOR_PID=$!

# FluxMQ 벤치마크 실행
echo ""
echo "3️⃣  FluxMQ 벤치마크 실행 중..."
cd "$PROJECT_ROOT/fluxmq-java-tests"

mvn -q exec:java \
    -Dexec.mainClass="com.fluxmq.tests.ComprehensiveBenchmark" \
    -Dexec.args="localhost:9092 FluxMQ $FLUXMQ_PID" \
    2>&1 | tee "$PROJECT_ROOT/benchmark-suite/results/fluxmq/benchmark_log.txt"

# 모니터링 종료 대기
wait $FLUXMQ_MONITOR_PID 2>/dev/null || true

echo "✅ FluxMQ 벤치마크 완료"

# FluxMQ 종료
echo ""
echo "4️⃣  FluxMQ 종료 중..."
kill $FLUXMQ_PID > /dev/null 2>&1 || true
sleep 2
echo "✅ FluxMQ 종료 완료"

# ========================================
# Phase 2: Kafka 벤치마크
# ========================================

echo ""
echo ""
echo "📊 Phase 2: Kafka 벤치마크"
echo "=============================================="
echo ""

# Kafka 시작
echo "1️⃣  Kafka 시작 중..."
cd "$PROJECT_ROOT"

# Kafka가 설치되어 있는지 확인
if [ ! -d "kafka" ]; then
    echo "❌ Kafka가 설치되지 않았습니다"
    echo "   설치 스크립트를 먼저 실행하세요:"
    echo "   ./benchmark-suite/setup/install_kafka.sh"
    echo "   ./benchmark-suite/setup/configure_kafka.sh"
    exit 1
fi

# Kafka 시작
./benchmark-suite/setup/start_kafka.sh

# Kafka PID 찾기
KAFKA_PID=$(pgrep -f "kafka.Kafka" || echo "")
if [ -z "$KAFKA_PID" ]; then
    echo "❌ Kafka 시작 실패"
    exit 1
fi

echo "✅ Kafka 시작 완료 (PID: $KAFKA_PID)"

# Kafka 리소스 모니터링 시작 (백그라운드)
echo ""
echo "2️⃣  Kafka 리소스 모니터링 시작..."
"$PROJECT_ROOT/benchmark-suite/monitors/resource_monitor.sh" \
    $KAFKA_PID \
    "$PROJECT_ROOT/benchmark-suite/results/kafka/resource_usage.csv" \
    120 > /dev/null 2>&1 &
KAFKA_MONITOR_PID=$!

# Kafka 벤치마크 실행
echo ""
echo "3️⃣  Kafka 벤치마크 실행 중..."
cd "$PROJECT_ROOT/fluxmq-java-tests"

mvn -q exec:java \
    -Dexec.mainClass="com.fluxmq.tests.ComprehensiveBenchmark" \
    -Dexec.args="localhost:9093 Kafka $KAFKA_PID" \
    2>&1 | tee "$PROJECT_ROOT/benchmark-suite/results/kafka/benchmark_log.txt"

# 모니터링 종료 대기
wait $KAFKA_MONITOR_PID 2>/dev/null || true

echo "✅ Kafka 벤치마크 완료"

# Kafka 종료
echo ""
echo "4️⃣  Kafka 종료 중..."
"$PROJECT_ROOT/benchmark-suite/setup/stop_kafka.sh"
echo "✅ Kafka 종료 완료"

# ========================================
# Phase 3: 결과 분석 및 리포트 생성
# ========================================

echo ""
echo ""
echo "📊 Phase 3: 결과 분석 및 비교 리포트 생성"
echo "=============================================="
echo ""

# 결과 파일 확인
echo "수집된 결과 파일:"
echo "FluxMQ:"
ls -lh "$PROJECT_ROOT/benchmark-suite/results/fluxmq/" 2>/dev/null || echo "  (없음)"
echo ""
echo "Kafka:"
ls -lh "$PROJECT_ROOT/benchmark-suite/results/kafka/" 2>/dev/null || echo "  (없음)"

# 리소스 사용량 요약 표시
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 리소스 사용량 비교"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -f "$PROJECT_ROOT/benchmark-suite/results/fluxmq/resource_usage_summary.txt" ]; then
    echo ""
    echo "🔹 FluxMQ:"
    cat "$PROJECT_ROOT/benchmark-suite/results/fluxmq/resource_usage_summary.txt" | grep "평균"
fi

if [ -f "$PROJECT_ROOT/benchmark-suite/results/kafka/resource_usage_summary.txt" ]; then
    echo ""
    echo "🔹 Kafka:"
    cat "$PROJECT_ROOT/benchmark-suite/results/kafka/resource_usage_summary.txt" | grep "평균"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
echo "✅ 비교 벤치마크 완료!"
echo ""
echo "결과 확인:"
echo "  - FluxMQ 결과: benchmark-suite/results/fluxmq/"
echo "  - Kafka 결과: benchmark-suite/results/kafka/"
echo "  - 리소스 사용량: benchmark-suite/results/*/resource_usage_summary.txt"
echo ""
echo "다음 단계:"
echo "  - 결과 비교: cat benchmark-suite/results/*/single_thread.json"
echo "  - 상세 로그: cat benchmark-suite/results/*/benchmark_log.txt"
echo "  - 리소스 상세: cat benchmark-suite/results/*/resource_usage.csv"
