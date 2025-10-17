#!/bin/bash

# FluxMQ 벤치마크 자동 실행 스크립트
# 사용법: ./run_benchmark.sh

set -e  # 에러 발생 시 스크립트 종료

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 로그 함수
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 현재 디렉토리 확인
PROJECT_ROOT="/Users/sonheesung/Documents/GitHub/fluxmq"
JAVA_TESTS_DIR="$PROJECT_ROOT/fluxmq-java-tests"

if [[ ! -d "$PROJECT_ROOT" ]]; then
    log_error "FluxMQ 프로젝트 디렉토리를 찾을 수 없습니다: $PROJECT_ROOT"
    exit 1
fi

cd "$PROJECT_ROOT"

log_info "🚀 FluxMQ 벤치마크 자동 실행 시작"
echo "========================================"

# 1. 기존 프로세스 정리
log_info "기존 프로세스 정리 중..."
pkill -f fluxmq 2>/dev/null || true
pkill -f MegaBatchBenchmark 2>/dev/null || true
sleep 2

# 2. FluxMQ 빌드 확인
log_info "FluxMQ 바이너리 확인 중..."
if [[ ! -f "target/release/fluxmq" ]]; then
    log_warning "FluxMQ 바이너리가 없습니다. 빌드를 시작합니다..."
    env RUSTFLAGS="-C target-cpu=native" cargo build --release
    log_success "FluxMQ 빌드 완료"
else
    log_success "FluxMQ 바이너리 확인됨"
fi

# 3. Java 의존성 확인
log_info "Java 의존성 확인 중..."
cd "$JAVA_TESTS_DIR"
if [[ ! -d "target/dependency" ]] || [[ ! -f "target/dependency/kafka-clients-4.1.0.jar" ]]; then
    log_warning "Java 의존성이 없습니다. 다운로드를 시작합니다..."
    mvn dependency:copy-dependencies compile
    log_success "Java 의존성 설치 완료"
else
    log_success "Java 의존성 확인됨"
fi

cd "$PROJECT_ROOT"

# 4. FluxMQ 서버 시작
log_info "FluxMQ 서버 시작 중..."
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SERVER_LOG="fluxmq_server_$TIMESTAMP.log"

env RUSTFLAGS="-C target-cpu=native" ./target/release/fluxmq \
    --port 9092 \
    --enable-consumer-groups \
    --log-level info > "$SERVER_LOG" 2>&1 &

SERVER_PID=$!
log_success "FluxMQ 서버 시작됨 (PID: $SERVER_PID, 로그: $SERVER_LOG)"

# 5. 서버 준비 대기
log_info "서버 준비 상태 확인 중..."
sleep 5

# 서버 상태 확인
if ! kill -0 $SERVER_PID 2>/dev/null; then
    log_error "FluxMQ 서버가 시작되지 않았습니다. 로그를 확인하세요:"
    tail -10 "$SERVER_LOG"
    exit 1
fi

# 포트 확인
if ! lsof -i :9092 >/dev/null 2>&1; then
    log_error "포트 9092가 열리지 않았습니다."
    exit 1
fi

log_success "FluxMQ 서버 준비 완료"

# 6. Java 벤치마크 실행
log_info "Java 벤치마크 시작 중..."
cd "$JAVA_TESTS_DIR"

BENCHMARK_LOG="java_benchmark_$TIMESTAMP.log"
java -cp "target/classes:target/dependency/*" \
    com.fluxmq.tests.MegaBatchBenchmark localhost:9092 > "$BENCHMARK_LOG" 2>&1 &

BENCHMARK_PID=$!
log_success "Java 벤치마크 시작됨 (PID: $BENCHMARK_PID, 로그: $BENCHMARK_LOG)"

# 7. 모니터링 시작
log_info "벤치마크 모니터링 시작..."
echo ""
echo "실시간 로그 확인:"
echo "  서버 로그: tail -f $PROJECT_ROOT/$SERVER_LOG"
echo "  벤치마크 로그: tail -f $JAVA_TESTS_DIR/$BENCHMARK_LOG"
echo ""
echo "프로세스 ID:"
echo "  FluxMQ 서버: $SERVER_PID"
echo "  Java 벤치마크: $BENCHMARK_PID"
echo ""

# 8. 벤치마크 완료 대기 (옵션)
read -p "벤치마크 완료까지 대기하시겠습니까? (y/N): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    log_info "벤치마크 완료 대기 중... (Ctrl+C로 중단 가능)"

    while kill -0 $BENCHMARK_PID 2>/dev/null; do
        sleep 10
        echo -n "."
    done

    echo ""
    log_success "벤치마크 완료!"

    # 결과 출력
    echo ""
    echo "========== 벤치마크 결과 =========="
    grep -A 10 "🏆 메가 배치 벤치마크 결과" "$BENCHMARK_LOG" || echo "결과를 찾을 수 없습니다."
    echo "================================="

    # 서버 종료 여부 확인
    read -p "FluxMQ 서버를 종료하시겠습니까? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        kill $SERVER_PID 2>/dev/null || true
        log_success "FluxMQ 서버 종료됨"
    fi
else
    log_info "벤치마크가 백그라운드에서 실행 중입니다."
    echo ""
    echo "진행 상황 확인: tail -f $JAVA_TESTS_DIR/$BENCHMARK_LOG"
    echo "서버 종료: kill $SERVER_PID"
    echo "벤치마크 종료: kill $BENCHMARK_PID"
fi

log_success "벤치마크 실행 스크립트 완료"