#!/bin/bash

# FluxMQ 벤치마크 로그 모니터링 스크립트
# 사용법: ./monitor_logs.sh

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

PROJECT_ROOT="/Users/sonheesung/Documents/GitHub/fluxmq"
JAVA_TESTS_DIR="$PROJECT_ROOT/fluxmq-java-tests"

cd "$PROJECT_ROOT"

log_info "📊 FluxMQ 벤치마크 로그 모니터링"
echo "=================================="

# 최신 로그 파일 찾기
echo "사용 가능한 로그 파일:"
echo ""

# 서버 로그
echo "🖥️  FluxMQ 서버 로그:"
find . -name "fluxmq_server*.log" -type f -exec ls -lht {} \; | head -5
echo ""

# 벤치마크 로그
echo "☕ Java 벤치마크 로그:"
find "$JAVA_TESTS_DIR" -name "java_benchmark*.log" -type f -exec ls -lht {} \; | head -5
echo ""

# 벤치마크 내부 로그
echo "📝 벤치마크 내부 로그:"
find "$JAVA_TESTS_DIR" -name "fluxmq_benchmark*.log" -type f -exec ls -lht {} \; | head -5
echo ""

# 최신 파일들 자동 감지
LATEST_SERVER_LOG=$(find . -name "fluxmq_server*.log" -type f -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2)
LATEST_BENCHMARK_LOG=$(find "$JAVA_TESTS_DIR" -name "java_benchmark*.log" -type f -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2)

if [[ -n "$LATEST_SERVER_LOG" ]]; then
    log_success "최신 서버 로그: $LATEST_SERVER_LOG"
fi

if [[ -n "$LATEST_BENCHMARK_LOG" ]]; then
    log_success "최신 벤치마크 로그: $LATEST_BENCHMARK_LOG"
fi

echo ""
echo "모니터링 옵션을 선택하세요:"
echo "1. 최신 서버 로그 실시간 모니터링"
echo "2. 최신 벤치마크 로그 실시간 모니터링"
echo "3. 두 로그 동시 모니터링"
echo "4. 벤치마크 결과만 확인"
echo "5. 프로세스 상태 확인"
echo "6. 로그 파일 수동 선택"
echo "q. 종료"
echo ""

while true; do
    read -p "선택 (1-6, q): " choice
    case $choice in
        1)
            if [[ -n "$LATEST_SERVER_LOG" ]]; then
                log_info "서버 로그 모니터링 시작: $LATEST_SERVER_LOG"
                tail -f "$LATEST_SERVER_LOG"
            else
                log_warning "서버 로그 파일을 찾을 수 없습니다."
            fi
            ;;
        2)
            if [[ -n "$LATEST_BENCHMARK_LOG" ]]; then
                log_info "벤치마크 로그 모니터링 시작: $LATEST_BENCHMARK_LOG"
                tail -f "$LATEST_BENCHMARK_LOG"
            else
                log_warning "벤치마크 로그 파일을 찾을 수 없습니다."
            fi
            ;;
        3)
            if [[ -n "$LATEST_SERVER_LOG" && -n "$LATEST_BENCHMARK_LOG" ]]; then
                log_info "두 로그 동시 모니터링 시작..."
                echo "서버 로그: $LATEST_SERVER_LOG"
                echo "벤치마크 로그: $LATEST_BENCHMARK_LOG"
                echo ""
                tail -f "$LATEST_SERVER_LOG" "$LATEST_BENCHMARK_LOG"
            else
                log_warning "로그 파일을 찾을 수 없습니다."
            fi
            ;;
        4)
            log_info "벤치마크 결과 확인 중..."
            echo ""
            if [[ -n "$LATEST_BENCHMARK_LOG" ]]; then
                echo "========== 벤치마크 결과 =========="
                grep -A 15 "🏆 메가 배치 벤치마크 결과" "$LATEST_BENCHMARK_LOG" 2>/dev/null || {
                    echo "아직 결과가 없습니다. 진행 상황 확인:"
                    tail -10 "$LATEST_BENCHMARK_LOG"
                }
                echo "================================="
            else
                log_warning "벤치마크 로그 파일을 찾을 수 없습니다."
            fi
            echo ""
            ;;
        5)
            log_info "프로세스 상태 확인 중..."
            echo ""
            echo "🖥️  FluxMQ 서버 프로세스:"
            ps aux | grep fluxmq | grep -v grep || echo "FluxMQ 프로세스가 실행 중이지 않습니다."
            echo ""
            echo "☕ Java 벤치마크 프로세스:"
            ps aux | grep MegaBatchBenchmark | grep -v grep || echo "Java 벤치마크가 실행 중이지 않습니다."
            echo ""
            echo "🔌 포트 9092 사용 현황:"
            lsof -i :9092 || echo "포트 9092가 사용되지 않고 있습니다."
            echo ""
            ;;
        6)
            echo ""
            echo "로그 파일을 선택하세요:"
            select log_file in $(find . "$JAVA_TESTS_DIR" -name "*.log" -type f | sort -r); do
                if [[ -n "$log_file" ]]; then
                    log_info "선택된 로그 파일: $log_file"
                    echo "실시간 모니터링을 시작합니다..."
                    tail -f "$log_file"
                else
                    log_warning "잘못된 선택입니다."
                fi
                break
            done
            ;;
        q|Q)
            log_info "모니터링을 종료합니다."
            exit 0
            ;;
        *)
            log_warning "잘못된 선택입니다. 1-6 또는 q를 입력하세요."
            ;;
    esac
    echo ""
    echo "다른 작업을 수행하려면 옵션을 선택하세요 (Ctrl+C로 중단 후 재선택):"
done