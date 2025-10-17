#!/bin/bash

# FluxMQ 벤치마크 정리 스크립트
# 사용법: ./cleanup.sh

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

PROJECT_ROOT="/Users/sonheesung/Documents/GitHub/fluxmq"
JAVA_TESTS_DIR="$PROJECT_ROOT/fluxmq-java-tests"

cd "$PROJECT_ROOT"

log_info "🧹 FluxMQ 벤치마크 정리 시작"
echo "==============================="

# 1. 실행 중인 프로세스 확인
log_info "실행 중인 프로세스 확인..."

FLUXMQ_PIDS=$(pgrep -f fluxmq)
JAVA_PIDS=$(pgrep -f MegaBatchBenchmark)

if [[ -n "$FLUXMQ_PIDS" ]]; then
    echo "FluxMQ 프로세스 발견: $FLUXMQ_PIDS"
else
    echo "FluxMQ 프로세스 없음"
fi

if [[ -n "$JAVA_PIDS" ]]; then
    echo "Java 벤치마크 프로세스 발견: $JAVA_PIDS"
else
    echo "Java 벤치마크 프로세스 없음"
fi

echo ""

# 2. 프로세스 종료 확인
if [[ -n "$FLUXMQ_PIDS" || -n "$JAVA_PIDS" ]]; then
    read -p "실행 중인 프로세스를 종료하시겠습니까? (y/N): " -n 1 -r
    echo ""

    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "프로세스 종료 중..."

        # FluxMQ 종료
        if [[ -n "$FLUXMQ_PIDS" ]]; then
            echo $FLUXMQ_PIDS | xargs kill 2>/dev/null || true
            sleep 2
            # 강제 종료가 필요한 경우
            if pgrep -f fluxmq >/dev/null; then
                echo $FLUXMQ_PIDS | xargs kill -9 2>/dev/null || true
            fi
            log_success "FluxMQ 프로세스 종료됨"
        fi

        # Java 벤치마크 종료
        if [[ -n "$JAVA_PIDS" ]]; then
            echo $JAVA_PIDS | xargs kill 2>/dev/null || true
            sleep 2
            # 강제 종료가 필요한 경우
            if pgrep -f MegaBatchBenchmark >/dev/null; then
                echo $JAVA_PIDS | xargs kill -9 2>/dev/null || true
            fi
            log_success "Java 벤치마크 프로세스 종료됨"
        fi
    else
        log_info "프로세스 종료를 건너뜁니다."
    fi
fi

# 3. 로그 파일 정리
echo ""
log_info "로그 파일 확인..."

# 로그 파일 목록
echo "현재 로그 파일:"
echo ""
echo "🖥️  서버 로그:"
find . -name "fluxmq_server*.log" -type f -exec ls -lh {} \; 2>/dev/null | head -10
echo ""
echo "☕ Java 벤치마크 로그:"
find "$JAVA_TESTS_DIR" -name "java_benchmark*.log" -type f -exec ls -lh {} \; 2>/dev/null | head -10
echo ""
echo "📝 벤치마크 내부 로그:"
find "$JAVA_TESTS_DIR" -name "fluxmq_benchmark*.log" -type f -exec ls -lh {} \; 2>/dev/null | head -10
echo ""

# 로그 파일 개수 및 크기
SERVER_LOG_COUNT=$(find . -name "fluxmq_server*.log" -type f | wc -l)
BENCHMARK_LOG_COUNT=$(find "$JAVA_TESTS_DIR" -name "java_benchmark*.log" -type f | wc -l)
INTERNAL_LOG_COUNT=$(find "$JAVA_TESTS_DIR" -name "fluxmq_benchmark*.log" -type f | wc -l)

TOTAL_SIZE=$(find . "$JAVA_TESTS_DIR" -name "*.log" -type f -exec du -ch {} + 2>/dev/null | tail -1 | cut -f1)

echo "로그 파일 통계:"
echo "  서버 로그: $SERVER_LOG_COUNT 개"
echo "  벤치마크 로그: $BENCHMARK_LOG_COUNT 개"
echo "  내부 로그: $INTERNAL_LOG_COUNT 개"
echo "  총 크기: $TOTAL_SIZE"
echo ""

# 로그 파일 정리 옵션
echo "로그 파일 정리 옵션:"
echo "1. 모든 로그 파일 삭제"
echo "2. 오래된 로그 파일만 삭제 (최근 3개 제외)"
echo "3. 큰 로그 파일만 삭제 (100MB 이상)"
echo "4. 로그 파일 유지"
echo ""

read -p "선택 (1-4): " -n 1 -r
echo ""

case $REPLY in
    1)
        log_warning "모든 로그 파일을 삭제합니다..."
        find . "$JAVA_TESTS_DIR" -name "*.log" -type f -delete 2>/dev/null || true
        log_success "모든 로그 파일 삭제 완료"
        ;;
    2)
        log_info "오래된 로그 파일 삭제 중..."
        # 서버 로그 (최근 3개 제외)
        find . -name "fluxmq_server*.log" -type f -printf '%T@ %p\n' 2>/dev/null | sort -nr | tail -n +4 | cut -d' ' -f2- | xargs rm -f 2>/dev/null || true
        # 벤치마크 로그 (최근 3개 제외)
        find "$JAVA_TESTS_DIR" -name "java_benchmark*.log" -type f -printf '%T@ %p\n' 2>/dev/null | sort -nr | tail -n +4 | cut -d' ' -f2- | xargs rm -f 2>/dev/null || true
        # 내부 로그 (최근 3개 제외)
        find "$JAVA_TESTS_DIR" -name "fluxmq_benchmark*.log" -type f -printf '%T@ %p\n' 2>/dev/null | sort -nr | tail -n +4 | cut -d' ' -f2- | xargs rm -f 2>/dev/null || true
        log_success "오래된 로그 파일 삭제 완료"
        ;;
    3)
        log_info "큰 로그 파일 삭제 중..."
        find . "$JAVA_TESTS_DIR" -name "*.log" -type f -size +100M -delete 2>/dev/null || true
        log_success "큰 로그 파일 삭제 완료"
        ;;
    4)
        log_info "로그 파일을 유지합니다."
        ;;
    *)
        log_warning "잘못된 선택입니다. 로그 파일을 유지합니다."
        ;;
esac

# 4. 임시 파일 정리
echo ""
log_info "임시 파일 정리 중..."

# Java 컴파일 캐시 정리 (선택사항)
if [[ -d "$JAVA_TESTS_DIR/target" ]]; then
    read -p "Java 컴파일 캐시를 정리하시겠습니까? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        cd "$JAVA_TESTS_DIR"
        mvn clean 2>/dev/null || true
        log_success "Java 컴파일 캐시 정리 완료"
        cd "$PROJECT_ROOT"
    fi
fi

# 5. 포트 확인
echo ""
log_info "포트 사용 현황 확인..."
if lsof -i :9092 >/dev/null 2>&1; then
    log_warning "포트 9092가 여전히 사용 중입니다:"
    lsof -i :9092
else
    log_success "포트 9092가 해제되었습니다."
fi

# 6. 최종 상태 확인
echo ""
log_info "최종 상태 확인..."

REMAINING_FLUXMQ=$(pgrep -f fluxmq | wc -l)
REMAINING_JAVA=$(pgrep -f MegaBatchBenchmark | wc -l)
REMAINING_LOGS=$(find . "$JAVA_TESTS_DIR" -name "*.log" -type f | wc -l)

echo "정리 결과:"
echo "  FluxMQ 프로세스: $REMAINING_FLUXMQ 개"
echo "  Java 프로세스: $REMAINING_JAVA 개"
echo "  남은 로그 파일: $REMAINING_LOGS 개"

if [[ $REMAINING_FLUXMQ -eq 0 && $REMAINING_JAVA -eq 0 ]]; then
    log_success "✅ 모든 프로세스가 정리되었습니다."
else
    log_warning "⚠️  일부 프로세스가 여전히 실행 중입니다."
fi

echo ""
log_success "🧹 정리 작업 완료!"