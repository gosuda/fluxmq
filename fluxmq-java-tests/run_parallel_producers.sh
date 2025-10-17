#!/bin/bash

# 병렬 Producer 테스트 스크립트
# 여러 Producer를 동시에 실행하여 서버의 최대 처리 용량 측정

NUM_PRODUCERS=${1:-4}  # 기본 4개의 Producer
DURATION=${2:-10}       # 기본 10초 동안 실행

echo "🚀 병렬 Producer 테스트 시작"
echo "================================"
echo "Producer 개수: $NUM_PRODUCERS"
echo "실행 시간: ${DURATION}초"
echo ""

# 백그라운드로 Producer들을 실행
PIDS=()
for i in $(seq 1 $NUM_PRODUCERS); do
    echo "⚡ Producer #$i 시작..."
    mvn -q exec:java \
        -Dexec.mainClass="com.fluxmq.tests.ContinuousThroughputBenchmark" \
        -Dexec.args="localhost:9092" \
        > /tmp/producer_${i}.log 2>&1 &
    PIDS+=($!)
    sleep 0.5  # 약간의 시간차를 두고 시작
done

echo ""
echo "⏱️  모든 Producer 실행 중... (${DURATION}초 대기)"
echo ""

# 실시간 결과 모니터링 (간단한 버전)
sleep $((DURATION + 5))

echo ""
echo "📊 각 Producer 결과:"
echo "================================"

TOTAL_MESSAGES=0
TOTAL_TIME=0

for i in $(seq 1 $NUM_PRODUCERS); do
    if [ -f "/tmp/producer_${i}.log" ]; then
        echo "Producer #$i:"
        grep -E "평균 처리량|총 확인" /tmp/producer_${i}.log | tail -2 | sed 's/^/  /'

        # 총 메시지 수 추출
        MESSAGES=$(grep "총 확인:" /tmp/producer_${i}.log | tail -1 | awk '{print $3}' | tr -d ',')
        if [ ! -z "$MESSAGES" ]; then
            TOTAL_MESSAGES=$((TOTAL_MESSAGES + MESSAGES))
        fi
    fi
    echo ""
done

# 모든 Producer 종료 대기
for pid in "${PIDS[@]}"; do
    wait $pid 2>/dev/null || true
done

echo "================================"
echo "📊 전체 결과"
echo "================================"
echo "총 Producer 수: $NUM_PRODUCERS"
echo "총 처리 메시지: $(printf "%'d" $TOTAL_MESSAGES)"

if [ $TOTAL_MESSAGES -gt 0 ] && [ ${DURATION} -gt 0 ]; then
    THROUGHPUT=$((TOTAL_MESSAGES / DURATION))
    echo "전체 처리량: $(printf "%'d" $THROUGHPUT) msg/sec"
fi

echo "================================"

# 로그 파일 정리
echo ""
echo "상세 로그: /tmp/producer_*.log"
