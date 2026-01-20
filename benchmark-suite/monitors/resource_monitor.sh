#!/bin/bash
# 리소스 사용량 모니터링 스크립트
# 사용법: ./resource_monitor.sh <PID> <OUTPUT_FILE> <DURATION_SECONDS>

if [ $# -lt 3 ]; then
    echo "Usage: $0 <PID> <OUTPUT_FILE> <DURATION_SECONDS>"
    exit 1
fi

PID=$1
OUTPUT_FILE=$2
DURATION=$3
INTERVAL=1  # 1초마다 샘플링

# CSV 헤더 작성
echo "timestamp,cpu_percent,mem_rss_mb,mem_vsz_mb" > "$OUTPUT_FILE"

# 시작 시간
START_TIME=$(date +%s)
SAMPLE_COUNT=0
TOTAL_CPU=0
TOTAL_MEM=0

echo "📊 리소스 모니터링 시작 (PID: $PID, Duration: ${DURATION}s)"

# 프로세스가 존재하는지 확인
if ! ps -p $PID > /dev/null 2>&1; then
    echo "❌ 프로세스 $PID가 존재하지 않습니다"
    exit 1
fi

while [ $(($(date +%s) - START_TIME)) -lt $DURATION ]; do
    # macOS용 ps 명령어
    if ps -p $PID > /dev/null 2>&1; then
        # CPU%, RSS(KB), VSZ(KB)
        STATS=$(ps -p $PID -o %cpu=,rss=,vsz= | tail -1)

        if [ -n "$STATS" ]; then
            CPU=$(echo "$STATS" | awk '{print $1}')
            RSS_KB=$(echo "$STATS" | awk '{print $2}')
            VSZ_KB=$(echo "$STATS" | awk '{print $3}')

            # KB를 MB로 변환
            RSS_MB=$(echo "scale=2; $RSS_KB / 1024" | bc)
            VSZ_MB=$(echo "scale=2; $VSZ_KB / 1024" | bc)

            # 타임스탬프
            TIMESTAMP=$(date +%s)

            # CSV에 기록
            echo "$TIMESTAMP,$CPU,$RSS_MB,$VSZ_MB" >> "$OUTPUT_FILE"

            # 평균 계산용 누적
            SAMPLE_COUNT=$((SAMPLE_COUNT + 1))
            TOTAL_CPU=$(echo "$TOTAL_CPU + $CPU" | bc)
            TOTAL_MEM=$(echo "$TOTAL_MEM + $RSS_MB" | bc)
        fi
    else
        echo "⚠️  프로세스 $PID가 종료되었습니다"
        break
    fi

    sleep $INTERVAL
done

# 평균 계산
if [ $SAMPLE_COUNT -gt 0 ]; then
    AVG_CPU=$(echo "scale=2; $TOTAL_CPU / $SAMPLE_COUNT" | bc)
    AVG_MEM=$(echo "scale=2; $TOTAL_MEM / $SAMPLE_COUNT" | bc)

    echo ""
    echo "✅ 리소스 모니터링 완료"
    echo "   샘플 수: $SAMPLE_COUNT"
    echo "   평균 CPU: ${AVG_CPU}%"
    echo "   평균 메모리: ${AVG_MEM} MB"
    echo ""
    echo "📁 결과 저장: $OUTPUT_FILE"

    # 평균값을 별도 파일에 저장
    SUMMARY_FILE="${OUTPUT_FILE%.csv}_summary.txt"
    cat > "$SUMMARY_FILE" << EOF
리소스 사용량 요약
===================
PID: $PID
측정 기간: ${DURATION}초
샘플 수: $SAMPLE_COUNT

평균 CPU 사용률: ${AVG_CPU}%
평균 메모리 사용량: ${AVG_MEM} MB
EOF

    echo "📁 요약 저장: $SUMMARY_FILE"
fi
