#!/bin/bash
# 전체 리소스 사용량 모니터링 스크립트 (프로세스 그룹 전체)
# 사용법: ./resource_monitor_total.sh <PROCESS_NAME> <OUTPUT_FILE> <DURATION_SECONDS>

if [ $# -lt 3 ]; then
    echo "Usage: $0 <PROCESS_NAME> <OUTPUT_FILE> <DURATION_SECONDS>"
    echo "Example: $0 'kafka.Kafka|zookeeper' kafka_total.csv 60"
    exit 1
fi

PROCESS_NAME=$1
OUTPUT_FILE=$2
DURATION=$3
INTERVAL=1

# CSV 헤더 작성
echo "timestamp,total_cpu_percent,total_mem_rss_mb,process_count" > "$OUTPUT_FILE"

echo "📊 전체 리소스 모니터링 시작 (Process: $PROCESS_NAME, Duration: ${DURATION}s)"

START_TIME=$(date +%s)
SAMPLE_COUNT=0
TOTAL_CPU=0
TOTAL_MEM=0

while [ $(($(date +%s) - START_TIME)) -lt $DURATION ]; do
    # 프로세스 패턴에 매칭되는 모든 PID 찾기
    PIDS=$(pgrep -f "$PROCESS_NAME" | grep -v grep)

    if [ -n "$PIDS" ]; then
        CPU_SUM=0
        MEM_SUM=0
        PROC_COUNT=0

        # 각 PID의 리소스 사용량 합산
        for PID in $PIDS; do
            if ps -p $PID > /dev/null 2>&1; then
                STATS=$(ps -p $PID -o %cpu=,rss= 2>/dev/null | tail -1)

                if [ -n "$STATS" ]; then
                    CPU=$(echo "$STATS" | awk '{print $1}')
                    RSS_KB=$(echo "$STATS" | awk '{print $2}')

                    # 숫자 검증
                    if [ -n "$CPU" ] && [ -n "$RSS_KB" ]; then
                        CPU_SUM=$(echo "$CPU_SUM + $CPU" | bc)
                        MEM_SUM=$(echo "$MEM_SUM + $RSS_KB" | bc)
                        PROC_COUNT=$((PROC_COUNT + 1))
                    fi
                fi
            fi
        done

        # KB를 MB로 변환
        if [ $PROC_COUNT -gt 0 ]; then
            MEM_MB=$(echo "scale=2; $MEM_SUM / 1024" | bc)
            TIMESTAMP=$(date +%s)

            # CSV에 기록
            echo "$TIMESTAMP,$CPU_SUM,$MEM_MB,$PROC_COUNT" >> "$OUTPUT_FILE"

            # 평균 계산용 누적
            SAMPLE_COUNT=$((SAMPLE_COUNT + 1))
            TOTAL_CPU=$(echo "$TOTAL_CPU + $CPU_SUM" | bc)
            TOTAL_MEM=$(echo "$TOTAL_MEM + $MEM_MB" | bc)
        fi
    else
        echo "⚠️  프로세스를 찾을 수 없습니다: $PROCESS_NAME"
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
리소스 사용량 요약 (전체 프로세스)
====================================
프로세스 패턴: $PROCESS_NAME
측정 기간: ${DURATION}초
샘플 수: $SAMPLE_COUNT

평균 CPU 사용률: ${AVG_CPU}%
평균 메모리 사용량: ${AVG_MEM} MB

메모리 효율성:
- RSS (실제 물리 메모리): ${AVG_MEM} MB
EOF

    echo "📁 요약 저장: $SUMMARY_FILE"
fi
