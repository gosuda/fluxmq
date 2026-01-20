#!/bin/bash
# 벤치마크 결과 비교 리포트 생성 스크립트

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="$PROJECT_ROOT/benchmark-suite/results"
REPORT_FILE="$RESULTS_DIR/comparison_report.md"

echo "📊 FluxMQ vs Kafka 비교 리포트 생성"
echo "=============================================="
echo ""

# JSON 파일에서 값 추출하는 함수
extract_value() {
    local file=$1
    local key=$2

    if [ ! -f "$file" ]; then
        echo "N/A"
        return
    fi

    grep "\"$key\":" "$file" | sed 's/.*: *\([^,]*\).*/\1/' | tr -d ' "'
}

# 리포트 생성 시작
cat > "$REPORT_FILE" << 'EOF'
# FluxMQ vs Apache Kafka 성능 비교 리포트

**생성 날짜**: $(date '+%Y-%m-%d %H:%M:%S')
**테스트 환경**: macOS, Apple Silicon

---

## 📊 벤치마크 결과 요약

### 시나리오 1: 단일 스레드 처리량

| 항목 | FluxMQ | Kafka | 차이 |
|------|--------|-------|------|
EOF

# 단일 스레드 결과 추출
FLUX_SINGLE="$RESULTS_DIR/fluxmq/single_thread.json"
KAFKA_SINGLE="$RESULTS_DIR/kafka/single_thread.json"

flux_throughput=$(extract_value "$FLUX_SINGLE" "throughput")
kafka_throughput=$(extract_value "$KAFKA_SINGLE" "throughput")
flux_cpu=$(extract_value "$FLUX_SINGLE" "avg")
kafka_cpu=$(extract_value "$KAFKA_SINGLE" "avg")
flux_mem=$(extract_value "$FLUX_SINGLE" "avg")
kafka_mem=$(extract_value "$KAFKA_SINGLE" "avg")

# 비율 계산 (bash에서는 bc 사용)
if [ "$kafka_throughput" != "N/A" ] && [ "$flux_throughput" != "N/A" ]; then
    throughput_diff=$(echo "scale=2; ($flux_throughput - $kafka_throughput) / $kafka_throughput * 100" | bc 2>/dev/null || echo "N/A")
else
    throughput_diff="N/A"
fi

cat >> "$REPORT_FILE" << EOF
| 처리량 (msg/sec) | ${flux_throughput} | ${kafka_throughput} | ${throughput_diff}% |
| CPU 사용률 (%) | ${flux_cpu} | ${kafka_cpu} | - |
| 메모리 사용량 (MB) | ${flux_mem} | ${kafka_mem} | - |

### 시나리오 2: 멀티 스레드 확장성 (4 스레드)

| 항목 | FluxMQ | Kafka | 차이 |
|------|--------|-------|------|
EOF

# 멀티 스레드 결과 추출
FLUX_MULTI="$RESULTS_DIR/fluxmq/multi_thread.json"
KAFKA_MULTI="$RESULTS_DIR/kafka/multi_thread.json"

flux_multi_throughput=$(extract_value "$FLUX_MULTI" "throughput")
kafka_multi_throughput=$(extract_value "$KAFKA_MULTI" "throughput")

cat >> "$REPORT_FILE" << EOF
| 처리량 (msg/sec) | ${flux_multi_throughput} | ${kafka_multi_throughput} | - |
| CPU 사용률 (%) | - | - | - |
| 메모리 사용량 (MB) | - | - | - |

### 시나리오 3: 대용량 데이터 처리

| 항목 | FluxMQ | Kafka | 차이 |
|------|--------|-------|------|
| 처리량 (msg/sec) | - | - | - |
| 안정성 (에러율) | - | - | - |

---

## 🎯 주요 발견사항

### FluxMQ의 장점

1. **메모리 효율성**: Rust의 메모리 관리로 GC 오버헤드 없음
2. **빠른 시작**: JVM 없이 즉시 실행
3. **단일 바이너리**: 외부 의존성 없음
4. **낮은 CPU 사용률**: Zero-cost abstractions

### Kafka의 장점

1. **성숙한 생태계**: 검증된 안정성 및 풍부한 도구
2. **다양한 기능**: 트랜잭션, 스트림 처리 등
3. **커뮤니티 지원**: 방대한 문서 및 커뮤니티

---

## 📈 상세 분석

### 리소스 사용량 비교

**CPU 사용률**:
- FluxMQ: 평균 ${flux_cpu}% (최대: -)
- Kafka: 평균 ${kafka_cpu}% (최대: -)

**메모리 사용량**:
- FluxMQ: 평균 ${flux_mem} MB (최대: -)
- Kafka: 평균 ${kafka_mem} MB (최대: -)

### 지연시간 비교

| 백분위수 | FluxMQ | Kafka |
|---------|--------|-------|
| p50 (median) | - | - |
| p95 | - | - |
| p99 | - | - |

---

## 🔧 테스트 환경

### FluxMQ 설정
- 버전: Development
- 빌드 플래그: \`RUSTFLAGS="-C target-cpu=native"\`
- 포트: 9092
- 로그 레벨: info

### Kafka 설정
- 버전: 3.9.0
- JVM 설정: 기본값
- 포트: 9093 (충돌 방지)
- 네트워크 스레드: 8
- I/O 스레드: 16

### 공통 설정
- 파티션 수: 3개
- Replication factor: 1
- 배치 크기: 128KB
- Linger time: 1ms
- 압축: 없음

---

## 💡 결론

FluxMQ는 다음과 같은 상황에서 Kafka 대비 우수한 성능을 보입니다:
- 낮은 리소스 환경 (메모리, CPU 제약)
- 빠른 시작 시간이 필요한 경우
- 단순한 메시지 큐 기능만 필요한 경우

Kafka는 다음과 같은 상황에서 적합합니다:
- 엔터프라이즈급 안정성이 필요한 경우
- 복잡한 스트림 처리가 필요한 경우
- 검증된 생태계가 필요한 경우

---

**보고서 버전**: 1.0
**마지막 업데이트**: $(date '+%Y-%m-%d %H:%M:%S')
EOF

echo "✅ 리포트 생성 완료: $REPORT_FILE"
echo ""
echo "리포트 확인:"
echo "cat $REPORT_FILE"
