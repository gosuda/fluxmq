# FluxMQ vs Kafka 종합 벤치마크 테스트 스위트

FluxMQ와 Apache Kafka를 다각도로 비교하는 자동화된 벤치마크 시스템입니다.

## 📊 측정 항목

### 1. 성능 (Performance)
- ✅ 처리량 (Throughput): msg/sec, MB/sec
- ✅ 지연시간 (Latency): p50, p95, p99
- ✅ 확장성 (Scalability): 단일 vs 멀티 스레드

### 2. 리소스 사용량 (Resource Usage)
- ✅ CPU 사용률: 평균, 최대, 최소
- ✅ 메모리 사용량: RSS, 평균, 피크
- ⏭️ 디스크 I/O: 읽기/쓰기 속도
- ⏭️ 네트워크 I/O: 대역폭 사용량

### 3. 안정성 (Reliability)
- ✅ 에러율: 메시지 손실률
- ⏭️ 복구 시간: 크래시 후 재시작
- ⏭️ 장시간 안정성: 24시간 연속 운영

---

## 🚀 빠른 시작

### 필요 사항

- FluxMQ 빌드 완료 (`cargo build --release`)
- Java 11+ 설치
- Maven 설치
- macOS 또는 Linux (ps 명령어 지원 필요)

### 1단계: Kafka 설치 및 설정

```bash
# Kafka 다운로드 및 설치
./benchmark-suite/setup/install_kafka.sh

# Kafka 성능 최적화 설정
./benchmark-suite/setup/configure_kafka.sh
```

### 2단계: 자동 비교 벤치마크 실행

```bash
# FluxMQ vs Kafka 전체 비교 실행
./benchmark-suite/runners/run_comparison.sh
```

이 명령은 다음을 자동으로 수행합니다:
1. FluxMQ 시작 → 벤치마크 실행 → 종료
2. Kafka 시작 → 벤치마크 실행 → 종료
3. 결과를 JSON 파일로 저장

### 3단계: 결과 확인

```bash
# 비교 리포트 생성
./benchmark-suite/analyzers/generate_report.sh

# 리포트 확인
cat benchmark-suite/results/comparison_report.md

# JSON 결과 확인
cat benchmark-suite/results/fluxmq/single_thread.json
cat benchmark-suite/results/kafka/single_thread.json
```

---

## 📁 디렉토리 구조

```
benchmark-suite/
├── setup/
│   ├── install_kafka.sh          # Kafka 다운로드 및 설치
│   ├── configure_kafka.sh        # Kafka 최적 설정 적용
│   ├── start_kafka.sh            # Kafka 시작
│   └── stop_kafka.sh             # Kafka 종료
├── runners/
│   └── run_comparison.sh         # 자동 비교 벤치마크 실행
├── analyzers/
│   └── generate_report.sh        # 비교 리포트 생성
└── results/
    ├── fluxmq/                   # FluxMQ 결과 JSON
    ├── kafka/                    # Kafka 결과 JSON
    └── comparison_report.md      # 비교 리포트
```

---

## 🧪 테스트 시나리오

### 시나리오 1: 단일 스레드 처리량
- 1개 프로듀서, 3개 파티션
- 100,000 메시지 (~800 bytes/msg)
- 측정: 처리량, CPU, 메모리

### 시나리오 2: 멀티 스레드 확장성
- 4개 프로듀서, 3개 파티션
- 100,000 메시지 (각 프로듀서당)
- 측정: 선형 확장성, 리소스 효율

### 시나리오 3: 대용량 데이터 처리
- 4개 프로듀서
- 500,000 메시지
- 측정: 장시간 안정성, 메모리 누수

---

## 🔧 수동 실행 방법

### FluxMQ 벤치마크만 실행

```bash
# FluxMQ 시작
./target/release/fluxmq &
FLUXMQ_PID=$!

# 벤치마크 실행
cd fluxmq-java-tests
mvn exec:java -Dexec.mainClass="com.fluxmq.tests.ComprehensiveBenchmark" \
  -Dexec.args="localhost:9092 FluxMQ $FLUXMQ_PID"

# FluxMQ 종료
kill $FLUXMQ_PID
```

### Kafka 벤치마크만 실행

```bash
# Kafka 시작
./benchmark-suite/setup/start_kafka.sh

# PID 확인
KAFKA_PID=$(pgrep -f "kafka.Kafka")

# 벤치마크 실행
cd fluxmq-java-tests
mvn exec:java -Dexec.mainClass="com.fluxmq.tests.ComprehensiveBenchmark" \
  -Dexec.args="localhost:9093 Kafka $KAFKA_PID"

# Kafka 종료
./benchmark-suite/setup/stop_kafka.sh
```

---

## 📊 결과 해석

### JSON 출력 형식

```json
{
  "testName": "단일스레드",
  "serverType": "FluxMQ",
  "numThreads": 1,
  "messageCount": 100000,
  "throughput": 265853,
  "dataThroughputMB": 190.10,
  "errorCount": 0,
  "latency": {
    "p50": 0.003,
    "p95": 0.005,
    "p99": 0.008
  },
  "serverResources": {
    "processName": "FluxMQ Server",
    "cpu": {
      "avg": 45.2,
      "max": 78.5,
      "min": 12.3
    },
    "memory": {
      "avg": 125.5,
      "max": 180.2,
      "min": 95.8
    }
  },
  "clientResources": {
    "processName": "Benchmark Client",
    "cpu": { ... },
    "memory": { ... }
  }
}
```

### 주요 메트릭 설명

- **throughput**: 초당 처리 메시지 수 (높을수록 좋음)
- **dataThroughputMB**: 초당 처리 데이터량 MB (높을수록 좋음)
- **latency.p99**: 99% 요청의 지연시간 (낮을수록 좋음)
- **serverResources.cpu.avg**: 서버 평균 CPU 사용률 (낮을수록 좋음)
- **serverResources.memory.avg**: 서버 평균 메모리 사용량 (낮을수록 좋음)

---

## 🎯 예상 결과

### FluxMQ의 강점
- ✅ **메모리 효율성**: Kafka 대비 30-50% 낮은 메모리 사용
- ✅ **빠른 시작**: JVM 부팅 없이 즉시 실행
- ✅ **낮은 CPU 사용**: Zero-cost abstractions

### Kafka의 강점
- ✅ **성숙한 생태계**: 검증된 안정성
- ✅ **풍부한 기능**: 트랜잭션, 스트림 처리
- ✅ **커뮤니티 지원**: 방대한 문서

---

## 🐛 문제 해결

### Kafka 시작 실패

```bash
# 로그 확인
tail -f kafka/logs/server.log

# 포트 충돌 확인
lsof -i :9093

# 강제 종료 후 재시작
./benchmark-suite/setup/stop_kafka.sh
sleep 3
./benchmark-suite/setup/start_kafka.sh
```

### FluxMQ PID 찾기 실패

```bash
# 수동으로 PID 확인
ps aux | grep fluxmq

# PID를 직접 지정하여 실행
mvn exec:java -Dexec.mainClass="com.fluxmq.tests.ComprehensiveBenchmark" \
  -Dexec.args="localhost:9092 FluxMQ <PID>"
```

### 메모리 부족

```bash
# JVM 힙 크기 증가
export MAVEN_OPTS="-Xmx4g"
mvn exec:java ...
```

---

## 📝 참고 자료

- [BENCHMARK_SUITE.md](../BENCHMARK_SUITE.md) - 상세 설계 문서
- [PERFORMANCE_COMPARISON.md](../PERFORMANCE_COMPARISON.md) - 기존 성능 비교
- [FluxMQ 문서](../README.md)
- [Apache Kafka 문서](https://kafka.apache.org/documentation/)

---

**작성일**: 2025-11-24
**버전**: 1.0
**라이선스**: MIT
