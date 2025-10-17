# FluxMQ 벤치마크 실행 가이드

## 📋 개요
이 가이드는 FluxMQ Java 벤치마크를 안정적으로 실행하고 모니터링하는 방법을 설명합니다.
프로세스가 중간에 종료되는 문제를 해결하기 위해 파일 로깅 방식을 사용합니다.

## 🔧 준비사항

### 1. FluxMQ 서버 빌드
```bash
cd /Users/sonheesung/Documents/GitHub/fluxmq
env RUSTFLAGS="-C target-cpu=native" cargo build --release
```

### 2. Java 벤치마크 의존성 설치
```bash
cd fluxmq-java-tests
mvn dependency:copy-dependencies
mvn compile
```

## 🚀 벤치마크 실행 방법

### 단계 1: FluxMQ 서버 시작 (파일 로깅)
```bash
# 메인 프로젝트 디렉토리에서 실행
cd /Users/sonheesung/Documents/GitHub/fluxmq
env RUSTFLAGS="-C target-cpu=native" ./target/release/fluxmq --port 9092 --enable-consumer-groups --log-level info > fluxmq_server.log 2>&1 &
```

**서버 상태 확인:**
```bash
# 서버 로그 실시간 모니터링
tail -f fluxmq_server.log

# 프로세스 확인
ps aux | grep fluxmq
```

### 단계 2: Java 벤치마크 실행 (파일 로깅)
```bash
cd fluxmq-java-tests
java -cp "target/classes:target/dependency/*" com.fluxmq.tests.MegaBatchBenchmark localhost:9092 > java_benchmark_$(date +%Y%m%d_%H%M%S).log 2>&1 &
```

**또는 백그라운드 실행:**
```bash
nohup java -cp "target/classes:target/dependency/*" com.fluxmq.tests.MegaBatchBenchmark localhost:9092 > java_benchmark_$(date +%Y%m%d_%H%M%S).log 2>&1 &
```

## 📊 모니터링 방법

### 실시간 로그 모니터링
```bash
# Java 벤치마크 로그 확인
tail -f java_benchmark_*.log

# FluxMQ 서버 로그 확인
tail -f ../fluxmq_server.log

# 두 로그를 동시에 모니터링
tail -f java_benchmark_*.log ../fluxmq_server.log
```

### 진행 상황 확인
```bash
# 로그 파일 크기 확인
ls -lah *.log

# 벤치마크 완료 여부 확인
grep "벤치마크 완료" java_benchmark_*.log
grep "🏆 메가 배치 벤치마크 결과" java_benchmark_*.log
```

### 프로세스 상태 확인
```bash
# Java 프로세스 확인
ps aux | grep MegaBatchBenchmark

# 포트 사용 확인
lsof -i :9092
```

## 📁 생성되는 로그 파일

### Java 벤치마크 로그
- **파일명**: `java_benchmark_YYYYMMDD_HHMMSS.log`
- **위치**: `fluxmq-java-tests/`
- **내용**: 벤치마크 진행 상황, 성능 결과, 에러 메시지

### FluxMQ 서버 로그
- **파일명**: `fluxmq_server.log`
- **위치**: 메인 프로젝트 디렉토리
- **내용**: 서버 상태, 메트릭, 클라이언트 연결 정보

### 벤치마크 내부 로그
- **파일명**: `fluxmq_benchmark_YYYYMMDD_HHMMSS.log`
- **위치**: `fluxmq-java-tests/`
- **내용**: 벤치마크 애플리케이션의 상세 로그 (타임스탬프 포함)

## 🔧 트러블슈팅

### 서버가 시작되지 않는 경우
```bash
# 포트 충돌 확인
lsof -i :9092
pkill -f fluxmq

# 다시 시작
env RUSTFLAGS="-C target-cpu=native" ./target/release/fluxmq --port 9092 --enable-consumer-groups --log-level info > fluxmq_server.log 2>&1 &
```

### Java 클래스패스 오류
```bash
# 의존성 재다운로드
mvn clean dependency:copy-dependencies compile

# 클래스패스 확인
ls -la target/dependency/kafka-clients-*.jar
```

### 프로세스 정리
```bash
# 모든 관련 프로세스 종료
pkill -f fluxmq
pkill -f MegaBatchBenchmark

# 로그 파일 정리 (선택사항)
rm -f *.log
```

## 📈 성능 결과 확인

벤치마크 완료 후 다음 명령으로 결과 확인:

```bash
# 최종 성능 결과
grep -A 10 "🏆 메가 배치 벤치마크 결과" java_benchmark_*.log

# 처리량 확인
grep "처리량" java_benchmark_*.log

# 성능 평가 확인
grep -E "목표 달성|탁월|우수|양호" java_benchmark_*.log
```

## 🔄 반복 실행 스크립트

다음에 제공될 자동화 스크립트를 사용하여 더 편리하게 실행할 수 있습니다:
- `run_benchmark.sh`: 전체 벤치마크 자동 실행
- `monitor_logs.sh`: 로그 실시간 모니터링
- `cleanup.sh`: 프로세스 및 로그 정리

## 📝 주요 장점

1. **안정성**: 프로세스가 터미널과 독립적으로 실행
2. **모니터링**: 파일을 통한 안전한 진행 상황 추적
3. **디버깅**: 모든 로그가 파일에 저장되어 문제 분석 용이
4. **재현성**: 동일한 방식으로 반복 실행 가능
5. **백업**: 로그 파일을 통한 결과 보관

---

**최종 업데이트**: 2025-09-29
**테스트 환경**: macOS, Java 21, FluxMQ v1.0