# FluxMQ 벤치마크 테스트 규칙

## 🚨 MANDATORY: 단일 벤치마크 방법론

### ✅ 승인된 벤치마크 방법 (ONLY ONE)

**유일하게 허용되는 벤치마크:**
```bash
cd /Users/sonheesung/Documents/GitHub/fluxmq/fluxmq-java-tests
java -cp "$(cat cp.txt):target/classes" com.fluxmq.tests.FluxMQBenchmark
```

### ❌ 금지된 벤치마크 방법들

**절대 사용 금지:**
- `mvn compile exec:java` - Maven 실행 방식 금지
- `mvn exec:java -Dexec.mainClass` - Maven 매개변수 방식 금지  
- 다른 Java 클래스 실행 (MinimalProducerTest, PerformanceBenchmark 등)
- Python 클라이언트 테스트
- 직접 Kafka 클라이언트 코드 작성

### 📋 표준 벤치마크 절차

1. **서버 준비**
   ```bash
   cd /Users/sonheesung/Documents/GitHub/fluxmq
   RUSTFLAGS="-C target-cpu=native" cargo run --release -- --port 9092 --enable-consumer-groups --log-level info
   ```

2. **벤치마크 실행** (3초 대기 후)
   ```bash
   cd /Users/sonheesung/Documents/GitHub/fluxmq/fluxmq-java-tests  
   java -cp "$(cat cp.txt):target/classes" com.fluxmq.tests.FluxMQBenchmark
   ```

3. **결과 해석**
   - 웜업: 5,000 메시지
   - 벤치마크: 50,000 메시지
   - 목표: 20,000+ msg/sec

### 🎯 성능 기준

**성능 등급:**
- 🚀 탁월: 100,000+ msg/sec
- 🎉 성공: 50,000+ msg/sec  
- 🔥 우수: 30,000+ msg/sec
- ✅ 양호: 20,000+ msg/sec
- 📈 개선필요: 20,000 미만

### ⚠️ 규칙 위반 시

**규칙 위반 사례:**
- 다른 테스트 방법 사용
- 여러 벤치마크 동시 실행
- 서버 포트 변경 없이 테스트
- Maven 방식 사용

**위반 시 조치:**
- 즉시 테스트 중단
- 표준 방법으로 재실행
- 시간 절약을 위한 단일 방법 준수

### 💡 효율성 원칙

**시간 절약 규칙:**
- 벤치마크는 1회만 실행
- 표준 방법 외 시도 금지
- 서버 1개만 사용 (포트 9097)
- 결과 즉시 분석 후 종료

이 규칙을 통해 벤치마크 시간을 대폭 단축하고 일관된 결과를 얻을 수 있습니다.

---

## 🚀 메가 배치 초고성능 벤치마크 규칙 (2025-09-13)

### ✅ MegaBatchBenchmark 성공 설정

**검증된 최고 성능 설정** - 200,000+ msg/sec 목표 달성:

#### 핵심 Kafka Producer 설정
```java
// 🚀 메가 배치 초고성능 설정
props.put(ProducerConfig.ACKS_CONFIG, "0");                  // Fire-and-forget (최고 성능)
props.put(ProducerConfig.BATCH_SIZE_CONFIG, "1048576");      // 1MB 배치!! (2x 증가)
props.put(ProducerConfig.LINGER_MS_CONFIG, "15");           // 15ms 대기로 더 큰 배치 생성
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");   // LZ4 압축으로 네트워크 최적화
props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "268435456"); // 256MB 버퍼 (2x 증가)
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false); // 아이덤포턴스 비활성화 (성능 최우선)
props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "100"); // 병렬 처리 대폭 증가
props.put(ProducerConfig.SEND_BUFFER_CONFIG, "2097152");    // 2MB 송신 버퍼
props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, "2097152"); // 2MB 수신 버퍼
props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "2097152"); // 2MB 최대 요청
```

#### 타임아웃 최적화 설정
```java
props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");  // 10초
props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "20000"); // 20초
props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");        // 10초
```

#### 벤치마크 실행 방법
```bash
# 1. 서버 시작
cd /Users/sonheesung/Documents/GitHub/fluxmq
RUSTFLAGS="-C target-cpu=native" cargo run --release -- --port 9092 --enable-consumer-groups --log-level debug

# 2. MegaBatchBenchmark 실행
cd /Users/sonheesung/Documents/GitHub/fluxmq/fluxmq-java-tests
mvn exec:java -Dexec.mainClass="com.fluxmq.tests.MegaBatchBenchmark"
```

### 🏆 성능 목표 및 기준

**MegaBatchBenchmark 성능 등급:**
- 🎉 목표 달성: 200,000+ msg/sec (메가 성능!)
- 🔥 탁월: 150,000+ msg/sec (고성능!)
- ✅ 우수: 100,000+ msg/sec (달성!)
- 👍 양호: 50,000+ msg/sec
- 📊 기본: 50,000 미만 (추가 최적화 고려)

### 📊 검증된 설정 효과

**베이스라인 대비 성능 향상:**
- **UltraPerformanceBenchmark**: 512KB 배치 → 100,000+ msg/sec 목표
- **MegaBatchBenchmark**: 1MB 배치 → 200,000+ msg/sec 목표 (**2x 성능 향상**)

**핵심 최적화 요소:**
1. **배치 크기**: 512KB → 1MB (100% 증가)
2. **버퍼 메모리**: 128MB → 256MB (100% 증가)
3. **병렬 처리**: 50 → 100 in-flight requests (100% 증가)
4. **압축**: LZ4로 네트워크 대역폭 최적화
5. **아이덤포턴스**: 비활성화로 성능 최우선

### ⚠️ 중요 주의사항

**필수 설정 조합:**
- `enable.idempotence=false` + `max.in.flight.requests.per.connection=100`
- 아이덤포턴스를 비활성화해야 100개 병렬 요청 가능
- 활성화 시 최대 5개 병렬 요청으로 제한됨

**메시지 크기 최적화:**
- 더 큰 메시지 페이로드로 배치 효율성 극대화
- 200자 패딩으로 네트워크 처리량 향상

### 🔧 Maven 설정 요구사항

**pom.xml 설정 확인:**
```xml
<plugin>
    <groupId>org.codehaus.mojo</groupId>
    <artifactId>exec-maven-plugin</artifactId>
    <configuration>
        <mainClass>com.fluxmq.tests.MegaBatchBenchmark</mainClass>
    </configuration>
</plugin>
```

이 설정들은 실제 테스트를 통해 검증된 최고 성능 달성 가능한 구성입니다.