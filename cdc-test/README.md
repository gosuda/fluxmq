# FluxMQ CDC Test Project

> PostgreSQL + Debezium + **FluxMQ** + Rust를 사용한 CDC(Change Data Capture) 파이프라인 테스트

## 아키텍처

```
PostgreSQL (WAL) → Debezium Connector → FluxMQ → Rust Consumer
```

## 빠른 시작

### 1. FluxMQ 브로커 시작

```bash
./target/release/fluxmq --port 9092 --enable-consumer-groups
```

### 2. Docker 서비스 시작

```bash
cd cdc-test
docker-compose up -d
```

### 3. 테스트 실행

```bash
# FluxMQ 직접 테스트 (Producer → Consumer)
cargo run --bin simple_producer -- 1000
cargo run --bin simple_consumer -- 1000

# CDC E2E 레이턴시 테스트 (PostgreSQL → Debezium → FluxMQ → Consumer)
cargo run --bin e2e_latency_test -- 100
```

## 바이너리 목록

| 바이너리 | 용도 |
|---------|------|
| `simple_producer` | FluxMQ 직접 성능 테스트 |
| `simple_consumer` | FluxMQ 소비 테스트 + E2E latency |
| `e2e_latency_test` | CDC 전체 경로 latency 측정 |

## 환경 변수

```bash
# FluxMQ
export KAFKA_BROKERS="localhost:9092"

# CDC 테스트
export PG_HOST="localhost"
export CDC_TOPIC="cdc.public.users"
```

## 프로젝트 구조

```
cdc-test/
├── docker-compose.yml        # PostgreSQL, Zookeeper, Kafka Connect
├── init-db.sql               # PostgreSQL 스키마
├── debezium-postgres-connector.json
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── config.rs
    ├── metrics.rs
    ├── models/
    │   └── debezium.rs       # Debezium 메시지 파싱
    └── bin/
        ├── simple_producer.rs
        ├── simple_consumer.rs
        └── e2e_latency_test.rs
```
