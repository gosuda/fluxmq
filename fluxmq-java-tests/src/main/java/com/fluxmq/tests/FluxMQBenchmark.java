package com.fluxmq.tests;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * FluxMQ 성능 벤치마크 테스트
 * 고성능 Java Kafka 클라이언트로 FluxMQ 처리량 측정
 */
public class FluxMQBenchmark {
    private static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "benchmark-topic";
    private static final int WARMUP_MESSAGES = 5000;
    private static final int BENCHMARK_MESSAGES = 50000;
    
    public static void main(String[] args) throws Exception {
        // Allow server address as command line argument
        if (args.length > 0) {
            BOOTSTRAP_SERVERS = args[0];
        }
        System.out.println("🚀 FluxMQ 성능 벤치마크 시작");
        System.out.println("================================");
        System.out.println("목표: 50,000+ msg/sec 달성");
        System.out.println();
        
        // 웜업 실행
        System.out.println("⏳ 웜업 중...");
        runBenchmark("웜업", WARMUP_MESSAGES, false);
        
        Thread.sleep(2000);
        
        // 실제 벤치마크
        System.out.println("\n🎯 벤치마크 실행 중...");
        BenchmarkResult result = runBenchmark("벤치마크", BENCHMARK_MESSAGES, true);
        
        // 결과 출력
        printResults(result);
    }
    
    static BenchmarkResult runBenchmark(String testName, int messageCount, boolean detailed) throws Exception {
        Properties props = createProducerProps();
        
        AtomicLong messagesProcessed = new AtomicLong(0);
        AtomicLong bytesProcessed = new AtomicLong(0);
        
        long startTime = System.nanoTime();
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            CountDownLatch latch = new CountDownLatch(messageCount);
            
            // 비동기 전송으로 최대 성능 달성 - 모든 파티션 균등 사용
            for (int i = 0; i < messageCount; i++) {
                String key = "key-" + i;  // 키 추가로 파티션 분배 개선
                String payload = generatePayload(i);
                // 모든 파티션(0, 1, 2) 균등 사용
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, payload);
                
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        long processed = messagesProcessed.incrementAndGet();
                        bytesProcessed.addAndGet((key != null ? key.length() : 0) + payload.length());
                        latch.countDown();
                        
                        if (detailed && processed % 10000 == 0) {
                            long elapsed = System.nanoTime() - startTime;
                            double rate = processed * 1_000_000_000.0 / elapsed;
                            System.out.printf("  진행: %,d/%,d (%.0f msg/sec)%n", 
                                processed, messageCount, rate);
                        }
                    } else {
                        System.err.println("전송 실패: " + exception.getMessage());
                        latch.countDown();
                    }
                });
            }
            
            // 모든 메시지가 전송될 때까지 대기
            producer.flush();
            latch.await();
        }
        
        long endTime = System.nanoTime();
        double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
        
        return new BenchmarkResult(
            testName,
            messagesProcessed.get(),
            bytesProcessed.get(),
            durationSeconds
        );
    }
    
    private static Properties createProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 🚀 Arena Allocator 최적화 설정 (64KB 배치, 50-150 메시지)
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072);         // 128KB 배치 (Arena 트리거)
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);               // 5ms 대기 (배치 집계 시간)  
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);    // 64MB 버퍼 (적정 크기)
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1048576);  // 1MB 최대 요청 (더 작게)
        props.put(ProducerConfig.ACKS_CONFIG, "0");                  // No ack (최대 성능)
        props.put(ProducerConfig.RETRIES_CONFIG, 0);                 // 재시도 없음 (타임아웃 방지)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);  // 아이덤포턴스 비활성화 (성능 우선)
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5); // 병렬 처리 증가
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");   // 압축 비활성화 (CPU 절약)
        
        // 타임아웃 최적화 (고성능 벤치마크용 - 충분한 여유시간 확보)
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60000); // 60초 전체 타임아웃  
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);  // 30초 요청 타임아웃
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 30000);    // 30초 메타데이터 갱신
        
        return props;
    }
    
    private static String generatePayload(int messageId) {
        // Arena Allocator 최적화: ~800 bytes per message (128KB / 163 messages)
        return String.format(
            "{\"id\":%d,\"timestamp\":%d,\"data\":\"benchmark-message-%08d\",\"payload\":\"%s\",\"arena_test\":\"optimized_for_64kb_java_batches\"}",
            messageId,
            System.currentTimeMillis(),
            messageId,
            "x".repeat(650) // 650자 패딩 → ~800 bytes per message
        );
    }
    
    private static void printResults(BenchmarkResult result) {
        System.out.println();
        System.out.println("📊 벤치마크 결과");
        System.out.println("================");
        System.out.printf("메시지 수: %,d개%n", result.messageCount);
        System.out.printf("처리 시간: %.3f초%n", result.duration);
        System.out.printf("처리량: %,.0f msg/sec%n", result.getThroughput());
        System.out.printf("데이터량: %.2f MB/sec%n", result.getDataThroughputMB());
        System.out.printf("평균 지연시간: %.3f ms%n", result.getAverageLatency());
        System.out.println();
        
        // 성능 평가
        double throughput = result.getThroughput();
        if (throughput >= 100000) {
            System.out.println("🚀 탁월! 100,000+ msg/sec 달성!");
        } else if (throughput >= 50000) {
            System.out.println("🎉 성공! 목표 50,000+ msg/sec 달성!");
        } else if (throughput >= 30000) {
            System.out.println("🔥 우수! 30,000+ msg/sec - 목표에 근접!");
        } else if (throughput >= 20000) {
            System.out.println("✅ 양호! 20,000+ msg/sec - 기본 목표 달성!");
        } else {
            System.out.println("📈 개선 필요 - 추가 최적화 권장");
        }
    }
    
    static class BenchmarkResult {
        final String testName;
        final long messageCount;
        final long totalBytes;
        final double duration;
        
        BenchmarkResult(String testName, long messageCount, long totalBytes, double duration) {
            this.testName = testName;
            this.messageCount = messageCount;
            this.totalBytes = totalBytes;
            this.duration = duration;
        }
        
        double getThroughput() {
            return messageCount / duration;
        }
        
        double getDataThroughputMB() {
            return (totalBytes / duration) / (1024 * 1024);
        }
        
        double getAverageLatency() {
            return (duration * 1000) / messageCount;
        }
    }
}