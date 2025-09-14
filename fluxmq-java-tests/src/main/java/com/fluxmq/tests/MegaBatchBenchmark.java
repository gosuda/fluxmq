package com.fluxmq.tests;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 메가 배치 FluxMQ 벤치마크 - 1MB 배치 크기 + acks=0
 * 목표: 200,000+ msg/sec 달성 (배치 크기 2배 증가로 성능 향상)
 */
public class MegaBatchBenchmark {
    private static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "mega-batch-topic";
    private static final int MESSAGE_COUNT = 5000000;  // 500만 메시지 - 대용량!
    private static final int THREAD_COUNT = 16;  // 16개 스레드 - 더 많은 병렬 처리!
    
    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            BOOTSTRAP_SERVERS = args[0];
        }
        
        System.out.println("🚀 FluxMQ 메가 배치 벤치마크 시작");
        System.out.println("=====================================");
        System.out.println("서버: " + BOOTSTRAP_SERVERS);
        System.out.println("배치 크기: 1MB (1048576 bytes)");
        System.out.println("메시지 수: " + MESSAGE_COUNT);
        System.out.println("스레드 수: " + THREAD_COUNT);
        System.out.println("목표: 200,000+ msg/sec 달성!");
        System.out.println();
        
        // 웜업
        System.out.println("⏳ 웜업 중 (5,000 메시지)...");
        runBenchmark(5000);
        Thread.sleep(3000);
        
        // 메인 벤치마크
        System.out.println("🎯 메가 배치 벤치마크 실행 중...");
        BenchmarkResult result = runBenchmark(MESSAGE_COUNT);
        
        // 결과 출력
        printResults(result);
    }
    
    static BenchmarkResult runBenchmark(int messageCount) throws Exception {
        Properties props = createMegaBatchProps();
        
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicLong totalMessages = new AtomicLong(0);
        AtomicLong totalBytes = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        
        long startTime = System.nanoTime();
        
        // 스레드별 메시지 수
        int messagesPerThread = messageCount / THREAD_COUNT;
        
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    KafkaProducer<String, String> producer = new KafkaProducer<>(props);
                    
                    for (int j = 0; j < messagesPerThread; j++) {
                        String key = "mega-thread-" + threadId + "-msg-" + j;
                        String value = "메가배치_초고성능_데이터_" + threadId + "_" + j + "_" 
                                     + System.nanoTime() + "_" + "x".repeat(200);  // 더 큰 메시지
                        
                        ProducerRecord<String, String> record = 
                            new ProducerRecord<>(TOPIC, key, value);
                        
                        producer.send(record, (metadata, exception) -> {
                            if (exception == null) {
                                totalMessages.incrementAndGet();
                                totalBytes.addAndGet(key.length() + value.length());
                            } else {
                                System.err.println("전송 실패: " + exception.getMessage());
                            }
                        });
                    }
                    
                    producer.flush();
                    producer.close();
                    
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await();
        long endTime = System.nanoTime();
        executor.shutdown();
        
        long duration = endTime - startTime;
        return new BenchmarkResult(totalMessages.get(), totalBytes.get(), duration);
    }
    
    static Properties createMegaBatchProps() {
        Properties props = new Properties();
        
        // 기본 설정
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 🚀 메가 배치 초고성능 설정
        props.put(ProducerConfig.ACKS_CONFIG, "0");  // Fire-and-forget
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "1048576");  // 1MB 배치!! (2x 증가)
        props.put(ProducerConfig.LINGER_MS_CONFIG, "15");  // 15ms 대기로 더 큰 배치 생성
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");  // LZ4 압축으로 네트워크 최적화
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "268435456");  // 256MB 버퍼 (2x 증가)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);  // 아이덤포턴스 비활성화 (성능 최우선)
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "100");  // 병렬 처리 대폭 증가
        props.put(ProducerConfig.SEND_BUFFER_CONFIG, "2097152");  // 2MB 송신 버퍼
        props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, "2097152");  // 2MB 수신 버퍼
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "2097152");  // 2MB 최대 요청
        
        // 타임아웃 최적화
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");  // 10초
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "20000");  // 20초
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");  // 10초
        
        return props;
    }
    
    static void printResults(BenchmarkResult result) {
        double durationSeconds = result.durationNanos / 1_000_000_000.0;
        double messagesPerSecond = result.messageCount / durationSeconds;
        double bytesPerSecond = result.byteCount / durationSeconds;
        double mbPerSecond = bytesPerSecond / (1024 * 1024);
        
        System.out.println("\n🏆 메가 배치 벤치마크 결과");
        System.out.println("==============================");
        System.out.printf("📊 메시지 수: %,d\n", result.messageCount);
        System.out.printf("⏱️  소요 시간: %.2f 초\n", durationSeconds);
        System.out.printf("🚀 처리량: %,.0f msg/sec\n", messagesPerSecond);
        System.out.printf("💾 대역폭: %.2f MB/sec\n", mbPerSecond);
        System.out.printf("📈 평균 지연시간: %.3f ms/msg\n", 1000.0 / messagesPerSecond);
        
        // 성능 평가
        if (messagesPerSecond >= 200000) {
            System.out.println("🎉 목표 달성! 200k+ msg/sec 메가 성능!");
        } else if (messagesPerSecond >= 150000) {
            System.out.println("🔥 탁월! 150k+ msg/sec 고성능!");
        } else if (messagesPerSecond >= 100000) {
            System.out.println("✅ 우수! 100k+ msg/sec 달성!");
        } else if (messagesPerSecond >= 50000) {
            System.out.println("👍 양호! 50k+ msg/sec");
        } else {
            System.out.println("📊 기본 성능 - 추가 최적화 고려");
        }
        
        System.out.println("\n🔧 메가 배치 최적화 설정:");
        System.out.println("- acks=0 (fire-and-forget)");
        System.out.println("- batch.size=1MB (메가 배치!)");
        System.out.println("- linger.ms=15 (배치 최적화)");
        System.out.println("- compression=lz4 (압축)");
        System.out.println("- buffer.memory=256MB");
        System.out.println("- max.in.flight=100 (초병렬)");
        System.out.println("- send/receive.buffer=2MB");
    }
    
    static class BenchmarkResult {
        final long messageCount;
        final long byteCount;
        final long durationNanos;
        
        BenchmarkResult(long messageCount, long byteCount, long durationNanos) {
            this.messageCount = messageCount;
            this.byteCount = byteCount;
            this.durationNanos = durationNanos;
        }
    }
}