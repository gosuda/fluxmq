package com.fluxmq.tests;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 초고성능 FluxMQ 벤치마크 - acks=0 + 대용량 배치 크기
 * 목표: 100,000+ msg/sec 달성
 */
public class UltraPerformanceBenchmark {
    private static String BOOTSTRAP_SERVERS = "localhost:9092";  // 기본 포트 사용
    private static final String TOPIC = "ultra-performance-topic";
    private static final int MESSAGE_COUNT = 100000;  // 10만 메시지
    private static final int THREAD_COUNT = 8;  // 8개 스레드
    
    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            BOOTSTRAP_SERVERS = args[0];
        }
        
        System.out.println("🚀 FluxMQ 초고성능 벤치마크 시작");
        System.out.println("================================");
        System.out.println("서버: " + BOOTSTRAP_SERVERS);
        System.out.println("메시지 수: " + MESSAGE_COUNT);
        System.out.println("스레드 수: " + THREAD_COUNT);
        System.out.println("목표: 100,000+ msg/sec");
        System.out.println();
        
        // 웜업
        System.out.println("⏳ 웜업 중 (10,000 메시지)...");
        runBenchmark(10000);
        Thread.sleep(2000);
        
        // 메인 벤치마크
        System.out.println("🎯 초고성능 벤치마크 실행 중...");
        BenchmarkResult result = runBenchmark(MESSAGE_COUNT);
        
        // 결과 출력
        printResults(result);
    }
    
    static BenchmarkResult runBenchmark(int messageCount) throws Exception {
        Properties props = createUltraPerformanceProps();
        
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
                        String key = "thread-" + threadId + "-msg-" + j;
                        String value = "고성능_메시지_데이터_" + threadId + "_" + j + "_" + System.nanoTime();
                        
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
    
    static Properties createUltraPerformanceProps() {
        Properties props = new Properties();
        
        // 기본 설정
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 🚀 초고성능 설정
        props.put(ProducerConfig.ACKS_CONFIG, "0");  // 응답 대기 안함 (fire-and-forget)
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "524288");  // 512KB 배치 (대폭 증가!)
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");  // 10ms 대기로 배치 최적화
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");  // 고성능 압축
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "134217728");  // 128MB 버퍼
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "50");  // 동시 요청 증가
        props.put(ProducerConfig.SEND_BUFFER_CONFIG, "1048576");  // 1MB 송신 버퍼
        props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, "1048576");  // 1MB 수신 버퍼
        
        // 타임아웃 최적화
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        
        return props;
    }
    
    static void printResults(BenchmarkResult result) {
        double durationSeconds = result.durationNanos / 1_000_000_000.0;
        double messagesPerSecond = result.messageCount / durationSeconds;
        double bytesPerSecond = result.byteCount / durationSeconds;
        double mbPerSecond = bytesPerSecond / (1024 * 1024);
        
        System.out.println("\n🏆 초고성능 벤치마크 결과");
        System.out.println("============================");
        System.out.printf("📊 메시지 수: %,d\n", result.messageCount);
        System.out.printf("⏱️  소요 시간: %.2f 초\n", durationSeconds);
        System.out.printf("🚀 처리량: %,.0f msg/sec\n", messagesPerSecond);
        System.out.printf("💾 대역폭: %.2f MB/sec\n", mbPerSecond);
        System.out.printf("📈 평균 지연시간: %.3f ms/msg\n", 1000.0 / messagesPerSecond);
        
        // 성능 평가
        if (messagesPerSecond >= 100000) {
            System.out.println("✅ 목표 달성! 100k+ msg/sec 초고성능!");
        } else if (messagesPerSecond >= 50000) {
            System.out.println("🎯 우수한 성능! 50k+ msg/sec");
        } else {
            System.out.println("⚠️  성능 개선 필요");
        }
        
        System.out.println("\n🔧 최적화 설정:");
        System.out.println("- acks=0 (fire-and-forget)");
        System.out.println("- batch.size=512KB");
        System.out.println("- linger.ms=10");
        System.out.println("- compression=lz4");
        System.out.println("- buffer.memory=128MB");
        System.out.println("- max.in.flight=50");
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