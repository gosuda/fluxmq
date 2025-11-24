package com.fluxmq.tests;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;
import java.util.List;

/**
 * FluxMQ 멀티스레드 성능 벤치마크
 * 목표: 100,000+ msg/sec 달성
 *
 * 전략:
 * - 4-8개 프로듀서 스레드 병렬 실행
 * - 각 스레드가 독립적인 KafkaProducer 인스턴스 사용
 * - 파티션 분산으로 동시성 극대화
 */
public class MultiThreadBenchmark {
    private static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "benchmark-topic";
    private static final int WARMUP_MESSAGES = 10000;
    private static final int BENCHMARK_MESSAGES = 100000;

    // 멀티스레드 설정
    private static final int NUM_THREADS = 4; // CPU 코어 수에 맞춰 조정 가능

    public static void main(String[] args) throws Exception {
        // Allow server address as command line argument
        if (args.length > 0) {
            BOOTSTRAP_SERVERS = args[0];
        }

        System.out.println("🚀 FluxMQ 멀티스레드 성능 벤치마크 시작");
        System.out.println("==========================================");
        System.out.println("스레드 수: " + NUM_THREADS);
        System.out.println("목표: 100,000+ msg/sec 달성");
        System.out.println();

        // 웜업 실행
        System.out.println("⏳ 웜업 중...");
        runMultiThreadBenchmark("웜업", WARMUP_MESSAGES, false);

        Thread.sleep(2000);

        // 실제 벤치마크
        System.out.println("\n🎯 멀티스레드 벤치마크 실행 중...");
        BenchmarkResult result = runMultiThreadBenchmark("벤치마크", BENCHMARK_MESSAGES, true);

        // 결과 출력
        printResults(result);
    }

    static BenchmarkResult runMultiThreadBenchmark(String testName, int totalMessages, boolean detailed) throws Exception {
        AtomicLong messagesProcessed = new AtomicLong(0);
        AtomicLong bytesProcessed = new AtomicLong(0);
        AtomicLong errorCount = new AtomicLong(0);
        AtomicLong startTimeNanos = new AtomicLong(0);

        int messagesPerThread = totalMessages / NUM_THREADS;

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(NUM_THREADS);

        List<Future<?>> futures = new ArrayList<>();

        // 각 스레드 시작
        for (int threadId = 0; threadId < NUM_THREADS; threadId++) {
            final int finalThreadId = threadId;
            final int startMessageId = threadId * messagesPerThread;
            final int endMessageId = (threadId == NUM_THREADS - 1)
                ? totalMessages  // 마지막 스레드는 나머지 모두 처리
                : startMessageId + messagesPerThread;

            Future<?> future = executor.submit(() -> {
                try {
                    // 모든 스레드가 동시에 시작하도록 대기
                    startLatch.await();

                    Properties props = createProducerProps();

                    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                        int threadMessages = endMessageId - startMessageId;
                        CountDownLatch threadLatch = new CountDownLatch(threadMessages);

                        for (int i = startMessageId; i < endMessageId; i++) {
                            String key = "key-" + i;
                            String payload = generatePayload(i);
                            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, payload);

                            producer.send(record, (metadata, exception) -> {
                                if (exception == null) {
                                    long processed = messagesProcessed.incrementAndGet();
                                    bytesProcessed.addAndGet((key != null ? key.length() : 0) + payload.length());

                                    if (detailed && processed % 20000 == 0) {
                                        long startNanos = startTimeNanos.get();
                                        if (startNanos > 0) {
                                            long elapsed = System.nanoTime() - startNanos;
                                            double rate = processed * 1_000_000_000.0 / elapsed;
                                            System.out.printf("  진행: %,d/%,d (%.0f msg/sec)%n",
                                                processed, totalMessages, rate);
                                        }
                                    }
                                } else {
                                    errorCount.incrementAndGet();
                                    if (detailed && errorCount.get() <= 10) {
                                        System.err.println("전송 실패: " + exception.getMessage());
                                    }
                                }
                                threadLatch.countDown();
                            });
                        }

                        producer.flush();
                        if (!threadLatch.await(60, TimeUnit.SECONDS)) {
                            System.err.println("⚠️ 스레드 " + finalThreadId + " 타임아웃!");
                        }
                    }
                } catch (Exception e) {
                    System.err.println("스레드 " + finalThreadId + " 오류: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    finishLatch.countDown();
                }
            });

            futures.add(future);
        }

        // 모든 스레드 동시 시작
        Thread.sleep(100); // 모든 스레드가 준비될 시간
        startTimeNanos.set(System.nanoTime()); // 실제 시작 시간 기록
        startLatch.countDown();

        // 모든 스레드 완료 대기
        if (!finishLatch.await(120, TimeUnit.SECONDS)) {
            System.err.println("⚠️ 일부 스레드가 완료되지 않았습니다!");
        }

        long endTime = System.nanoTime();
        executor.shutdown();

        double durationSeconds = (endTime - startTimeNanos.get()) / 1_000_000_000.0;

        if (errorCount.get() > 0) {
            System.err.println("⚠️ 총 에러 수: " + errorCount.get());
        }

        return new BenchmarkResult(
            testName,
            messagesProcessed.get(),
            bytesProcessed.get(),
            durationSeconds,
            NUM_THREADS
        );
    }

    private static Properties createProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 고성능 멀티스레드 설정
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072);         // 128KB 배치
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);               // 1ms 대기
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);    // 64MB 버퍼
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1048576);  // 1MB 최대 요청
        props.put(ProducerConfig.ACKS_CONFIG, "1");                  // Leader ack
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");

        // 타임아웃 설정
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 300000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 30000);

        return props;
    }

    private static String generatePayload(int messageId) {
        return String.format(
            "{\"id\":%d,\"timestamp\":%d,\"data\":\"multithread-benchmark-%08d\",\"payload\":\"%s\"}",
            messageId,
            System.currentTimeMillis(),
            messageId,
            "x".repeat(650) // ~800 bytes per message
        );
    }

    private static void printResults(BenchmarkResult result) {
        System.out.println();
        System.out.println("📊 멀티스레드 벤치마크 결과");
        System.out.println("============================");
        System.out.printf("스레드 수: %d%n", result.numThreads);
        System.out.printf("메시지 수: %,d개%n", result.messageCount);
        System.out.printf("처리 시간: %.3f초%n", result.duration);
        System.out.printf("처리량: %,.0f msg/sec%n", result.getThroughput());
        System.out.printf("스레드당: %,.0f msg/sec%n", result.getThroughput() / result.numThreads);
        System.out.printf("데이터량: %.2f MB/sec%n", result.getDataThroughputMB());
        System.out.printf("평균 지연시간: %.3f ms%n", result.getAverageLatency());
        System.out.println();

        // 성능 평가
        double throughput = result.getThroughput();
        if (throughput >= 100000) {
            System.out.println("🚀🚀🚀 탁월! 100,000+ msg/sec 달성! 목표 달성!");
            System.out.printf("   → Java 단일 스레드(20k) 대비 %.1fx 성능 향상!%n", throughput / 20115.0);
        } else if (throughput >= 80000) {
            System.out.println("🔥 우수! 80,000+ msg/sec - 목표에 매우 근접!");
        } else if (throughput >= 60000) {
            System.out.println("✅ 양호! 60,000+ msg/sec - 단일 스레드 대비 3배 향상!");
        } else if (throughput >= 40000) {
            System.out.println("📈 개선됨! 40,000+ msg/sec - 단일 스레드 대비 2배 향상!");
        } else {
            System.out.println("⚠️ 추가 최적화 필요 - 스레드 수 조정 권장");
        }
    }

    static class BenchmarkResult {
        final String testName;
        final long messageCount;
        final long totalBytes;
        final double duration;
        final int numThreads;

        BenchmarkResult(String testName, long messageCount, long totalBytes, double duration, int numThreads) {
            this.testName = testName;
            this.messageCount = messageCount;
            this.totalBytes = totalBytes;
            this.duration = duration;
            this.numThreads = numThreads;
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
