package com.fluxmq.tests;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * FluxMQ vs Kafka 종합 벤치마크
 * 성능 + 리소스 사용량을 동시에 측정
 */
public class ComprehensiveBenchmark {
    private static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "benchmark-topic";

    // 벤치마크 설정
    private static final int WARMUP_MESSAGES = 10000;
    private static final int BENCHMARK_MESSAGES = 100000;
    private static final int NUM_THREADS = 4;

    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            BOOTSTRAP_SERVERS = args[0];
        }

        String serverType = args.length > 1 ? args[1] : "FluxMQ";
        long serverPid = args.length > 2 ? Long.parseLong(args[2]) : -1;

        System.out.println("🚀 " + serverType + " 종합 벤치마크 시작");
        System.out.println("==========================================");
        System.out.println("서버: " + BOOTSTRAP_SERVERS);
        System.out.println("서버 PID: " + (serverPid > 0 ? serverPid : "자동 감지"));
        System.out.println("스레드 수: " + NUM_THREADS);
        System.out.println();

        // 웜업
        System.out.println("⏳ 웜업 중...");
        runBenchmark("웜업", WARMUP_MESSAGES, 1, serverPid, serverType, false);
        Thread.sleep(2000);

        // 시나리오 1: 단일 스레드 처리량 + 리소스 측정
        System.out.println("\n🎯 시나리오 1: 단일 스레드 처리량 테스트");
        BenchmarkResult result1 = runBenchmark("단일스레드", BENCHMARK_MESSAGES, 1, serverPid, serverType, true);
        result1.print();
        result1.saveToFile("results/" + serverType.toLowerCase() + "/single_thread.json");

        Thread.sleep(3000);

        // 시나리오 2: 멀티 스레드 확장성 + 리소스 측정
        System.out.println("\n🎯 시나리오 2: 멀티스레드 확장성 테스트");
        BenchmarkResult result2 = runBenchmark("멀티스레드", BENCHMARK_MESSAGES, NUM_THREADS, serverPid, serverType, true);
        result2.print();
        result2.saveToFile("results/" + serverType.toLowerCase() + "/multi_thread.json");

        Thread.sleep(3000);

        // 시나리오 3: 대용량 데이터 (장시간 안정성)
        System.out.println("\n🎯 시나리오 3: 대용량 데이터 처리 테스트");
        BenchmarkResult result3 = runBenchmark("대용량", BENCHMARK_MESSAGES * 5, NUM_THREADS, serverPid, serverType, true);
        result3.print();
        result3.saveToFile("results/" + serverType.toLowerCase() + "/high_volume.json");

        // 최종 요약
        System.out.println("\n📊 종합 벤치마크 완료");
        System.out.println("결과 저장: results/" + serverType.toLowerCase() + "/");
    }

    /**
     * 벤치마크 실행 + 리소스 모니터링
     */
    static BenchmarkResult runBenchmark(
        String testName,
        int totalMessages,
        int numThreads,
        long serverPid,
        String serverType,
        boolean detailed
    ) throws Exception {
        BenchmarkResult result = new BenchmarkResult(testName, serverType, numThreads);

        // 리소스 모니터링 시작
        ResourceMonitor serverMonitor = null;
        ResourceMonitor clientMonitor = null;

        if (serverPid > 0) {
            serverMonitor = new ResourceMonitor(serverPid, serverType + " Server");
            serverMonitor.start();
        }

        // 자체 클라이언트 프로세스 모니터링
        clientMonitor = new ResourceMonitor(-1, "Benchmark Client");
        clientMonitor.start();

        // 성능 벤치마크 실행
        long startTime = System.nanoTime();
        PerformanceMetrics perfMetrics = runProducerTest(totalMessages, numThreads, detailed);
        long endTime = System.nanoTime();

        // 모니터링 중지
        if (serverMonitor != null) {
            serverMonitor.stop();
            result.serverResourceStats = serverMonitor.getStats();
        }

        clientMonitor.stop();
        result.clientResourceStats = clientMonitor.getStats();

        // 결과 집계
        result.messageCount = perfMetrics.messagesSent;
        result.totalBytes = perfMetrics.bytesSent;
        result.duration = (endTime - startTime) / 1_000_000_000.0;
        result.errorCount = perfMetrics.errorCount;
        result.latencies = perfMetrics.latencies;

        return result;
    }

    /**
     * 프로듀서 테스트 실행
     */
    static PerformanceMetrics runProducerTest(int totalMessages, int numThreads, boolean detailed) throws Exception {
        AtomicLong messagesSent = new AtomicLong(0);
        AtomicLong bytesSent = new AtomicLong(0);
        AtomicLong errorCount = new AtomicLong(0);
        List<Long> latencies = new CopyOnWriteArrayList<>();

        int messagesPerThread = totalMessages / numThreads;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(numThreads);

        for (int threadId = 0; threadId < numThreads; threadId++) {
            final int finalThreadId = threadId;
            final int startMessageId = threadId * messagesPerThread;
            final int endMessageId = (threadId == numThreads - 1)
                ? totalMessages
                : startMessageId + messagesPerThread;

            executor.submit(() -> {
                try {
                    startLatch.await();

                    Properties props = createProducerProps();

                    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                        int threadMessages = endMessageId - startMessageId;
                        CountDownLatch threadLatch = new CountDownLatch(threadMessages);

                        for (int i = startMessageId; i < endMessageId; i++) {
                            String key = "key-" + i;
                            String payload = generatePayload(i);
                            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, payload);

                            long sendStart = System.nanoTime();

                            producer.send(record, (metadata, exception) -> {
                                if (exception == null) {
                                    long sendEnd = System.nanoTime();
                                    long latency = (sendEnd - sendStart) / 1_000_000; // ms

                                    messagesSent.incrementAndGet();
                                    bytesSent.addAndGet((key != null ? key.length() : 0) + payload.length());

                                    if (detailed && latencies.size() < 10000) {
                                        latencies.add(latency);
                                    }
                                } else {
                                    errorCount.incrementAndGet();
                                }
                                threadLatch.countDown();
                            });
                        }

                        producer.flush();
                        threadLatch.await(60, TimeUnit.SECONDS);
                    }
                } catch (Exception e) {
                    System.err.println("스레드 " + finalThreadId + " 오류: " + e.getMessage());
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        // 모든 스레드 동시 시작
        Thread.sleep(100);
        startLatch.countDown();

        // 완료 대기
        finishLatch.await(120, TimeUnit.SECONDS);
        executor.shutdown();

        PerformanceMetrics metrics = new PerformanceMetrics();
        metrics.messagesSent = messagesSent.get();
        metrics.bytesSent = bytesSent.get();
        metrics.errorCount = errorCount.get();
        metrics.latencies = latencies;

        return metrics;
    }

    private static Properties createProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1048576);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");

        return props;
    }

    private static String generatePayload(int messageId) {
        return String.format(
            "{\"id\":%d,\"timestamp\":%d,\"data\":\"benchmark-%08d\",\"payload\":\"%s\"}",
            messageId,
            System.currentTimeMillis(),
            messageId,
            "x".repeat(650)
        );
    }

    /**
     * 성능 메트릭
     */
    static class PerformanceMetrics {
        long messagesSent;
        long bytesSent;
        long errorCount;
        List<Long> latencies;
    }

    /**
     * 벤치마크 결과
     */
    static class BenchmarkResult {
        String testName;
        String serverType;
        int numThreads;
        long messageCount;
        long totalBytes;
        double duration;
        long errorCount;
        List<Long> latencies;

        ResourceMonitor.ResourceStats serverResourceStats;
        ResourceMonitor.ResourceStats clientResourceStats;

        BenchmarkResult(String testName, String serverType, int numThreads) {
            this.testName = testName;
            this.serverType = serverType;
            this.numThreads = numThreads;
        }

        double getThroughput() {
            return messageCount / duration;
        }

        double getDataThroughputMB() {
            return (totalBytes / duration) / (1024 * 1024);
        }

        void print() {
            System.out.println();
            System.out.println("📊 벤치마크 결과: " + testName);
            System.out.println("============================");
            System.out.println("서버: " + serverType);
            System.out.println("스레드 수: " + numThreads);
            System.out.printf("메시지 수: %,d개%n", messageCount);
            System.out.printf("처리 시간: %.3f초%n", duration);
            System.out.printf("처리량: %,.0f msg/sec%n", getThroughput());
            System.out.printf("데이터량: %.2f MB/sec%n", getDataThroughputMB());
            System.out.printf("에러: %,d건%n", errorCount);

            if (latencies != null && !latencies.isEmpty()) {
                Collections.sort(latencies);
                System.out.println();
                System.out.println("지연시간 통계 (ms):");
                System.out.printf("  p50: %.3f ms%n", getPercentile(50));
                System.out.printf("  p95: %.3f ms%n", getPercentile(95));
                System.out.printf("  p99: %.3f ms%n", getPercentile(99));
            }

            if (serverResourceStats != null) {
                System.out.println();
                System.out.println(serverResourceStats);
            }

            if (clientResourceStats != null) {
                System.out.println();
                System.out.println(clientResourceStats);
            }
        }

        double getPercentile(int percentile) {
            if (latencies == null || latencies.isEmpty()) {
                return 0.0;
            }
            int index = (int) Math.ceil(percentile / 100.0 * latencies.size()) - 1;
            return latencies.get(Math.max(0, Math.min(index, latencies.size() - 1)));
        }

        void saveToFile(String filename) {
            try {
                File file = new File(filename);
                file.getParentFile().mkdirs();

                try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
                    writer.println("{");
                    writer.println("  \"testName\": \"" + testName + "\",");
                    writer.println("  \"serverType\": \"" + serverType + "\",");
                    writer.println("  \"timestamp\": \"" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\",");
                    writer.println("  \"numThreads\": " + numThreads + ",");
                    writer.println("  \"messageCount\": " + messageCount + ",");
                    writer.println("  \"totalBytes\": " + totalBytes + ",");
                    writer.println("  \"duration\": " + String.format("%.3f", duration) + ",");
                    writer.println("  \"throughput\": " + String.format("%.0f", getThroughput()) + ",");
                    writer.println("  \"dataThroughputMB\": " + String.format("%.2f", getDataThroughputMB()) + ",");
                    writer.println("  \"errorCount\": " + errorCount + ",");

                    if (latencies != null && !latencies.isEmpty()) {
                        writer.println("  \"latency\": {");
                        writer.println("    \"p50\": " + String.format("%.3f", getPercentile(50)) + ",");
                        writer.println("    \"p95\": " + String.format("%.3f", getPercentile(95)) + ",");
                        writer.println("    \"p99\": " + String.format("%.3f", getPercentile(99)));
                        writer.println("  },");
                    }

                    if (serverResourceStats != null) {
                        writer.println("  \"serverResources\": " + serverResourceStats.toJson() + ",");
                    }

                    if (clientResourceStats != null) {
                        writer.println("  \"clientResources\": " + clientResourceStats.toJson());
                    }

                    writer.println("}");
                }

                System.out.println("\n✅ 결과 저장: " + filename);

            } catch (Exception e) {
                System.err.println("파일 저장 오류: " + e.getMessage());
            }
        }
    }
}
