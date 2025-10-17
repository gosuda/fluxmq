package com.fluxmq.tests;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 연속적인 처리량 측정 벤치마크
 * - flush() 없이 연속적으로 메시지 전송
 * - 실시간 처리량 모니터링
 * - 10초 동안 최대한 많은 메시지 전송
 */
public class ContinuousThroughputBenchmark {
    private static final String TOPIC = "benchmark-topic";
    private static final int DURATION_SECONDS = 10;
    private static final int PAYLOAD_SIZE = 1024;  // 1KB 메시지

    public static void main(String[] args) {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        System.out.println("🚀 FluxMQ 연속 처리량 벤치마크");
        System.out.println("================================");
        System.out.println("목표: 연속적인 최대 처리량 측정");
        System.out.println("Duration: " + DURATION_SECONDS + " seconds");
        System.out.println("Payload: " + PAYLOAD_SIZE + " bytes");
        System.out.println();

        try {
            runContinuousBenchmark(bootstrapServers);
        } catch (Exception e) {
            System.err.println("벤치마크 실패: " + e.getMessage());
            e.printStackTrace();
        }
    }

    static void runContinuousBenchmark(String bootstrapServers) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 🚀 최대 처리량 설정 - linger.ms=0으로 즉시 전송
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384 * 8);      // 128KB 배치
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);                // ⚡ 즉시 전송 (배치 대기 없음)
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64 * 1024 * 1024);  // 64MB 버퍼
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        AtomicLong messagesSent = new AtomicLong(0);
        AtomicLong messagesAcked = new AtomicLong(0);
        AtomicLong bytesSent = new AtomicLong(0);
        AtomicLong errors = new AtomicLong(0);
        AtomicBoolean running = new AtomicBoolean(true);

        // 고정 크기 페이로드 생성
        String payload = generatePayload(PAYLOAD_SIZE);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("⏱️  벤치마크 시작...\n");

            long startTime = System.nanoTime();
            AtomicLong lastReportTime = new AtomicLong(startTime);
            AtomicLong lastMessageCount = new AtomicLong(0);

            // 🚀 모니터링 스레드
            Thread monitor = new Thread(() -> {
                try {
                    while (running.get()) {
                        Thread.sleep(1000);  // 1초마다 리포트

                        long now = System.nanoTime();
                        long currentMessages = messagesAcked.get();
                        long prevMessages = lastMessageCount.get();
                        long prevTime = lastReportTime.get();

                        long intervalMessages = currentMessages - prevMessages;
                        double intervalSeconds = (now - prevTime) / 1_000_000_000.0;
                        double throughput = intervalMessages / intervalSeconds;

                        long pending = messagesSent.get() - messagesAcked.get();

                        System.out.printf("📊 %d msg/sec | Total: %,d acked, %,d pending | Errors: %d%n",
                                (long)throughput, currentMessages, pending, errors.get());

                        lastReportTime.set(now);
                        lastMessageCount.set(currentMessages);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            monitor.start();

            // 🚀 메시지 전송 루프 (연속적으로 보냄, flush 없음!)
            long endTime = startTime + (DURATION_SECONDS * 1_000_000_000L);
            int messageId = 0;

            while (System.nanoTime() < endTime) {
                final int msgId = messageId++;
                String key = "key-" + msgId;

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, payload);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        messagesAcked.incrementAndGet();
                        bytesSent.addAndGet(payload.length());
                    } else {
                        errors.incrementAndGet();
                        if (errors.get() <= 10) {  // 처음 10개 에러만 출력
                            System.err.println("전송 실패: " + exception.getMessage());
                        }
                    }
                });

                messagesSent.incrementAndGet();

                // ❌ flush() 호출 없음 - 연속적으로 전송!
            }

            System.out.println("\n⏱️  전송 완료, 응답 대기 중...");

            // 전송 중지, 모니터링 계속
            running.set(false);

            // 🚀 마지막 flush (모든 pending 메시지 전송)
            producer.flush();

            monitor.join();

            long totalTime = System.nanoTime() - startTime;
            double durationSeconds = totalTime / 1_000_000_000.0;

            // 최종 결과
            System.out.println("\n" + "=".repeat(60));
            System.out.println("📊 최종 결과");
            System.out.println("=".repeat(60));
            System.out.printf("총 전송: %,d messages%n", messagesSent.get());
            System.out.printf("총 확인: %,d messages%n", messagesAcked.get());
            System.out.printf("총 실패: %,d messages%n", errors.get());
            System.out.printf("소요 시간: %.2f seconds%n", durationSeconds);
            System.out.printf("평균 처리량: %,.0f msg/sec%n", messagesAcked.get() / durationSeconds);
            System.out.printf("총 데이터: %.2f MB%n", bytesSent.get() / (1024.0 * 1024.0));
            System.out.printf("데이터 전송률: %.2f MB/sec%n",
                    (bytesSent.get() / (1024.0 * 1024.0)) / durationSeconds);
            System.out.println("=".repeat(60));

        }
    }

    static String generatePayload(int size) {
        StringBuilder sb = new StringBuilder(size);
        String base = "x";
        for (int i = 0; i < size; i++) {
            sb.append(base);
        }
        return sb.toString();
    }
}
