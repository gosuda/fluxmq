package com.fluxmq.tests;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * FluxMQ Consumer Group 기능 테스트
 *
 * 테스트 항목:
 * 1. Consumer Group 생성 및 참여
 * 2. 파티션 자동 할당 (리밸런싱)
 * 3. 메시지 소비
 * 4. Offset Commit/Fetch
 * 5. Heartbeat 유지
 * 6. Consumer 추가/제거 시 리밸런싱
 */
public class ConsumerGroupTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TEST_TOPIC = "consumer-group-test";
    private static final String GROUP_ID = "test-consumer-group";
    private static final int NUM_MESSAGES = 100;
    private static final int NUM_CONSUMERS = 3;

    public static void main(String[] args) throws Exception {
        System.out.println("🧪 FluxMQ Consumer Group 기능 테스트 시작\n");

        // 1. 테스트 메시지 생성
        System.out.println("📝 Step 1: 테스트 메시지 생성 중...");
        produceTestMessages();
        System.out.println("✅ " + NUM_MESSAGES + "개 메시지 생성 완료\n");

        // 잠시 대기
        Thread.sleep(1000);

        // 2. Consumer Group 테스트
        System.out.println("👥 Step 2: Consumer Group 테스트 시작");
        System.out.println("   - Consumer 수: " + NUM_CONSUMERS);
        System.out.println("   - Group ID: " + GROUP_ID);
        System.out.println("   - Topic: " + TEST_TOPIC + "\n");

        testConsumerGroup();

        System.out.println("\n✅ 모든 테스트 완료!");
    }

    /**
     * 테스트용 메시지 생성
     */
    private static void produceTestMessages() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < NUM_MESSAGES; i++) {
                String key = "key-" + i;
                String value = "message-" + i + "-" + System.currentTimeMillis();
                ProducerRecord<String, String> record = new ProducerRecord<>(TEST_TOPIC, key, value);
                producer.send(record);

                if ((i + 1) % 20 == 0) {
                    System.out.println("   생성: " + (i + 1) + "/" + NUM_MESSAGES);
                }
            }
            producer.flush();
        } catch (Exception e) {
            System.err.println("❌ 메시지 생성 실패: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Consumer Group 테스트
     */
    private static void testConsumerGroup() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_CONSUMERS);
        AtomicInteger totalConsumed = new AtomicInteger(0);

        // 여러 Consumer 동시 실행
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            final int consumerId = i;
            executor.submit(() -> runConsumer(consumerId, totalConsumed));
        }

        // 30초 동안 실행
        System.out.println("⏱️  30초 동안 Consumer Group 동작 관찰...\n");
        Thread.sleep(30000);

        // 종료
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("\n📊 최종 결과:");
        System.out.println("   - 생성된 메시지: " + NUM_MESSAGES);
        System.out.println("   - 소비된 메시지: " + totalConsumed.get());
        System.out.println("   - Consumer 수: " + NUM_CONSUMERS);
    }

    /**
     * 개별 Consumer 실행
     */
    private static void runConsumer(int consumerId, AtomicInteger totalConsumed) {
        Properties props = createConsumerProps(consumerId);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Topic 구독
            consumer.subscribe(Arrays.asList(TEST_TOPIC));
            System.out.println("🔵 Consumer-" + consumerId + " 시작 (Group: " + GROUP_ID + ")");

            int messageCount = 0;
            long lastPrint = System.currentTimeMillis();

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    // 메시지 폴링
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                    if (!records.isEmpty()) {
                        messageCount += records.count();
                        totalConsumed.addAndGet(records.count());

                        // Offset 커밋
                        consumer.commitSync();

                        // 5초마다 진행 상황 출력
                        long now = System.currentTimeMillis();
                        if (now - lastPrint > 5000) {
                            System.out.println("   Consumer-" + consumerId + ": " + messageCount + "개 소비 (파티션: " + records.partitions() + ")");
                            lastPrint = now;
                        }
                    }
                } catch (Exception e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        System.err.println("❌ Consumer-" + consumerId + " 오류: " + e.getMessage());
                    }
                    break;
                }
            }

            System.out.println("🔴 Consumer-" + consumerId + " 종료 (총 " + messageCount + "개 소비)");

        } catch (Exception e) {
            System.err.println("❌ Consumer-" + consumerId + " 초기화 실패: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Consumer 설정 생성
     */
    private static Properties createConsumerProps(int consumerId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Consumer Group 설정
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 수동 커밋
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 처음부터 읽기
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000"); // 10초
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000"); // 3초
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // Consumer 식별
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-" + consumerId);

        return props;
    }
}
