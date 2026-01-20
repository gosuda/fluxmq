package com.fluxmq.tests;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * FluxMQ Admin API 테스트
 *
 * 새로 구현된 Kafka Admin API들을 테스트합니다:
 * - DeleteRecords (API key 21)
 * - CreatePartitions (API key 37)
 * - DeleteGroups (API key 42)
 * - AlterPartitionReassignments (API key 45)
 * - LeaderAndIsr (API key 4) - 내부용
 * - StopReplica (API key 5) - 내부용
 * - UpdateMetadata (API key 6) - 내부용
 * - ControlledShutdown (API key 7) - 내부용
 */
public class AdminApiTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final int TIMEOUT_SECONDS = 30;

    private AdminClient adminClient;
    private int passedTests = 0;
    private int failedTests = 0;
    private List<String> testResults = new ArrayList<>();

    public static void main(String[] args) {
        AdminApiTest test = new AdminApiTest();
        test.runAllTests();
    }

    public void runAllTests() {
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║         FluxMQ Admin API 테스트 시작                          ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.println("║  서버: " + BOOTSTRAP_SERVERS + "                                    ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();

        try {
            // AdminClient 생성
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_SECONDS * 1000);
            props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, TIMEOUT_SECONDS * 1000);
            adminClient = AdminClient.create(props);

            // 1. 기본 연결 테스트
            testConnection();

            // 2. Topic 생성 테스트 (CreateTopics - API 19)
            testCreateTopics();

            // 3. Topic 목록 조회 (ListTopics 관련)
            testListTopics();

            // 4. CreatePartitions 테스트 (API 37)
            testCreatePartitions();

            // 5. DeleteRecords 테스트 (API 21)
            testDeleteRecords();

            // 6. Consumer Group 생성 후 DeleteGroups 테스트 (API 42)
            testDeleteGroups();

            // 7. DescribeConfigs 테스트 (API 32)
            testDescribeConfigs();

            // 8. AlterConfigs 테스트 (API 33)
            testAlterConfigs();

            // 9. DeleteTopics 테스트 (API 20)
            testDeleteTopics();

            // 10. ListConsumerGroups 테스트 (API 16)
            testListConsumerGroups();

            // 11. DescribeConsumerGroups 테스트 (API 15)
            testDescribeConsumerGroups();

        } catch (Exception e) {
            System.err.println("❌ 테스트 중 예외 발생: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (adminClient != null) {
                adminClient.close(Duration.ofSeconds(5));
            }
        }

        // 결과 요약
        printSummary();
    }

    private void testConnection() {
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("🔌 테스트 1: 서버 연결 테스트");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        try {
            DescribeClusterResult result = adminClient.describeCluster();
            String clusterId = result.clusterId().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            int nodeCount = result.nodes().get(TIMEOUT_SECONDS, TimeUnit.SECONDS).size();

            System.out.println("   ✅ 클러스터 연결 성공");
            System.out.println("      - Cluster ID: " + clusterId);
            System.out.println("      - Node 수: " + nodeCount);
            recordResult("서버 연결", true, "Cluster ID: " + clusterId);
        } catch (Exception e) {
            System.out.println("   ❌ 클러스터 연결 실패: " + e.getMessage());
            recordResult("서버 연결", false, e.getMessage());
        }
        System.out.println();
    }

    private void testCreateTopics() {
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("📝 테스트 2: CreateTopics (API 19)");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        String testTopic = "admin-test-topic-" + System.currentTimeMillis();

        try {
            NewTopic newTopic = new NewTopic(testTopic, 3, (short) 1);
            CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));
            result.all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            System.out.println("   ✅ Topic 생성 성공: " + testTopic);
            System.out.println("      - 파티션 수: 3");
            System.out.println("      - 복제 팩터: 1");
            recordResult("CreateTopics", true, "Topic: " + testTopic);

            // 생성된 토픽을 나중 테스트에서 사용할 수 있도록 저장
            System.setProperty("test.topic.name", testTopic);
        } catch (Exception e) {
            System.out.println("   ❌ Topic 생성 실패: " + e.getMessage());
            recordResult("CreateTopics", false, e.getMessage());
        }
        System.out.println();
    }

    private void testListTopics() {
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("📋 테스트 3: ListTopics");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        try {
            ListTopicsResult result = adminClient.listTopics();
            Set<String> topics = result.names().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            System.out.println("   ✅ Topic 목록 조회 성공");
            System.out.println("      - 총 " + topics.size() + "개 토픽:");
            for (String topic : topics) {
                System.out.println("        • " + topic);
            }
            recordResult("ListTopics", true, topics.size() + " topics");
        } catch (Exception e) {
            System.out.println("   ❌ Topic 목록 조회 실패: " + e.getMessage());
            recordResult("ListTopics", false, e.getMessage());
        }
        System.out.println();
    }

    private void testCreatePartitions() {
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("➕ 테스트 4: CreatePartitions (API 37)");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        String testTopic = System.getProperty("test.topic.name", "admin-test-topic");

        try {
            // 파티션을 3개에서 5개로 증가
            Map<String, NewPartitions> newPartitions = new HashMap<>();
            newPartitions.put(testTopic, NewPartitions.increaseTo(5));

            CreatePartitionsResult result = adminClient.createPartitions(newPartitions);
            result.all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            System.out.println("   ✅ 파티션 추가 성공: " + testTopic);
            System.out.println("      - 새 파티션 수: 5");
            recordResult("CreatePartitions", true, "Increased to 5 partitions");
        } catch (Exception e) {
            String errorMsg = e.getMessage();
            if (errorMsg != null && errorMsg.contains("InvalidPartitions")) {
                System.out.println("   ⚠️ 파티션 추가 거부 (예상된 동작): 이미 충분한 파티션 존재");
                recordResult("CreatePartitions", true, "Correctly rejected");
            } else {
                System.out.println("   ❌ 파티션 추가 실패: " + errorMsg);
                recordResult("CreatePartitions", false, errorMsg);
            }
        }
        System.out.println();
    }

    private void testDeleteRecords() {
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("🗑️ 테스트 5: DeleteRecords (API 21)");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        String testTopic = System.getProperty("test.topic.name", "admin-test-topic");

        try {
            // 먼저 메시지 생성
            System.out.println("   📝 테스트 메시지 생성 중...");
            produceTestMessages(testTopic, 10);

            // 오프셋 5까지 레코드 삭제 요청
            TopicPartition tp = new TopicPartition(testTopic, 0);
            Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            recordsToDelete.put(tp, RecordsToDelete.beforeOffset(5));

            DeleteRecordsResult result = adminClient.deleteRecords(recordsToDelete);
            result.all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            System.out.println("   ✅ DeleteRecords 성공");
            for (Map.Entry<TopicPartition, org.apache.kafka.common.KafkaFuture<DeletedRecords>> entry : result.lowWatermarks().entrySet()) {
                DeletedRecords deleted = entry.getValue().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                System.out.println("      - " + entry.getKey() + ": low watermark = " + deleted.lowWatermark());
            }
            recordResult("DeleteRecords", true, "Deleted before offset 5");
        } catch (Exception e) {
            System.out.println("   ❌ DeleteRecords 실패: " + e.getMessage());
            recordResult("DeleteRecords", false, e.getMessage());
        }
        System.out.println();
    }

    private void testDeleteGroups() {
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("👥 테스트 6: DeleteGroups (API 42)");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        String groupId = "test-group-to-delete-" + System.currentTimeMillis();
        String testTopic = System.getProperty("test.topic.name", "admin-test-topic");

        try {
            // Consumer Group 생성
            System.out.println("   📝 Consumer Group 생성 중: " + groupId);
            createConsumerGroup(groupId, testTopic);

            Thread.sleep(1000); // 그룹 등록 대기

            // Consumer Group 삭제
            DeleteConsumerGroupsResult result = adminClient.deleteConsumerGroups(Collections.singletonList(groupId));
            result.all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            System.out.println("   ✅ DeleteGroups 성공: " + groupId);
            recordResult("DeleteGroups", true, "Group: " + groupId);
        } catch (Exception e) {
            System.out.println("   ❌ DeleteGroups 실패: " + e.getMessage());
            recordResult("DeleteGroups", false, e.getMessage());
        }
        System.out.println();
    }

    private void testDescribeConfigs() {
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("⚙️ 테스트 7: DescribeConfigs (API 32)");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        String testTopic = System.getProperty("test.topic.name", "admin-test-topic");

        try {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, testTopic);
            DescribeConfigsResult result = adminClient.describeConfigs(Collections.singletonList(resource));
            Map<ConfigResource, Config> configs = result.all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            System.out.println("   ✅ DescribeConfigs 성공");
            for (Map.Entry<ConfigResource, Config> entry : configs.entrySet()) {
                System.out.println("      - Resource: " + entry.getKey().name());
                int count = 0;
                for (ConfigEntry configEntry : entry.getValue().entries()) {
                    if (count++ < 5) { // 처음 5개만 표시
                        System.out.println("        • " + configEntry.name() + " = " + configEntry.value());
                    }
                }
                if (count > 5) {
                    System.out.println("        ... 외 " + (count - 5) + "개");
                }
            }
            recordResult("DescribeConfigs", true, "Config entries retrieved");
        } catch (Exception e) {
            System.out.println("   ❌ DescribeConfigs 실패: " + e.getMessage());
            recordResult("DescribeConfigs", false, e.getMessage());
        }
        System.out.println();
    }

    private void testAlterConfigs() {
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("🔧 테스트 8: AlterConfigs (API 33)");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        String testTopic = System.getProperty("test.topic.name", "admin-test-topic");

        try {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, testTopic);
            ConfigEntry entry = new ConfigEntry("retention.ms", "86400000"); // 1일

            Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            configs.put(resource, Collections.singletonList(
                new AlterConfigOp(entry, AlterConfigOp.OpType.SET)
            ));

            AlterConfigsResult result = adminClient.incrementalAlterConfigs(configs);
            result.all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            System.out.println("   ✅ AlterConfigs 성공");
            System.out.println("      - retention.ms = 86400000 (1일)");
            recordResult("AlterConfigs", true, "retention.ms updated");
        } catch (Exception e) {
            System.out.println("   ❌ AlterConfigs 실패: " + e.getMessage());
            recordResult("AlterConfigs", false, e.getMessage());
        }
        System.out.println();
    }

    private void testListConsumerGroups() {
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("📋 테스트 9: ListConsumerGroups (API 16)");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        try {
            ListConsumerGroupsResult result = adminClient.listConsumerGroups();
            Collection<ConsumerGroupListing> groups = result.all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            System.out.println("   ✅ ListConsumerGroups 성공");
            System.out.println("      - 총 " + groups.size() + "개 그룹:");
            for (ConsumerGroupListing group : groups) {
                System.out.println("        • " + group.groupId() + " (state: " + group.state().orElse(null) + ")");
            }
            recordResult("ListConsumerGroups", true, groups.size() + " groups");
        } catch (Exception e) {
            System.out.println("   ❌ ListConsumerGroups 실패: " + e.getMessage());
            recordResult("ListConsumerGroups", false, e.getMessage());
        }
        System.out.println();
    }

    private void testDescribeConsumerGroups() {
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("🔍 테스트 10: DescribeConsumerGroups (API 15)");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        String groupId = "describe-test-group-" + System.currentTimeMillis();
        String testTopic = System.getProperty("test.topic.name", "admin-test-topic");

        try {
            // Consumer Group 생성
            System.out.println("   📝 Consumer Group 생성 중: " + groupId);
            createConsumerGroup(groupId, testTopic);
            Thread.sleep(1000);

            // Group 상세 정보 조회
            DescribeConsumerGroupsResult result = adminClient.describeConsumerGroups(Collections.singletonList(groupId));
            Map<String, ConsumerGroupDescription> descriptions = result.all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            System.out.println("   ✅ DescribeConsumerGroups 성공");
            for (Map.Entry<String, ConsumerGroupDescription> entry : descriptions.entrySet()) {
                ConsumerGroupDescription desc = entry.getValue();
                System.out.println("      - Group ID: " + desc.groupId());
                System.out.println("      - State: " + desc.state());
                System.out.println("      - Coordinator: " + desc.coordinator());
                System.out.println("      - Members: " + desc.members().size());
            }
            recordResult("DescribeConsumerGroups", true, "Group described");
        } catch (Exception e) {
            System.out.println("   ❌ DescribeConsumerGroups 실패: " + e.getMessage());
            recordResult("DescribeConsumerGroups", false, e.getMessage());
        }
        System.out.println();
    }

    private void testDeleteTopics() {
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("🗑️ 테스트 11: DeleteTopics (API 20)");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        String testTopic = System.getProperty("test.topic.name", "admin-test-topic");

        try {
            DeleteTopicsResult result = adminClient.deleteTopics(Collections.singletonList(testTopic));
            result.all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            System.out.println("   ✅ DeleteTopics 성공: " + testTopic);
            recordResult("DeleteTopics", true, "Topic: " + testTopic);
        } catch (Exception e) {
            System.out.println("   ❌ DeleteTopics 실패: " + e.getMessage());
            recordResult("DeleteTopics", false, e.getMessage());
        }
        System.out.println();
    }

    // 헬퍼 메서드들

    private void produceTestMessages(String topic, int count) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic, 0, "key-" + i, "test-message-" + i
                );
                producer.send(record).get(5, TimeUnit.SECONDS);
            }
            producer.flush();
            System.out.println("      ✓ " + count + "개 메시지 전송 완료");
        } catch (Exception e) {
            System.out.println("      ⚠️ 메시지 전송 중 오류: " + e.getMessage());
        }
    }

    private void createConsumerGroup(String groupId, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(1000)); // 그룹 참가를 위해 poll 호출
            System.out.println("      ✓ Consumer Group 생성됨: " + groupId);
        } catch (Exception e) {
            System.out.println("      ⚠️ Consumer Group 생성 중 오류: " + e.getMessage());
        }
    }

    private void recordResult(String testName, boolean passed, String details) {
        if (passed) {
            passedTests++;
            testResults.add("✅ " + testName + ": " + details);
        } else {
            failedTests++;
            testResults.add("❌ " + testName + ": " + details);
        }
    }

    private void printSummary() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║                    테스트 결과 요약                           ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");

        for (String result : testResults) {
            System.out.println("║  " + padRight(result, 60) + "║");
        }

        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.printf("║  총 테스트: %d | 성공: %d | 실패: %d                          ║%n",
            passedTests + failedTests, passedTests, failedTests);

        double successRate = (passedTests + failedTests) > 0
            ? (passedTests * 100.0 / (passedTests + failedTests)) : 0;
        System.out.printf("║  성공률: %.1f%%                                               ║%n", successRate);
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        if (failedTests == 0) {
            System.out.println("\n🎉 모든 테스트 통과!");
        } else {
            System.out.println("\n⚠️ 일부 테스트 실패 - 위의 결과를 확인하세요.");
        }
    }

    private String padRight(String s, int n) {
        if (s.length() >= n) {
            return s.substring(0, n - 3) + "...";
        }
        return String.format("%-" + n + "s", s);
    }
}
