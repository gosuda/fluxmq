package com.fluxmq.tests;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

/**
 * Transaction API E2E Test for FluxMQ
 *
 * Tests the following transaction APIs:
 * - InitProducerId (API Key 22)
 * - AddPartitionsToTxn (API Key 24)
 * - AddOffsetsToTxn (API Key 25)
 * - EndTxn (API Key 26)
 * - TxnOffsetCommit (API Key 28)
 */
public class TransactionTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "txn-test-topic";
    private static final String TRANSACTIONAL_ID = "fluxmq-txn-test-" + UUID.randomUUID().toString().substring(0, 8);
    private static final String GROUP_ID = "txn-consumer-group";

    public static void main(String[] args) {
        System.out.println("=================================================");
        System.out.println("  FluxMQ Transaction API E2E Test");
        System.out.println("=================================================");
        System.out.println();

        TransactionTest test = new TransactionTest();

        // Test 1: Transactional Producer (InitProducerId, AddPartitionsToTxn, EndTxn)
        boolean producerTest = test.testTransactionalProducer();

        // Test 2: Transaction with Consumer Offsets (AddOffsetsToTxn, TxnOffsetCommit)
        boolean consumerTest = test.testTransactionWithConsumerOffsets();

        // Test 3: Transaction Abort
        boolean abortTest = test.testTransactionAbort();

        System.out.println();
        System.out.println("=================================================");
        System.out.println("  Test Results Summary");
        System.out.println("=================================================");
        System.out.println("  Transactional Producer: " + (producerTest ? "✅ PASSED" : "❌ FAILED"));
        System.out.println("  Consumer Offset Commit: " + (consumerTest ? "✅ PASSED" : "❌ FAILED"));
        System.out.println("  Transaction Abort:      " + (abortTest ? "✅ PASSED" : "❌ FAILED"));
        System.out.println();

        boolean allPassed = producerTest && consumerTest && abortTest;
        System.out.println("  Overall: " + (allPassed ? "✅ ALL TESTS PASSED" : "❌ SOME TESTS FAILED"));
        System.out.println("=================================================");

        System.exit(allPassed ? 0 : 1);
    }

    /**
     * Test 1: Basic Transactional Producer
     * Tests: InitProducerId, AddPartitionsToTxn, EndTxn (commit)
     */
    private boolean testTransactionalProducer() {
        System.out.println("─────────────────────────────────────────────────");
        System.out.println("Test 1: Transactional Producer");
        System.out.println("─────────────────────────────────────────────────");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID + "-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "30000");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("  [1/4] Initializing transactions (InitProducerId)...");
            producer.initTransactions();
            System.out.println("        ✓ InitProducerId successful");

            System.out.println("  [2/4] Beginning transaction...");
            producer.beginTransaction();
            System.out.println("        ✓ Transaction begun");

            System.out.println("  [3/4] Sending messages (AddPartitionsToTxn)...");
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    TOPIC_NAME,
                    "txn-key-" + i,
                    "txn-value-" + i
                );
                producer.send(record).get();
            }
            System.out.println("        ✓ 10 messages sent within transaction");

            System.out.println("  [4/4] Committing transaction (EndTxn)...");
            producer.commitTransaction();
            System.out.println("        ✓ Transaction committed successfully");

            System.out.println("  → Test 1 PASSED");
            return true;

        } catch (Exception e) {
            System.err.println("  ✗ Test 1 FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Test 2: Transaction with Consumer Offset Commit
     * Tests: AddOffsetsToTxn, TxnOffsetCommit
     */
    private boolean testTransactionWithConsumerOffsets() {
        System.out.println();
        System.out.println("─────────────────────────────────────────────────");
        System.out.println("Test 2: Transaction with Consumer Offsets");
        System.out.println("─────────────────────────────────────────────────");

        // Producer properties
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID + "-consume-transform");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        producerProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "30000");

        // Consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {

            System.out.println("  [1/5] Initializing transactional producer...");
            producer.initTransactions();
            System.out.println("        ✓ Producer initialized");

            System.out.println("  [2/5] Subscribing consumer to topic...");
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            System.out.println("        ✓ Consumer subscribed");

            System.out.println("  [3/5] Polling for records...");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            System.out.println("        ✓ Received " + records.count() + " records");

            System.out.println("  [4/5] Beginning consume-transform-produce transaction...");
            producer.beginTransaction();

            // Send transformed records
            for (ConsumerRecord<String, String> record : records) {
                ProducerRecord<String, String> outputRecord = new ProducerRecord<>(
                    TOPIC_NAME + "-output",
                    record.key(),
                    "TRANSFORMED-" + record.value()
                );
                producer.send(outputRecord);
            }
            System.out.println("        ✓ Sent " + records.count() + " transformed records");

            System.out.println("  [5/5] Committing offsets within transaction (AddOffsetsToTxn + TxnOffsetCommit)...");
            producer.sendOffsetsToTransaction(
                getOffsetsToCommit(records),
                new ConsumerGroupMetadata(GROUP_ID)
            );
            producer.commitTransaction();
            System.out.println("        ✓ Transaction with offsets committed");

            System.out.println("  → Test 2 PASSED");
            return true;

        } catch (Exception e) {
            System.err.println("  ✗ Test 2 FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Test 3: Transaction Abort
     * Tests: EndTxn (abort)
     */
    private boolean testTransactionAbort() {
        System.out.println();
        System.out.println("─────────────────────────────────────────────────");
        System.out.println("Test 3: Transaction Abort");
        System.out.println("─────────────────────────────────────────────────");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID + "-abort");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "30000");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("  [1/4] Initializing transactions...");
            producer.initTransactions();
            System.out.println("        ✓ InitProducerId successful");

            System.out.println("  [2/4] Beginning transaction...");
            producer.beginTransaction();
            System.out.println("        ✓ Transaction begun");

            System.out.println("  [3/4] Sending messages (will be aborted)...");
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    TOPIC_NAME + "-abort-test",
                    "abort-key-" + i,
                    "abort-value-" + i
                );
                producer.send(record).get();
            }
            System.out.println("        ✓ 5 messages sent (pending abort)");

            System.out.println("  [4/4] Aborting transaction (EndTxn with abort)...");
            producer.abortTransaction();
            System.out.println("        ✓ Transaction aborted successfully");

            System.out.println("  → Test 3 PASSED");
            return true;

        } catch (Exception e) {
            System.err.println("  ✗ Test 3 FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    private java.util.Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> getOffsetsToCommit(
            ConsumerRecords<String, String> records) {
        java.util.Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> offsets = new java.util.HashMap<>();
        for (org.apache.kafka.common.TopicPartition partition : records.partitions()) {
            java.util.List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
        }
        return offsets;
    }
}
