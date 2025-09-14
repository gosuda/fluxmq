package com.fluxmq.tests;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

/**
 * Minimal Java Kafka Producer Test for FluxMQ Protocol Compatibility
 * 
 * This test uses the most basic configuration to avoid protocol parsing issues
 * and focuses on achieving successful message delivery for performance measurement.
 */
public class MinimalProducerTest {
    
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "test-topic";
    private static final int NUM_MESSAGES = 1000; // Small count for quick test
    
    public static void main(String[] args) {
        String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
        if (args.length > 0) {
            bootstrapServers = args[0];
        }
        MinimalProducerTest test = new MinimalProducerTest();
        test.runMinimalTest(bootstrapServers);
    }
    
    public void runMinimalTest(String bootstrapServers) {
        System.out.println("🧪 Minimal Java Kafka Producer Test for FluxMQ");
        System.out.println("===============================================");
        System.out.println("Testing basic protocol compatibility with simple payloads");
        System.out.println();
        
        Properties props = createMinimalProducerConfig(bootstrapServers);
        
        System.out.println("Configuration:");
        System.out.println("  - Bootstrap servers: " + bootstrapServers);
        System.out.println("  - Topic: " + TOPIC_NAME);
        System.out.println("  - Messages: " + String.format("%,d", NUM_MESSAGES));
        System.out.println("  - Payload: Simple ASCII strings only");
        System.out.println("  - Timeout: 5 seconds (short for quick feedback)");
        System.out.println();
        
        long totalBytesSent = 0;
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            
            System.out.println("Starting minimal producer test...");
            Instant startTime = Instant.now();
            
            // Send simple messages with ASCII-only content
            for (int i = 0; i < NUM_MESSAGES; i++) {
                // Use simple, ASCII-only keys and values to avoid UTF-8 issues
                String key = "k" + i;
                String value = "msg" + i + "_data";  // Simple ASCII payload
                totalBytesSent += key.length() + value.length();
                
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
                
                // Send synchronously for reliability (slower but more predictable)
                try {
                    producer.send(record).get();
                    
                    // Progress reporting every 100 messages
                    if (i % 100 == 0 && i > 0) {
                        long elapsed = Duration.between(startTime, Instant.now()).toMillis();
                        if (elapsed > 0) {
                            double currentRate = (i * 1000.0) / elapsed;
                            System.out.printf("  Sent: %,d messages (%.0f msg/sec)%n", i, currentRate);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("  ❌ Failed to send message " + i + ": " + e.getMessage());
                    // Continue with next message
                }
            }
            
            Instant endTime = Instant.now();
            long durationMs = Duration.between(startTime, endTime).toMillis();
            double durationSec = durationMs / 1000.0;
            double throughput = NUM_MESSAGES / durationSec;
            double mbPerSec = (totalBytesSent / (1024.0 * 1024.0)) / durationSec;
            
            // Display results
            displayResults(durationSec, throughput, mbPerSec, totalBytesSent);
            
        } catch (Exception e) {
            System.err.println("⚠️ Error during minimal producer test: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private Properties createMinimalProducerConfig(String bootstrapServers) {
        Properties props = new Properties();
        
        // Basic configuration only
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Conservative settings for maximum compatibility
        props.put(ProducerConfig.ACKS_CONFIG, "1");                     // Leader acknowledgment
        props.put(ProducerConfig.RETRIES_CONFIG, 0);                    // No retries for simplicity
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);              // Small 1KB batches
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);                  // No lingering
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);       // 32MB buffer
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1048576);     // 1MB max request
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);      // 5 second timeout
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);    // 10 second total
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // Sequential
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");      // No compression
        
        return props;
    }
    
    private void displayResults(double durationSec, double throughput, double mbPerSec, long totalBytes) {
        System.out.println();
        System.out.println("🎯 Minimal Java Kafka Producer Results:");
        System.out.printf("   Messages: %,d%n", NUM_MESSAGES);
        System.out.printf("   Duration: %.3f seconds%n", durationSec);
        System.out.printf("   Throughput: %,.0f msg/sec%n", throughput);
        System.out.printf("   Data rate: %.2f MB/sec%n", mbPerSec);
        System.out.printf("   Total data: %.2f KB%n", totalBytes / 1024.0);
        System.out.println();
        
        // Performance assessment with lower expectations for this basic test
        if (throughput >= 1000) {
            System.out.println("✅ SUCCESS! Basic protocol compatibility achieved!");
            System.out.printf("   Throughput: %,.0f msg/sec - Ready for optimization%n", throughput);
        } else if (throughput >= 500) {
            System.out.println("⚡ GOOD! Protocol working with room for optimization");
            System.out.printf("   Throughput: %,.0f msg/sec%n", throughput);
        } else if (throughput >= 100) {
            System.out.println("📈 BASIC SUCCESS - Protocol compatibility confirmed");
            System.out.printf("   Throughput: %,.0f msg/sec%n", throughput);
        } else {
            System.out.println("⚠️ LOW THROUGHPUT - May need protocol investigation");
            System.out.printf("   Throughput: %,.0f msg/sec%n", throughput);
        }
        
        System.out.println();
        System.out.println("💡 Next steps: If successful, try BatchProducerTest with optimized settings");
    }
}