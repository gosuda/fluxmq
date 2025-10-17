package com.fluxmq.tests;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 메가 배치 FluxMQ 벤치마크 - 1MB 배치 크기 + acks=0
 * 목표: 200,000+ msg/sec 달성 (배치 크기 2배 증가로 성능 향상)
 */
public class MegaBatchBenchmark {
    private static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "mega-batch-topic";
    private static final int MESSAGE_COUNT = 2000000;  // 200만 메시지로 증가 (안정적인 선에서)
    private static final int THREAD_COUNT = 12;  // 12개 스레드로 증가 (적당한 병렬성)
    private static PrintWriter logWriter;
    
    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            BOOTSTRAP_SERVERS = args[0];
        }

        // 로그 파일 설정
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String logFileName = "fluxmq_benchmark_" + timestamp + ".log";
        logWriter = new PrintWriter(new FileWriter(logFileName, true), true);

        // 콘솔과 파일에 모두 로깅
        log("🚀 FluxMQ 메가 배치 벤치마크 시작");
        log("=====================================");
        log("서버: " + BOOTSTRAP_SERVERS);
        log("배치 크기: 1MB (1048576 bytes)");
        log("메시지 수: " + MESSAGE_COUNT);
        log("스레드 수: " + THREAD_COUNT);
        log("목표: 200,000+ msg/sec 달성!");
        log("로그 파일: " + logFileName);
        log("");

        try {
            // 웜업
            log("⏳ 웜업 중 (5,000 메시지)...");
            runBenchmark(5000);
            Thread.sleep(3000);

            // 메인 벤치마크
            log("🎯 메가 배치 벤치마크 실행 중...");
            BenchmarkResult result = runBenchmark(MESSAGE_COUNT);

            // 결과 출력
            printResults(result);

            // 성공 메시지
            log("✅ 벤치마크 완료!");
        } catch (Exception e) {
            log("❌ 오류 발생: " + e.getMessage());
            e.printStackTrace(logWriter);
        } finally {
            if (logWriter != null) {
                logWriter.close();
            }
        }
    }

    private static void log(String message) {
        System.out.println(message);
        if (logWriter != null) {
            logWriter.println("[" + new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()) + "] " + message);
            logWriter.flush();
        }
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
                    
                    // 배치 단위로 전송하여 메모리 효율성 향상
                    int batchSize = 1000;
                    for (int j = 0; j < messagesPerThread; j++) {
                        String key = "mega-thread-" + threadId + "-msg-" + j;
                        String value = "메가배치_초고성능_데이터_" + threadId + "_" + j;  // 메시지 크기 축소
                        
                        ProducerRecord<String, String> record = 
                            new ProducerRecord<>(TOPIC, key, value);
                        
                        // Fire-and-forget 방식으로 콜백 제거 (메모리 절약)
                        producer.send(record);
                        totalMessages.incrementAndGet();
                        totalBytes.addAndGet(key.length() + value.length());

                        // 주기적으로 flush하여 메모리 해제
                        if (j > 0 && j % batchSize == 0) {
                            producer.flush();
                        }
                    }
                    
                    producer.flush();
                    producer.close();
                    
                } catch (Exception e) {
                    log("스레드 " + threadId + " 오류: " + e.getMessage());
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
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "1048576");  // 1MB 배치로 복구 (더 큰 배치)
        props.put(ProducerConfig.LINGER_MS_CONFIG, "15");  // 15ms 대기로 더 큰 배치 생성
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");  // LZ4 압축으로 네트워크 최적화
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "134217728");  // 128MB 버퍼로 증가 (성능 향상)
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

        log("\n🏆 메가 배치 벤치마크 결과");
        log("==============================");
        log(String.format("📊 메시지 수: %,d", result.messageCount));
        log(String.format("⏱️  소요 시간: %.2f 초", durationSeconds));
        log(String.format("🚀 처리량: %,.0f msg/sec", messagesPerSecond));
        log(String.format("💾 대역폭: %.2f MB/sec", mbPerSecond));
        log(String.format("📈 평균 지연시간: %.3f ms/msg", 1000.0 / messagesPerSecond));

        // 성능 평가
        if (messagesPerSecond >= 200000) {
            log("🎉 목표 달성! 200k+ msg/sec 메가 성능!");
        } else if (messagesPerSecond >= 150000) {
            log("🔥 탁월! 150k+ msg/sec 고성능!");
        } else if (messagesPerSecond >= 100000) {
            log("✅ 우수! 100k+ msg/sec 달성!");
        } else if (messagesPerSecond >= 50000) {
            log("👍 양호! 50k+ msg/sec");
        } else {
            log("📊 기본 성능 - 추가 최적화 고려");
        }

        log("\n🔧 메가 배치 최적화 설정:");
        log("- acks=0 (fire-and-forget)");
        log("- batch.size=1MB (메가 배치!)");
        log("- linger.ms=15 (배치 최적화)");
        log("- compression=lz4 (압축)");
        log("- buffer.memory=256MB");
        log("- max.in.flight=100 (초병렬)");
        log("- send/receive.buffer=2MB");
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