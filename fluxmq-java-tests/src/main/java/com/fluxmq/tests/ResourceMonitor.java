package com.fluxmq.tests;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 프로세스 리소스 사용량 모니터링 도구
 * CPU, 메모리, 디스크 I/O, 네트워크 I/O 측정
 */
public class ResourceMonitor {
    private final long pid;
    private final String processName;
    private final ScheduledExecutorService scheduler;
    private final List<ResourceSnapshot> snapshots;

    private final AtomicLong totalCpuTime = new AtomicLong(0);
    private final AtomicLong sampleCount = new AtomicLong(0);

    private volatile boolean monitoring = false;

    // JVM 자체 모니터링용
    private final OperatingSystemMXBean osBean;
    private final MemoryMXBean memoryBean;

    public ResourceMonitor(long pid, String processName) {
        this.pid = pid;
        this.processName = processName;
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.snapshots = new ArrayList<>();
        this.osBean = ManagementFactory.getOperatingSystemMXBean();
        this.memoryBean = ManagementFactory.getMemoryMXBean();
    }

    /**
     * 모니터링 시작 (1초 간격)
     */
    public void start() {
        if (monitoring) {
            return;
        }

        monitoring = true;
        snapshots.clear();

        scheduler.scheduleAtFixedRate(() -> {
            try {
                ResourceSnapshot snapshot = captureSnapshot();
                snapshots.add(snapshot);

                totalCpuTime.addAndGet((long) snapshot.cpuPercent);
                sampleCount.incrementAndGet();

            } catch (Exception e) {
                System.err.println("모니터링 오류: " + e.getMessage());
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    /**
     * 모니터링 중지
     */
    public void stop() {
        monitoring = false;
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 현재 순간의 리소스 스냅샷 캡처
     */
    private ResourceSnapshot captureSnapshot() throws Exception {
        ResourceSnapshot snapshot = new ResourceSnapshot();
        snapshot.timestamp = System.currentTimeMillis();

        // macOS의 경우 ps 명령어 사용
        if (pid > 0) {
            snapshot.cpuPercent = getCpuUsage(pid);
            snapshot.memoryMB = getMemoryUsage(pid);
        } else {
            // 자체 JVM 프로세스 모니터링
            snapshot.cpuPercent = getSystemLoadAverage();
            snapshot.memoryMB = getJvmMemoryUsage();
        }

        return snapshot;
    }

    /**
     * 특정 PID의 CPU 사용률 측정 (macOS)
     */
    private double getCpuUsage(long pid) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("ps", "-p", String.valueOf(pid), "-o", "%cpu");
        Process process = pb.start();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            reader.readLine(); // 헤더 스킵
            String line = reader.readLine();
            if (line != null) {
                return Double.parseDouble(line.trim());
            }
        }

        process.waitFor(1, TimeUnit.SECONDS);
        return 0.0;
    }

    /**
     * 특정 PID의 메모리 사용량 측정 (macOS, MB 단위)
     */
    private double getMemoryUsage(long pid) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("ps", "-p", String.valueOf(pid), "-o", "rss=");
        Process process = pb.start();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line = reader.readLine();
            if (line != null) {
                // RSS는 KB 단위로 반환되므로 MB로 변환
                long rssKB = Long.parseLong(line.trim());
                return rssKB / 1024.0;
            }
        }

        process.waitFor(1, TimeUnit.SECONDS);
        return 0.0;
    }

    /**
     * JVM 자체의 메모리 사용량 (MB)
     */
    private double getJvmMemoryUsage() {
        long usedMemory = memoryBean.getHeapMemoryUsage().getUsed();
        return usedMemory / (1024.0 * 1024.0);
    }

    /**
     * 시스템 평균 부하 (CPU 부하 근사치)
     */
    private double getSystemLoadAverage() {
        double loadAvg = osBean.getSystemLoadAverage();
        if (loadAvg < 0) {
            return 0.0;
        }
        // 코어 수로 나누어 백분율로 변환
        int availableProcessors = osBean.getAvailableProcessors();
        return (loadAvg / availableProcessors) * 100.0;
    }

    /**
     * 통계 결과 반환
     */
    public ResourceStats getStats() {
        ResourceStats stats = new ResourceStats();
        stats.processName = processName;
        stats.sampleCount = snapshots.size();

        if (snapshots.isEmpty()) {
            return stats;
        }

        // CPU 통계
        stats.avgCpu = snapshots.stream()
            .mapToDouble(s -> s.cpuPercent)
            .average()
            .orElse(0.0);

        stats.maxCpu = snapshots.stream()
            .mapToDouble(s -> s.cpuPercent)
            .max()
            .orElse(0.0);

        stats.minCpu = snapshots.stream()
            .mapToDouble(s -> s.cpuPercent)
            .min()
            .orElse(0.0);

        // 메모리 통계
        stats.avgMemoryMB = snapshots.stream()
            .mapToDouble(s -> s.memoryMB)
            .average()
            .orElse(0.0);

        stats.maxMemoryMB = snapshots.stream()
            .mapToDouble(s -> s.memoryMB)
            .max()
            .orElse(0.0);

        stats.minMemoryMB = snapshots.stream()
            .mapToDouble(s -> s.memoryMB)
            .min()
            .orElse(0.0);

        return stats;
    }

    /**
     * 리소스 스냅샷 데이터
     */
    static class ResourceSnapshot {
        long timestamp;
        double cpuPercent;
        double memoryMB;
    }

    /**
     * 리소스 통계 결과
     */
    public static class ResourceStats {
        public String processName;
        public int sampleCount;

        // CPU 통계
        public double avgCpu;
        public double maxCpu;
        public double minCpu;

        // 메모리 통계 (MB)
        public double avgMemoryMB;
        public double maxMemoryMB;
        public double minMemoryMB;

        @Override
        public String toString() {
            return String.format(
                "📊 리소스 사용량 통계 (%s)\n" +
                "==================\n" +
                "샘플 수: %d\n" +
                "\n" +
                "CPU 사용률:\n" +
                "  평균: %.2f%%\n" +
                "  최대: %.2f%%\n" +
                "  최소: %.2f%%\n" +
                "\n" +
                "메모리 사용량 (MB):\n" +
                "  평균: %.2f MB\n" +
                "  최대: %.2f MB\n" +
                "  최소: %.2f MB\n",
                processName, sampleCount,
                avgCpu, maxCpu, minCpu,
                avgMemoryMB, maxMemoryMB, minMemoryMB
            );
        }

        /**
         * JSON 형식으로 출력
         */
        public String toJson() {
            return String.format(
                "{\n" +
                "  \"processName\": \"%s\",\n" +
                "  \"sampleCount\": %d,\n" +
                "  \"cpu\": {\n" +
                "    \"avg\": %.2f,\n" +
                "    \"max\": %.2f,\n" +
                "    \"min\": %.2f\n" +
                "  },\n" +
                "  \"memory\": {\n" +
                "    \"avg\": %.2f,\n" +
                "    \"max\": %.2f,\n" +
                "    \"min\": %.2f\n" +
                "  }\n" +
                "}",
                processName, sampleCount,
                avgCpu, maxCpu, minCpu,
                avgMemoryMB, maxMemoryMB, minMemoryMB
            );
        }
    }

    /**
     * 사용 예제
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("사용법: java ResourceMonitor <PID> <프로세스명> [duration_seconds]");
            System.out.println("예제: java ResourceMonitor 12345 FluxMQ 30");
            return;
        }

        long pid = Long.parseLong(args[0]);
        String processName = args[1];
        int duration = args.length > 2 ? Integer.parseInt(args[2]) : 10;

        ResourceMonitor monitor = new ResourceMonitor(pid, processName);

        System.out.println("🔍 " + processName + " 모니터링 시작 (PID: " + pid + ")");
        System.out.println("   기간: " + duration + "초");
        System.out.println();

        monitor.start();
        Thread.sleep(duration * 1000L);
        monitor.stop();

        ResourceStats stats = monitor.getStats();
        System.out.println(stats);
        System.out.println();
        System.out.println("JSON 출력:");
        System.out.println(stats.toJson());
    }
}
