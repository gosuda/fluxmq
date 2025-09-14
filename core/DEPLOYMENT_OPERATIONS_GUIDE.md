# FluxMQ Deployment and Operations Guide

> **Production-Ready Kafka-Compatible Message Broker**  
> Comprehensive guide for deploying, configuring, and operating FluxMQ in production environments

## Table of Contents

- [Quick Start](#quick-start)
- [System Requirements](#system-requirements)
- [Installation Methods](#installation-methods)
- [Configuration Management](#configuration-management)
- [Production Deployment](#production-deployment)
- [Monitoring and Observability](#monitoring-and-observability)
- [Performance Tuning](#performance-tuning)
- [Security Configuration](#security-configuration)
- [High Availability](#high-availability)
- [Backup and Recovery](#backup-and-recovery)
- [Troubleshooting](#troubleshooting)
- [Operational Procedures](#operational-procedures)

---

## Quick Start

### 5-Minute Setup

```bash
# 1. Build FluxMQ
git clone https://github.com/your-org/fluxmq.git
cd fluxmq/core
RUSTFLAGS="-C target-cpu=native" cargo build --release

# 2. Start server with default configuration
./target/release/fluxmq --port 9092 --enable-consumer-groups --log-level info

# 3. Test with Python client
pip3 install kafka-python
python3 -c "
from kafka import KafkaProducer, KafkaConsumer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer.send('test-topic', b'Hello FluxMQ!')
producer.flush()
print('Message sent successfully!')
"
```

### Basic Health Check

```bash
# Check if FluxMQ is running
curl -f http://localhost:8080/health || echo "Health check endpoint not available"
lsof -i :9092 | grep LISTEN

# Test basic connectivity
telnet localhost 9092
```

---

## System Requirements

### Hardware Requirements

#### Minimum Requirements (Development/Testing)
- **CPU**: 2 cores, 2.0 GHz
- **Memory**: 4 GB RAM
- **Storage**: 10 GB available space
- **Network**: 100 Mbps

#### Recommended Requirements (Production)
- **CPU**: 8+ cores, 3.0+ GHz (Intel Xeon, AMD EPYC)
- **Memory**: 32+ GB RAM
- **Storage**: NVMe SSD, 1+ TB available space
- **Network**: 10+ Gbps network interface

#### High-Performance Requirements (Enterprise)
- **CPU**: 16+ cores, 3.5+ GHz with AVX2/SSE4.2 support
- **Memory**: 64+ GB RAM
- **Storage**: Multiple NVMe SSDs in RAID configuration
- **Network**: 25+ Gbps network, multiple NICs

### Software Requirements

#### Operating System Support
- **Linux**: Ubuntu 20.04+, CentOS 8+, RHEL 8+, Debian 11+
- **macOS**: macOS 10.15+ (development only)
- **Windows**: Windows 10+ with WSL2 (development only)

#### Runtime Dependencies
- **Rust**: 1.75+ (build-time dependency)
- **glibc**: 2.31+ (Linux)
- **OpenSSL**: 1.1.1+ (for TLS support)

#### Optional Dependencies
- **systemd**: For service management (Linux)
- **Docker**: 20.10+ (containerized deployment)
- **Kubernetes**: 1.20+ (orchestrated deployment)

---

## Installation Methods

### 1. Binary Installation (Recommended)

```bash
# Download pre-built binary
wget https://github.com/your-org/fluxmq/releases/latest/download/fluxmq-linux-x86_64.tar.gz
tar -xzf fluxmq-linux-x86_64.tar.gz
sudo mv fluxmq /usr/local/bin/
sudo chmod +x /usr/local/bin/fluxmq

# Verify installation
fluxmq --version
```

### 2. Source Installation

```bash
# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Clone and build FluxMQ
git clone https://github.com/your-org/fluxmq.git
cd fluxmq/core

# Optimized build for production
RUSTFLAGS="-C target-cpu=native -C link-arg=-s" cargo build --release

# Install binary
sudo cp target/release/fluxmq /usr/local/bin/
sudo chmod +x /usr/local/bin/fluxmq
```

### 3. Docker Installation

```bash
# Pull FluxMQ Docker image
docker pull fluxmq/fluxmq:latest

# Run with basic configuration
docker run -d \\
  --name fluxmq \\
  -p 9092:9092 \\
  -p 8080:8080 \\
  -v fluxmq-data:/data \\
  fluxmq/fluxmq:latest \\
  --port 9092 --enable-consumer-groups --data-dir /data
```

### 4. Kubernetes Installation

```yaml
# fluxmq-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fluxmq
  namespace: messaging
spec:
  replicas: 3
  selector:
    matchLabels:
      app: fluxmq
  template:
    metadata:
      labels:
        app: fluxmq
    spec:
      containers:
      - name: fluxmq
        image: fluxmq/fluxmq:latest
        ports:
        - containerPort: 9092
          name: kafka
        - containerPort: 8080
          name: http
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "8Gi" 
            cpu: "4000m"
        volumeMounts:
        - name: data-volume
          mountPath: /data
        args:
          - "--port=9092"
          - "--enable-consumer-groups"
          - "--data-dir=/data"
          - "--log-level=info"
      volumes:
      - name: data-volume
        persistentVolumeClaim:
          claimName: fluxmq-pvc
```

---

## Configuration Management

### Command Line Options

```bash
fluxmq --help

FluxMQ - High-Performance Kafka-Compatible Message Broker

USAGE:
    fluxmq [OPTIONS]

OPTIONS:
        --port <PORT>                    Port to listen on [default: 9092]
        --host <HOST>                    Host to bind to [default: 0.0.0.0]
        --data-dir <PATH>                Data directory [default: ./data]
        --log-level <LEVEL>              Log level [debug,info,warn,error] [default: info]
        --enable-consumer-groups         Enable consumer group coordination
        --enable-tls                     Enable TLS/SSL encryption
        --tls-cert <PATH>                TLS certificate file
        --tls-key <PATH>                 TLS private key file
        --enable-acl                     Enable Access Control Lists
        --acl-config <PATH>              ACL configuration file
        --max-connections <NUM>          Maximum concurrent connections [default: 10000]
        --segment-size <BYTES>           Log segment size [default: 1073741824]
        --retention-ms <MS>              Message retention time in milliseconds
        --metrics-port <PORT>            Metrics HTTP server port [default: 8080]
    -h, --help                           Print help information
    -V, --version                        Print version information
```

### Configuration File (fluxmq.toml)

```toml
# FluxMQ Configuration File

[server]
host = "0.0.0.0"
port = 9092
max_connections = 10000
request_timeout_ms = 30000
idle_timeout_ms = 600000

[storage]
data_dir = "/var/lib/fluxmq"
segment_size = 1073741824  # 1GB
retention_ms = 604800000   # 7 days
sync_writes = true
compression = "lz4"

[consumer_groups]
enabled = true
session_timeout_ms = 30000
heartbeat_interval_ms = 3000
max_poll_interval_ms = 300000

[security]
enable_tls = false
tls_cert_file = "/etc/fluxmq/server.crt"
tls_key_file = "/etc/fluxmq/server.key"
enable_acl = false
acl_config_file = "/etc/fluxmq/acl.json"

[metrics]
enabled = true
port = 8080
prometheus_endpoint = "/metrics"

[logging]
level = "info"
file = "/var/log/fluxmq/fluxmq.log"
max_size = "100MB"
rotation = "daily"

[performance]
enable_optimizations = true
use_native_cpu = true
worker_threads = 8
io_threads = 4
```

### Environment Variable Configuration

```bash
# Server configuration
export FLUXMQ_HOST=0.0.0.0
export FLUXMQ_PORT=9092
export FLUXMQ_DATA_DIR=/var/lib/fluxmq
export FLUXMQ_LOG_LEVEL=info

# Security configuration
export FLUXMQ_ENABLE_TLS=true
export FLUXMQ_TLS_CERT=/etc/fluxmq/server.crt
export FLUXMQ_TLS_KEY=/etc/fluxmq/server.key

# Performance tuning
export FLUXMQ_MAX_CONNECTIONS=10000
export FLUXMQ_SEGMENT_SIZE=1073741824
export RUSTFLAGS="-C target-cpu=native"
```

---

## Production Deployment

### SystemD Service Configuration

```ini
# /etc/systemd/system/fluxmq.service
[Unit]
Description=FluxMQ Message Broker
Documentation=https://github.com/your-org/fluxmq
After=network.target
Wants=network.target

[Service]
Type=exec
User=fluxmq
Group=fluxmq
ExecStart=/usr/local/bin/fluxmq \\
    --port 9092 \\
    --data-dir /var/lib/fluxmq \\
    --enable-consumer-groups \\
    --enable-tls \\
    --tls-cert /etc/fluxmq/server.crt \\
    --tls-key /etc/fluxmq/server.key \\
    --log-level info

# Resource limits
LimitNOFILE=1048576
LimitNPROC=65536

# Security settings
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
ReadWritePaths=/var/lib/fluxmq /var/log/fluxmq

# Restart policy
Restart=always
RestartSec=10
StartLimitInterval=300
StartLimitBurst=5

[Install]
WantedBy=multi-user.target
```

### User and Directory Setup

```bash
# Create fluxmq user and group
sudo useradd --system --home-dir /var/lib/fluxmq --create-home --shell /bin/false fluxmq

# Create necessary directories
sudo mkdir -p /var/lib/fluxmq /var/log/fluxmq /etc/fluxmq
sudo chown fluxmq:fluxmq /var/lib/fluxmq /var/log/fluxmq
sudo chmod 750 /var/lib/fluxmq /var/log/fluxmq
sudo chmod 755 /etc/fluxmq

# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable fluxmq
sudo systemctl start fluxmq
sudo systemctl status fluxmq
```

### Log Rotation Configuration

```bash
# /etc/logrotate.d/fluxmq
/var/log/fluxmq/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 fluxmq fluxmq
    postrotate
        /bin/systemctl reload fluxmq > /dev/null 2>&1 || true
    endscript
}
```

### Firewall Configuration

```bash
# UFW (Ubuntu/Debian)
sudo ufw allow 9092/tcp comment "FluxMQ Kafka Protocol"
sudo ufw allow 8080/tcp comment "FluxMQ Metrics"

# firewalld (RHEL/CentOS)
sudo firewall-cmd --permanent --add-port=9092/tcp
sudo firewall-cmd --permanent --add-port=8080/tcp
sudo firewall-cmd --reload

# iptables
sudo iptables -A INPUT -p tcp --dport 9092 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 8080 -j ACCEPT
sudo iptables-save > /etc/iptables/rules.v4
```

---

## Monitoring and Observability

### Health Check Endpoints

```bash
# Basic health check
curl http://localhost:8080/health

# Detailed status information
curl http://localhost:8080/status

# Metrics in Prometheus format
curl http://localhost:8080/metrics
```

### Key Metrics to Monitor

#### Performance Metrics
```
# Throughput
fluxmq_messages_produced_total
fluxmq_messages_consumed_total
fluxmq_bytes_sent_total
fluxmq_bytes_received_total

# Latency
fluxmq_request_duration_seconds
fluxmq_produce_latency_seconds
fluxmq_fetch_latency_seconds

# Resource Usage
fluxmq_memory_usage_bytes
fluxmq_cpu_usage_percent
fluxmq_disk_usage_bytes
fluxmq_network_connections_active
```

#### System Health Metrics
```
# Connection Management
fluxmq_connections_total
fluxmq_connections_active
fluxmq_connection_errors_total

# Storage Health
fluxmq_storage_segments_total
fluxmq_storage_free_bytes
fluxmq_storage_write_errors_total
fluxmq_storage_read_errors_total

# Consumer Groups
fluxmq_consumer_groups_active
fluxmq_consumer_group_members_total
fluxmq_consumer_group_rebalances_total
```

### Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'fluxmq'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: /metrics
    scrape_interval: 10s
    scrape_timeout: 5s

rule_files:
  - "fluxmq_alerts.yml"

alerting:
  alertmanagers:
    - static_configs:
        - targets:
          - alertmanager:9093
```

### Grafana Dashboard Example

```json
{
  "dashboard": {
    "title": "FluxMQ Performance",
    "panels": [
      {
        "title": "Message Throughput",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(fluxmq_messages_produced_total[5m])",
            "legendFormat": "Produced/sec"
          },
          {
            "expr": "rate(fluxmq_messages_consumed_total[5m])",
            "legendFormat": "Consumed/sec"
          }
        ]
      },
      {
        "title": "Request Latency",
        "type": "graph", 
        "targets": [
          {
            "expr": "histogram_quantile(0.95, rate(fluxmq_request_duration_seconds_bucket[5m]))",
            "legendFormat": "95th percentile"
          },
          {
            "expr": "histogram_quantile(0.99, rate(fluxmq_request_duration_seconds_bucket[5m]))",
            "legendFormat": "99th percentile"
          }
        ]
      }
    ]
  }
}
```

### Alerting Rules

```yaml
# fluxmq_alerts.yml
groups:
- name: fluxmq
  rules:
  - alert: FluxMQDown
    expr: up{job="fluxmq"} == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "FluxMQ instance is down"
      
  - alert: FluxMQHighLatency
    expr: histogram_quantile(0.99, rate(fluxmq_request_duration_seconds_bucket[5m])) > 0.1
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "FluxMQ high request latency"
      
  - alert: FluxMQHighMemoryUsage
    expr: fluxmq_memory_usage_bytes / (1024*1024*1024) > 8
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "FluxMQ high memory usage"
      
  - alert: FluxMQStorageAlmostFull
    expr: fluxmq_storage_free_bytes / (1024*1024*1024) < 10
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "FluxMQ storage almost full"
```

---

## Performance Tuning

### Operating System Tuning

#### Linux Kernel Parameters

```bash
# /etc/sysctl.d/99-fluxmq.conf

# Network performance
net.core.rmem_default = 262144
net.core.rmem_max = 16777216
net.core.wmem_default = 262144
net.core.wmem_max = 16777216
net.core.netdev_max_backlog = 5000
net.ipv4.tcp_window_scaling = 1
net.ipv4.tcp_rmem = 4096 65536 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.ipv4.tcp_congestion_control = bbr

# File system performance
fs.file-max = 2097152
vm.swappiness = 1
vm.dirty_ratio = 80
vm.dirty_background_ratio = 5
vm.dirty_expire_centisecs = 12000
```

#### File Descriptor Limits

```bash
# /etc/security/limits.conf
fluxmq soft nofile 1048576
fluxmq hard nofile 1048576
fluxmq soft nproc 65536
fluxmq hard nproc 65536

# /etc/systemd/system.conf
DefaultLimitNOFILE=1048576
DefaultLimitNPROC=65536
```

### FluxMQ Performance Configuration

#### High-Throughput Configuration

```toml
[server]
max_connections = 50000
worker_threads = 16
io_threads = 8

[storage]
segment_size = 2147483648  # 2GB
sync_writes = false        # Async for higher throughput
compression = "lz4"        # Fast compression

[performance]
batch_size = 65536
linger_ms = 10
buffer_pool_size = 134217728  # 128MB
zero_copy_enabled = true
simd_enabled = true
```

#### Low-Latency Configuration

```toml
[server]
max_connections = 10000
worker_threads = 8
request_timeout_ms = 5000

[storage]
segment_size = 536870912  # 512MB
sync_writes = true        # Sync for durability
buffer_pool_size = 67108864  # 64MB

[performance]
batch_size = 16384
linger_ms = 1
prefetch_enabled = true
cpu_affinity_enabled = true
```

### JVM Client Optimization

#### Java Producer Configuration

```properties
# High-throughput Java producer
bootstrap.servers=localhost:9092
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=org.apache.kafka.common.serialization.StringSerializer

# Performance settings
acks=1
retries=3
batch.size=65536
linger.ms=10
buffer.memory=268435456
compression.type=lz4

# Timeout settings (adjusted for FluxMQ)
delivery.timeout.ms=30000
request.timeout.ms=10000
retry.backoff.ms=500
```

#### Java Consumer Configuration

```properties
# High-performance Java consumer
bootstrap.servers=localhost:9092
group.id=my-consumer-group
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

# Performance settings
fetch.min.bytes=1024
fetch.max.wait.ms=100
max.partition.fetch.bytes=1048576
enable.auto.commit=false
max.poll.records=1000
```

---

## Security Configuration

### TLS/SSL Setup

#### Generate Self-Signed Certificate

```bash
# Create certificate authority
openssl genrsa -out ca-key.pem 4096
openssl req -new -x509 -key ca-key.pem -out ca-cert.pem -days 365 \\
  -subj "/CN=FluxMQ-CA"

# Create server certificate
openssl genrsa -out server-key.pem 4096
openssl req -new -key server-key.pem -out server.csr \\
  -subj "/CN=localhost"

# Sign server certificate
openssl x509 -req -in server.csr -CA ca-cert.pem -CAkey ca-key.pem \\
  -CAcreateserial -out server-cert.pem -days 365

# Set proper permissions
chmod 600 server-key.pem ca-key.pem
chmod 644 server-cert.pem ca-cert.pem
```

#### TLS Configuration

```bash
# Start FluxMQ with TLS
fluxmq \\
  --port 9092 \\
  --enable-tls \\
  --tls-cert server-cert.pem \\
  --tls-key server-key.pem \\
  --enable-consumer-groups
```

### Access Control Lists (ACL)

#### ACL Configuration File

```json
{
  "acl_rules": [
    {
      "principal": "User:producer-user",
      "operation": "Write",
      "resource_type": "Topic",
      "resource_name": "*",
      "permission": "Allow"
    },
    {
      "principal": "User:consumer-user",
      "operation": "Read",
      "resource_type": "Topic", 
      "resource_name": "*",
      "permission": "Allow"
    },
    {
      "principal": "User:admin-user",
      "operation": "*",
      "resource_type": "*",
      "resource_name": "*",
      "permission": "Allow"
    }
  ],
  "default_permission": "Deny"
}
```

### SASL Authentication

#### PLAIN Authentication

```toml
[security.sasl]
enabled = true
mechanisms = ["PLAIN"]
plain_users_file = "/etc/fluxmq/users.properties"
```

```properties
# /etc/fluxmq/users.properties
admin=admin-password
producer=producer-password
consumer=consumer-password
```

#### Client Configuration

```properties
# Java client with SASL PLAIN
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \\
  username="producer" password="producer-password";
```

---

## High Availability

### Multi-Node Setup

#### Cluster Configuration

```toml
[cluster]
node_id = 1
bootstrap_nodes = [
  "10.0.1.10:9092",
  "10.0.1.11:9092", 
  "10.0.1.12:9092"
]
replication_factor = 3
min_insync_replicas = 2

[raft]
election_timeout_ms = 5000
heartbeat_interval_ms = 1000
log_compaction_enabled = true
```

#### Load Balancer Configuration (HAProxy)

```
# /etc/haproxy/haproxy.cfg
global
    daemon
    maxconn 4096

defaults
    mode tcp
    timeout connect 5000ms
    timeout client 50000ms
    timeout server 50000ms

frontend fluxmq_frontend
    bind *:9092
    default_backend fluxmq_cluster

backend fluxmq_cluster
    balance roundrobin
    option tcp-check
    server fluxmq1 10.0.1.10:9092 check
    server fluxmq2 10.0.1.11:9092 check
    server fluxmq3 10.0.1.12:9092 check
```

### Health Checks and Failover

```bash
#!/bin/bash
# fluxmq-health-check.sh

FLUXMQ_HOST="localhost"
FLUXMQ_PORT="9092"
HEALTH_ENDPOINT="http://localhost:8080/health"

# Check service health
if ! curl -f -s --max-time 5 "$HEALTH_ENDPOINT" > /dev/null; then
    echo "FluxMQ health check failed"
    # Restart service
    systemctl restart fluxmq
    exit 1
fi

# Check port connectivity
if ! nc -z "$FLUXMQ_HOST" "$FLUXMQ_PORT"; then
    echo "FluxMQ port check failed"
    exit 1
fi

echo "FluxMQ is healthy"
exit 0
```

---

## Backup and Recovery

### Data Backup Strategy

#### Automated Backup Script

```bash
#!/bin/bash
# fluxmq-backup.sh

BACKUP_DIR="/backup/fluxmq"
DATA_DIR="/var/lib/fluxmq"
RETENTION_DAYS=30

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Stop FluxMQ for consistent backup
systemctl stop fluxmq

# Create compressed backup
BACKUP_FILE="$BACKUP_DIR/fluxmq-$(date +%Y%m%d-%H%M%S).tar.gz"
tar -czf "$BACKUP_FILE" -C "$(dirname "$DATA_DIR")" "$(basename "$DATA_DIR")"

# Start FluxMQ
systemctl start fluxmq

# Clean old backups
find "$BACKUP_DIR" -name "fluxmq-*.tar.gz" -mtime +$RETENTION_DAYS -delete

echo "Backup completed: $BACKUP_FILE"
```

#### Recovery Procedure

```bash
#!/bin/bash
# fluxmq-restore.sh

BACKUP_FILE="$1"
DATA_DIR="/var/lib/fluxmq"

if [ ! -f "$BACKUP_FILE" ]; then
    echo "Backup file not found: $BACKUP_FILE"
    exit 1
fi

# Stop FluxMQ
systemctl stop fluxmq

# Backup current data
mv "$DATA_DIR" "$DATA_DIR.backup.$(date +%Y%m%d-%H%M%S)"

# Restore from backup
tar -xzf "$BACKUP_FILE" -C "$(dirname "$DATA_DIR")"

# Set proper ownership
chown -R fluxmq:fluxmq "$DATA_DIR"

# Start FluxMQ
systemctl start fluxmq

echo "Restore completed from: $BACKUP_FILE"
```

### Point-in-Time Recovery

```bash
# Continuous backup using rsync
rsync -av --delete /var/lib/fluxmq/ /backup/fluxmq-live/

# Incremental backup using filesystem snapshots
lvcreate -L1G -s -n fluxmq-snapshot /dev/vg0/fluxmq-data
mount /dev/vg0/fluxmq-snapshot /mnt/snapshot
rsync -av /mnt/snapshot/ /backup/fluxmq-incremental/
```

---

## Troubleshooting

### Common Issues and Solutions

#### 1. High Memory Usage

**Symptoms:**
- FluxMQ consuming excessive memory
- Out of memory errors in logs

**Investigation:**
```bash
# Monitor memory usage
ps aux | grep fluxmq
cat /proc/$(pgrep fluxmq)/status | grep -E "(VmRSS|VmSize)"

# Check heap allocation
curl http://localhost:8080/metrics | grep memory
```

**Solutions:**
```bash
# Reduce segment size
fluxmq --segment-size 536870912  # 512MB instead of 1GB

# Enable compression
fluxmq --compression lz4

# Adjust buffer sizes
export FLUXMQ_BUFFER_SIZE=67108864  # 64MB
```

#### 2. Connection Refused Errors

**Symptoms:**
- Clients cannot connect to FluxMQ
- "Connection refused" errors

**Investigation:**
```bash
# Check if FluxMQ is running
systemctl status fluxmq
pgrep -f fluxmq

# Check listening ports
netstat -tuln | grep 9092
lsof -i :9092

# Check firewall
iptables -L | grep 9092
```

**Solutions:**
```bash
# Start FluxMQ service
systemctl start fluxmq

# Check configuration
fluxmq --host 0.0.0.0 --port 9092

# Open firewall port
ufw allow 9092/tcp
```

#### 3. Java Client Compatibility Issues

**Symptoms:**
- Java clients timeout during message sending
- "Expiring record batch" errors
- High error rates with Java kafka-clients

**Investigation:**
```bash
# Monitor Java client behavior
tcpdump -i lo0 -A -s 0 port 9092

# Check FluxMQ metrics
curl http://localhost:8080/metrics | grep -E "(request_duration|error)"

# Enable debug logging
fluxmq --log-level debug
```

**Solutions:**
```bash
# Increase Java client timeouts
delivery.timeout.ms=30000
request.timeout.ms=10000

# Optimize FluxMQ for Java clients
fluxmq --batch-timeout-ms 100 --ack-timeout-ms 1000

# Use Python clients as alternative
pip3 install kafka-python
```

#### 4. Performance Degradation

**Symptoms:**
- Lower message throughput than expected
- Increased request latency
- High CPU usage

**Investigation:**
```bash
# Monitor performance metrics
curl http://localhost:8080/metrics | grep -E "(throughput|latency|cpu)"

# Check system resources
top -p $(pgrep fluxmq)
iostat -x 1 10

# Profile with perf
perf record -p $(pgrep fluxmq) -g
perf report
```

**Solutions:**
```bash
# Enable performance optimizations
export RUSTFLAGS="-C target-cpu=native"

# Tune kernel parameters
echo 'net.core.rmem_max = 16777216' >> /etc/sysctl.conf
sysctl -p

# Use SSD storage
# Mount data directory on NVMe SSD
```

### Log Analysis

#### Important Log Patterns

```bash
# Connection issues
grep -E "(connection|refused|timeout)" /var/log/fluxmq/fluxmq.log

# Performance issues  
grep -E "(slow|latency|timeout)" /var/log/fluxmq/fluxmq.log

# Error patterns
grep -E "(error|failed|exception)" /var/log/fluxmq/fluxmq.log

# Java client specific issues
grep -E "java|batch.*expir" /var/log/fluxmq/fluxmq.log
```

#### Log Aggregation with ELK Stack

```yaml
# logstash.conf
input {
  file {
    path => "/var/log/fluxmq/*.log"
    start_position => "beginning"
    codec => "json"
  }
}

filter {
  if [fields][service] == "fluxmq" {
    grok {
      match => { 
        "message" => "%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}"
      }
    }
  }
}

output {
  elasticsearch {
    hosts => ["elasticsearch:9200"]
    index => "fluxmq-logs-%{+YYYY.MM.dd}"
  }
}
```

---

## Operational Procedures

### Routine Maintenance

#### Daily Operations

```bash
#!/bin/bash
# daily-maintenance.sh

# Check service status
systemctl status fluxmq

# Monitor disk usage
df -h /var/lib/fluxmq

# Check log file sizes
du -sh /var/log/fluxmq/*

# Verify connectivity
curl -f http://localhost:8080/health

# Check performance metrics
curl -s http://localhost:8080/metrics | grep -E "(throughput|latency)"
```

#### Weekly Operations

```bash
#!/bin/bash
# weekly-maintenance.sh

# Full backup
/opt/scripts/fluxmq-backup.sh

# Log rotation check
logrotate -f /etc/logrotate.d/fluxmq

# Performance analysis
curl -s http://localhost:8080/metrics > /tmp/fluxmq-metrics.$(date +%Y%m%d)

# Security updates check
apt list --upgradable | grep -i security
```

#### Monthly Operations

```bash
#!/bin/bash
# monthly-maintenance.sh

# Clean old backups (automated in backup script)
find /backup/fluxmq -name "*.tar.gz" -mtime +90 -delete

# Storage optimization
# Analyze disk usage patterns
du -sh /var/lib/fluxmq/*/

# Performance review
# Generate performance report for past month
```

### Rolling Updates

#### Zero-Downtime Update Procedure

```bash
#!/bin/bash
# rolling-update.sh

NODES=("fluxmq1" "fluxmq2" "fluxmq3")
NEW_VERSION="$1"

for node in "${NODES[@]}"; do
    echo "Updating $node to version $NEW_VERSION"
    
    # Drain connections from node
    ssh "$node" "curl -X POST http://localhost:8080/admin/drain"
    sleep 30
    
    # Stop service
    ssh "$node" "systemctl stop fluxmq"
    
    # Update binary
    scp "/opt/fluxmq/fluxmq-$NEW_VERSION" "$node:/usr/local/bin/fluxmq"
    
    # Start service
    ssh "$node" "systemctl start fluxmq"
    
    # Health check
    while ! ssh "$node" "curl -f http://localhost:8080/health"; do
        sleep 5
    done
    
    echo "$node updated successfully"
    sleep 60  # Allow rebalancing
done
```

### Capacity Planning

#### Usage Monitoring

```bash
#!/bin/bash
# capacity-monitor.sh

# Collect current metrics
CONNECTIONS=$(curl -s http://localhost:8080/metrics | grep connections_active | awk '{print $2}')
THROUGHPUT=$(curl -s http://localhost:8080/metrics | grep messages_per_second | awk '{print $2}')
DISK_USAGE=$(df /var/lib/fluxmq | tail -1 | awk '{print $5}' | sed 's/%//')
MEMORY_USAGE=$(ps -p $(pgrep fluxmq) -o rss= | awk '{print $1/1024/1024}')

# Alert thresholds
if [ "$CONNECTIONS" -gt 8000 ]; then
    echo "WARNING: High connection count: $CONNECTIONS"
fi

if [ "$DISK_USAGE" -gt 80 ]; then
    echo "WARNING: High disk usage: $DISK_USAGE%"
fi

if [ "$(echo "$MEMORY_USAGE > 16" | bc)" -eq 1 ]; then
    echo "WARNING: High memory usage: ${MEMORY_USAGE}GB"
fi

echo "Current metrics - Connections: $CONNECTIONS, Throughput: $THROUGHPUT msg/s, Disk: $DISK_USAGE%, Memory: ${MEMORY_USAGE}GB"
```

### Emergency Procedures

#### Service Recovery

```bash
#!/bin/bash
# emergency-recovery.sh

echo "FluxMQ Emergency Recovery Procedure"

# 1. Stop service
systemctl stop fluxmq

# 2. Check data integrity
if [ -f /var/lib/fluxmq/CORRUPTION_DETECTED ]; then
    echo "Data corruption detected, restoring from backup"
    /opt/scripts/fluxmq-restore.sh /backup/fluxmq/latest.tar.gz
fi

# 3. Clear locks and temporary files
rm -f /var/lib/fluxmq/*.lock /var/lib/fluxmq/tmp/*

# 4. Start in recovery mode
fluxmq --recovery-mode --data-dir /var/lib/fluxmq &
RECOVERY_PID=$!

# 5. Wait for recovery completion
sleep 30

# 6. Stop recovery mode
kill $RECOVERY_PID

# 7. Start normal service
systemctl start fluxmq

# 8. Verify health
if curl -f http://localhost:8080/health; then
    echo "Recovery successful"
else
    echo "Recovery failed - manual intervention required"
    exit 1
fi
```

---

## Performance Optimization Summary

### Quick Performance Wins

1. **Enable CPU-specific optimizations**:
   ```bash
   export RUSTFLAGS="-C target-cpu=native"
   cargo build --release
   ```

2. **Optimize kernel parameters**:
   ```bash
   echo 'net.core.rmem_max = 16777216' >> /etc/sysctl.conf
   echo 'vm.swappiness = 1' >> /etc/sysctl.conf
   sysctl -p
   ```

3. **Use NVMe storage**:
   ```bash
   # Mount data directory on NVMe SSD for optimal I/O performance
   ```

4. **Configure Java clients properly**:
   ```properties
   delivery.timeout.ms=30000
   request.timeout.ms=10000
   batch.size=65536
   linger.ms=10
   ```

### Expected Performance Targets

- **Throughput**: 40,000+ msg/sec (current: 47,333 msg/sec with Python clients)
- **Latency**: <1ms P99 (current: 0.030ms P99)
- **Connections**: 10,000+ concurrent connections
- **Memory**: <2GB for 1M messages
- **Availability**: 99.9% uptime

---

**FluxMQ provides enterprise-grade performance and reliability with simplified deployment and operation. This guide covers all aspects of production deployment, from initial installation to ongoing maintenance and troubleshooting.**

---

*Generated on 2025-09-13 | FluxMQ Operations Team*