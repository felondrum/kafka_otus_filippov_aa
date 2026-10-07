# Kafka Monitoring - Полное руководство

## ✅ Всё работает!

### Prometheus Targets (13 total, all UP):
- **kafka-1:5556** - JVM metrics (JMX exporter)
- **kafka-2:5556** - JVM metrics (JMX exporter)
- **kafka-3:5556** - JVM metrics (JMX exporter)
- **kafka-exporter:9308** - Topics, partitions, consumer lag
- **call-processor:8081** - Spring Kafka producer metrics
- **fraud-detector:8082** - Spring Boot metrics
- **transcription-analyzer:8083** - Spring Kafka consumer metrics
- **reporting-nps:8084** - Spring Kafka consumer + HikariCP
- **postgresql:9187** - PostgreSQL metrics
- **prometheus:9090** - Prometheus self-monitoring
- **grafana:3000** - Infrastructure

### Grafana Dashboards (8 total):

1. **Kafka Cluster Overview** (`kafka-overview.json`)
   - JVM memory, CPU, GC, threads (3 brokers)
   - Topics count, partitions, consumer groups
   - Partition offsets, consumer lag
   - Consumer group members

2. **Call Processor** (`call-processor.json`)
   - JVM memory, CPU, threads, GC
   - Kafka producer send rate & latency
   - HTTP request rate & latency
   - HTTP error rate

3. **Fraud Detector** (`fraud-detector.json`)
   - JVM memory, CPU, threads, GC
   - HTTP request rate & latency
   - Process memory (RSS, Virtual)

4. **Transcription Analyzer** (`transcription-analyzer.json`)
   - JVM memory, CPU, threads, GC
   - Kafka consumer fetch rate & latency
   - HTTP request rate & latency

5. **Reporting NPS** (`reporting-nps.json`)
   - JVM memory, CPU, threads, GC
   - Kafka consumer fetch rate
   - Database connection pool (HikariCP)
   - HTTP request rate & latency

6. **System Overview** (`system-overview.json`)
   - Service health status
   - Kafka message throughput (Spring Kafka)
   - Database write rates
   - Kafka consumer/producer latency
   - HTTP request latency & rate
   - PostgreSQL connections

7. **PostgreSQL Overview** (`postgres-overview.json`)
   - Database connections, queries
   - Table statistics
   - Index usage

8. **Kafka Connect JDBC Sink** (`kafka-connect-jdbc-sink.json`)
   - Connector status
   - Task status
   - DLQ events

## Kafka Metrics Available

### JVM Metrics (from JMX exporter on port 5556):
```promql
jvm_memory_bytes_used{job="kafka", area="heap"}
jvm_memory_bytes_committed{job="kafka", area="heap"}
process_cpu_seconds_total{job="kafka"}
jvm_gc_collection_seconds_count{job="kafka"}
jvm_threads_live_threads{job="kafka"}
```

### Topics & Partitions (from kafka-exporter on port 9308):
```promql
kafka_topic_partitions{topic="calls.completed"}
kafka_topic_partition_current_offset{topic="calls.completed", partition="0"}
kafka_consumergroup_lag{consumergroup="enrichment-processor"}
kafka_consumergroup_current_offset{consumergroup="enrichment-processor"}
kafka_consumergroup_members{consumergroup="enrichment-processor"}
```

### Spring Kafka (from services):
```promql
# Producer (call-processor)
spring_kafka_template_seconds_count{job="call-processor"}
spring_kafka_template_seconds_sum{job="call-processor"}

# Consumer (transcription-analyzer)
spring_kafka_listener_seconds_count{job="transcription-analyzer"}
spring_kafka_listener_seconds_sum{job="transcription-analyzer"}

# Consumer (reporting-nps)
spring_kafka_listener_seconds_count{job="reporting-nps"}
spring_kafka_listener_seconds_sum{job="reporting-nps"}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Prometheus                               │
│                   (13 targets, all UP)                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  Kafka Brokers│  │ Kafka Exporter   │  │  Services        │
│  (JMX:5556)  │  │  (9308)          │  │  (Actuator)      │
│              │  │                  │  │                  │
│ • JVM memory │  │ • Topics         │  │ • call-processor │
│ • CPU        │  │ • Partitions     │  │ • fraud-detector │
│ • GC         │  │ • Consumer lag   │  │ • transcription  │
│ • Threads    │  │ • Offsets        │  │ • reporting-nps  │
└──────────────┘  └──────────────────┘  └──────────────────┘
      │                   │                      │
      │                   │                      │
      ▼                   ▼                      ▼
┌─────────────────────────────────────────────────────────────┐
│                     Grafana                                  │
│                   (8 dashboards)                             │
│  • Kafka Cluster Overview                                    │
│  • Call Processor                                            │
│  • Fraud Detector                                            │
│  • Transcription Analyzer                                    │
│  • Reporting NPS                                             │
│  • System Overview                                           │
│  • PostgreSQL Overview                                       │
│  • Kafka Connect JDBC Sink                                   │
└─────────────────────────────────────────────────────────────┘
```

## MONITORING Listener Setup

Kafka brokers configured with additional MONITORING listener on port 9099:
```
KAFKA_LISTENERS: CONTROLLER://0.0.0.0:9094,INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9093,MONITORING://0.0.0.0:9099
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-1:9092,EXTERNAL://localhost:9092,MONITORING://kafka-1:9099
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT,MONITORING:PLAINTEXT
```

Kafka Exporter connects to MONITORING listener without SASL authentication:
```yaml
kafka-exporter:
  command: ["--kafka.server=kafka-1:9099","--kafka.server=kafka-2:9099","--kafka.server=kafka-3:9099"]
```

## Known Issues

- **R-108**: JMX exporter javaagent was not connected initially - FIXED
- **R-110**: KafkaStreamsMetrics doesn't exist in Kafka Streams 3.6.1 - No custom Kafka Streams metrics available
- **Kafka Connect metrics**: Not available through standard endpoints, only HTTP metrics from Kafka Connect API

## How to View Topics & Partitions

### Option 1: Grafana Dashboard
Open `Kafka Cluster Overview` dashboard in Grafana (http://localhost:3000)

### Option 2: Kafdrop
http://localhost:9000 - Shows all topics, partitions, consumer groups, and lag

### Option 3: CLI
```bash
# List topics
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list

# Describe consumer groups
docker exec kafka-1 kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe
```
