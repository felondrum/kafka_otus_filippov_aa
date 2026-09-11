# 8. Monitoring & Observability

## 8.1. Метрики Prometheus

### 8.1.1. Kafka метрики

| Метрика | Тип | Описание |
|---------|-----|----------|
| `kafka_server_brokertopicmetrics_messagesinpersec` | Gauge | Throughput (messages/sec) для каждого топика |
| `kafka_server_brokertopicmetrics_bytesinpersec` | Gauge | Incoming bytes/sec |
| `kafka_server_brokertopicmetrics_bytesoutpersec` | Gauge | Outgoing bytes/sec |
| `kafka_consumer_group_lag` | Gauge | Lag потребителей по группе и топику |
| `kafka_server_replicamanager_underreplicatedpartitions` | Gauge | Количество unrelicated партиций |
| `kafka_controller_kafkacontroller_controllercount` | Gauge | Количество controller-ов |
| `kafka_network_server_requests_per_sec` | Gauge | Запросы к брокеру |
| `kafka_network_controllerchannelmanager_failedrequestspersec` | Gauge | Ошибки контроллера |
| `kafka_log_logstartoffset` | Gauge | Начальный offset лога |
| `kafka_log_logendoffset` | Gauge | Конечный offset лога |

### 8.1.2. JVM метрики

| Метрика | Тип | Описание |
|---------|-----|----------|
| `jvm_memory_bytes_used{area="heap"}` | Gauge | Использованная heap-память |
| `jvm_memory_bytes_max{area="heap"}` | Gauge | Максимальная heap-память |
| `jvm_gc_pause_seconds` | Summary | Паузы GC |
| `jvm_threads_current` | Gauge | Текущее количество потоков |
| `jvm_threads_peak` | Gauge | Пиковое количество потоков |
| `jvm_classes_loaded` | Gauge | Загруженные классы |
| `process_cpu_usage` | Gauge | Использование CPU процессом |
| `process_memory_usage_bytes` | Gauge | Использование памяти процессом |

### 8.1.3. Метрики приложения

| Метрика | Тип | Описание |
|---------|-----|----------|
| `app_calls_processed_total` | Counter | Обработанные звонки (по статусу) |
| `app_calls_processing_duration_seconds` | Histogram | Время обработки звонка (p50, p95, p99) |
| `app_fraud_alerts_total` | Counter | Обнаруженные фрод-алерты |
| `app_transcription_duration_seconds` | Histogram | Время транскрибации |
| `app_dlq_messages_total` | Counter | Сообщения в DLQ (по причине) |
| `app_db_queries_total` | Counter | Запросы к PostgreSQL (по типу) |
| `app_db_query_duration_seconds` | Histogram | Время выполнения запросов |
| `app_kafka_consumer_lag` | Gauge | Lag потребителей Spring Kafka |
| `app_kafka_produce_latency_seconds` | Histogram | Латентность продюсера |

### 8.1.4. Конфигурация Prometheus

```yaml
# monitoring/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092']
    metrics_path: '/metrics'

  - job_name: 'microservices'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['call-processor:8081', 'fraud-detector:8082', 'transcription-analyzer:8083', 'reporting-nps:8084']

  - job_name: 'postgres'
    static_configs:
      - targets: ['postgres-exporter:9187']

  - job_name: 'schema-registry'
    static_configs:
      - targets: ['schema-registry:8081']
```

## 8.2. Grafana дашборды

### 8.2.1. Кластер Kafka

```
┌──────────────────────────────────────────────────────────────┐
│                    Kafka Cluster Dashboard                    │
│                                                               │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐         │
│  │ Throughput   │ │ Partitions   │ │ Replication  │         │
│  │ (msg/sec)    │ │ Overview     │ │ Health       │         │
│  │              │ │              │ │              │         │
│  │  ┌────────┐  │ │  6/6 OK     │ │  0 under-   │         │
│  │  │░░░░░░░░│  │ │  3 brokers  │ │  replicated  │         │
│  │  │░░░░░░░░│  │ │  3 controllers│ │  partitions │         │
│  │  └────────┘  │ │              │ │              │         │
│  └──────────────┘ └──────────────┘ └──────────────┘         │
│                                                               │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  Messages In / Out per Topic (last 1 hour)               │ │
│  │                                                          │ │
│  │  calls.completed    ████████████████░░░░░░░░░░░░░░░░░░░░ │ │
│  │  calls.metadata   ████████████████████████░░░░░░░░░░░░░░ │ │
│  │  fraud-alerts    ████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ │ │
│  │  transcription   ████████████████████████████████░░░░░░ │ │
│  └──────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  Broker Disk Usage (GB)                                  │ │
│  │                                                          │ │
│  │  kafka-1  ████████████████████████░░░░░░░░░░░░░░░░░░░░  │ │
│  │  kafka-2  ████████████████████████░░░░░░░░░░░░░░░░░░░░  │ │
│  │  kafka-3  ████████████████████████░░░░░░░░░░░░░░░░░░░░  │ │
│  └──────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

**Метрики для дашборда:**

| Виджет | Метрика | Граф |
|--------|---------|------|
| Messages In/Out | `kafka_server_brokertopicmetrics_messagesinpersec` | Time series |
| Bytes In/Out | `kafka_server_brokertopicmetrics_bytesinpersec` | Time series |
| Under-replicated | `kafka_server_replicamanager_underreplicatedpartitions` | Gauge |
| Leader Count | `kafka_server_replicamanager_leadercount` | Gauge |
| ISR Shrink Rate | `kafka_server_replicamanager_isrshrinkratepersec` | Time series |

### 8.2.2. Состояние звонков

```
┌──────────────────────────────────────────────────────────────┐
│                  Calls Status Dashboard                       │
│                                                               │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐         │
│  │ Total Calls  │ │ Success Rate │ │ Avg Duration │         │
│  │              │ │              │ │ (sec)        │         │
│  │  1,247       │ │  99.8%       │ │  185         │         │
│  │  +12.5%      │ │  -0.1%       │ │  +3.2%       │         │
│  └──────────────┘ └──────────────┘ └──────────────┘         │
│                                                               │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  Status Distribution (Live)                              │ │
│  │                                                          │ │
│  │  COMPLETED    ████████████████████████████████  87%      │ │
│  │  TRANSCRIBING ████████░░░░░░░░░░░░░░░░░░░░░░░░  8%       │ │
│  │  SUMMARIZING  ████░░░░░░░░░░░░░░░░░░░░░░░░░░░░  3%       │ │
│  │  PENDING      ██░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  2%       │ │
│  └──────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  NPS Score Distribution                                  │ │
│  │                                                          │ │
│  │  10  ████████████████████████████████████████  28%       │ │
│  │  9   ██████████████████████████████████  22%             │ │
│  │  8   ██████████████████████████  18%                     │ │
│  │  7   ██████████████████  12%                            │ │
│  │  6   ██████████████  9%                                  │ │
│  │  5   ██████████  6%                                      │ │
│  │  4   ██████  3%                                          │ │
│  │  3   ███  1%                                             │ │
│  └──────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  Fraud Alerts (last 24h)                                 │ │
│  │                                                          │ │
│  │  Total: 23  |  High Risk: 8  |  Medium: 12  |  Low: 3   │ │
│  └──────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

**Метрики для дашборда:**

| Виджет | Метрика | Граф |
|--------|---------|------|
| Total Calls | `app_calls_processed_total` | Counter (rate) |
| Success Rate | `app_calls_processed_total{status="success"}` / total | Percentage |
| Avg Duration | `app_calls_processing_duration_seconds_avg` | Gauge |
| Status Distribution | `app_calls_processed_total{status}` | Pie chart |
| NPS Distribution | `app_calls_processed_total{nps_bucket}` | Histogram |
| Fraud Alerts | `app_fraud_alerts_total{type}` | Counter (rate) |

### 8.2.3. Lag потребителей

```
┌──────────────────────────────────────────────────────────────┐
│                  Consumer Lag Dashboard                       │
│                                                               │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐         │
│  │ Max Lag      │ │ Avg Lag      │ │ Groups       │         │
│  │ (any topic)  │ (all groups)   │ (active)       │         │
│  │              │              │ │              │         │
│  │  42          │ │  12          │ │  4 / 4       │         │
│  └──────────────┘ └──────────────┘ └──────────────┘         │
│                                                               │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  Lag by Consumer Group & Topic                           │ │
│  │                                                          │ │
│  │  fraud-detector-group                                    │ │
│  │    calls.completed     ████████████████████████░░░░░░░░░  42 │ │
│  │                                                          │ │
│  │  transcription-analyzer-group                            │ │
│  │    calls.completed     ████████░░░░░░░░░░░░░░░░░░░░░░░░░  18 │ │
│  │    customers.profile   ████████░░░░░░░░░░░░░░░░░░░░░░░░░  15 │ │
│  │                                                          │ │
│  │  reporting-nps-group                                     │ │
│  │    calls.metadata      ████████░░░░░░░░░░░░░░░░░░░░░░░░░  12 │ │
│  │    transcription.      █████░░░░░░░░░░░░░░░░░░░░░░░░░░░░  8  │ │
│  │    enriched                                            │ │
│  │    calls.fraud-        ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0  │ │
│  │    alerts                                              │ │
│  └──────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  Lag Trend (last 24 hours)                               │ │
│  │                                                          │ │
│  │  100 ┤     ╱╲    ╱╲    ╱╲    ╱╲    ╱╲    ╱╲    ╱╲       │ │
│  │   50 ┤   ╱╱  ╲╱  ╲╱  ╲╱  ╲╱  ╲╱  ╲╱  ╲╱  ╲╱  ╲╱        │ │
│  │    0 ┼──╱──╱──╱──╱──╱──╱──╱──╱──╱──╱──╱──╱──╱──╱──      │ │
│  └──────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

**Метрики для дашборда:**

| Виджет | Метрика | Граф |
|--------|---------|------|
| Max Lag | `app_kafka_consumer_lag` | Gauge (max) |
| Avg Lag | `app_kafka_consumer_lag` | Gauge (avg) |
| Lag by Group | `app_kafka_consumer_lag{group}` | Time series |
| Lag Trend | `app_kafka_consumer_lag` | Time series (24h) |

## 8.3. Логирование

### 8.3.1. Структурированное логирование (JSON)

```yaml
# logback-spring.xml
<configuration>
  <appender name="JSON_CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
    <encoder class="ch.qos.logback.classic.encoder.JsonEncoder"/>
  </appender>

  <appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>logs/application.log</file>
    <rollingPolicy class="ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy">
      <fileNamePattern>logs/application.%d{yyyy-MM-dd}.%i.log</fileNamePattern>
      <maxFileSize>100MB</maxFileSize>
      <maxHistory>30</maxHistory>
    </rollingPolicy>
    <encoder class="ch.qos.logback.classic.encoder.JsonEncoder"/>
  </appender>

  <root level="INFO">
    <appender-ref ref="JSON_CONSOLE"/>
    <appender-ref ref="FILE"/>
  </root>
</configuration>
```

### 8.3.2. Поля логов

| Поле | Тип | Описание |
|------|-----|----------|
| `timestamp` | ISO-8601 | Время события |
| `level` | string | Уровень логирования (INFO, WARN, ERROR) |
| `logger` | string | Имя логгера |
| `message` | string | Сообщение |
| `correlationId` | string | Correlation ID для tracing |
| `callId` | string | ID звонка (если применимо) |
| `phone` | string | Номер телефона (если применимо) |
| `serviceName` | string | Имя сервиса |
| `containerId` | string | ID контейнера Docker |
| `thread` | string | Имя потока |
| `exception` | string | Stack trace (при ошибках) |

### 8.3.3. Примеры логов

```json
{
  "timestamp": "2026-09-09T10:15:30.123Z",
  "level": "INFO",
  "logger": "ru.otus.callplatform.callprocessor.CallController",
  "message": "Call received",
  "correlationId": "abc-123-def-456",
  "callId": "550e8400-e29b-41d4-a716-446655440000",
  "serviceName": "call-processor",
  "containerId": "a1b2c3d4e5f6",
  "thread": "http-nio-8081-exec-1"
}
```

```json
{
  "timestamp": "2026-09-09T10:15:30.456Z",
  "level": "WARN",
  "logger": "ru.otus.callplatform.fraud.FraudDetector",
  "message": "Fraud alert triggered",
  "correlationId": "abc-123-def-456",
  "callId": "550e8400-e29b-41d4-a716-446655440000",
  "phone": "+79001234567",
  "alertType": "HIGH_FREQUENCY",
  "score": 0.85,
  "serviceName": "fraud-detector",
  "containerId": "b2c3d4e5f6a1",
  "thread": "kafka-streams-fraud-detector-1"
}
```

```json
{
  "timestamp": "2026-09-09T10:15:31.789Z",
  "level": "ERROR",
  "logger": "ru.otus.callplatform.common.ErrorHandler",
  "message": "Failed to process message, sending to DLQ",
  "correlationId": "xyz-789-ghi-012",
  "callId": "660e8400-e29b-41d4-a716-446655440001",
  "exception": "org.apache.kafka.common.errors.SerializationException: Error deserializing Avro message",
  "serviceName": "transcription-analyzer",
  "containerId": "c3d4e5f6a1b2",
  "thread": "kafka-consumer-transcription-analyzer-1"
}
```

## 8.4. Tracing (Correlation ID)

### 8.4.1. Flow tracing

```
1. Caller → POST /api/calls {correlationId: "abc-123"}
   │  MDC.put("correlationId", "abc-123")
   │
2. call-processor
   │  MDC: correlationId=abc-123
   │  Log: "Call received"
   │  Kafka headers: correlation-id=abc-123
   │
3. fraud-detector
   │  Read header: correlation-id=abc-123
   │  MDC: correlationId=abc-123
   │  Log: "Fraud alert triggered"
   │
4. transcription-analyzer
   │  Read header: correlation-id=abc-123
   │  MDC: correlationId=abc-123
   │  Log: "Transcription completed"
   │
5. reporting-nps
   │  Read header: correlation-id=abc-123
   │  MDC: correlationId=abc-123
   │  Log: "Report updated"
```

### 8.4.2. Реализация Correlation ID Filter

```java
@Component
public class CorrelationIdFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                     HttpServletResponse response,
                                     FilterChain filterChain) {
        String correlationId = request.getHeader("X-Correlation-ID");
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }

        MDC.put("correlationId", correlationId);
        response.setHeader("X-Correlation-ID", correlationId);

        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove("correlationId");
        }
    }
}
```

## 8.5. Алерты Grafana

| Алерт | Условие | Severity | Описание |
|-------|---------|----------|----------|
| High Consumer Lag | `app_kafka_consumer_lag > 100` for 5 min | WARNING | Lag потребителей превышает 100 |
| Critical Consumer Lag | `app_kafka_consumer_lag > 1000` for 1 min | CRITICAL | Lag потребителей превышает 1000 |
| DLQ Rate High | `rate(app_dlq_messages_total[5m]) > 10` | WARNING | Высокая скорость попадания в DLQ |
| Under-replicated Partitions | `kafka_server_replicamanager_underreplicatedpartitions > 0` | CRITICAL | Нереплицированные партиции |
| High Error Rate | `rate(app_calls_processed_total{status="error"}[5m]) > 0.1` | WARNING | Ошибка > 10% |
| PostgreSQL Connection Error | `app_db_queries_total{status="error"} > 0` | CRITICAL | Ошибки подключения к БД |
| Service Down | `up == 0` for 1 min | CRITICAL | Сервис недоступен |
| High Memory Usage | `jvm_memory_bytes_used{area="heap"} / jvm_memory_bytes_max{area="heap"} > 0.9` | WARNING | Использование памяти > 90% |
| High CPU Usage | `process_cpu_usage > 0.9` for 5 min | WARNING | Использование CPU > 90% |

## 8.6. Monitoring Summary

| Аспект | Инструмент | Порт |
|--------|-----------|------|
| Сбор метрик | Prometheus | 9090 |
| Визуализация | Grafana | 3000 |
| Kafka UI | Kafdrop | 9000 |
| Логирование | JSON (stdout + file) | - |
| Tracing | Correlation ID (MDC) | - |
| Алертинг | Grafana Alerts | - |
