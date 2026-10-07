# Платформа обработки звонков банковского колл-центра

## Техническая презентация

**Технологический стек:** Java 21, Spring Boot 3.2.3, Apache Kafka 7.6.1 (KRaft), PostgreSQL 15, Docker Compose.

**Пакет кода:** `com.example.*`

**Дата:** 2026-10-01

---

## 1. Обзор системы

### Что это

Событийно-ориентированная платформа реального времени для обработки звонков банковского колл-центра. Система принимает события о завершённых звонках, анализирует их на предмет мошеннических паттернов, генерирует суммаризацию диалогов и предоставляет аналитические дашборды.

### Бизнес-цели

- **Мгновенная реакция** на негативные сценарии (эскалация NPS, фрод-паттерны).
- **Автоматическая классификация** проблематики (потери карт, кредиты, жалобы).
- **Выявление мошеннических паттернов** (частые звонки, аномальная длительность, эскалация).
- **Суммаризация диалогов** для архивации и быстрого поиска.
- **Менеджерские дашборды** с актуальной статистикой (NPS, загрузка агентов, тональность).
- **Полная история** звонков с привязкой метаинформации к расшифровкам для аудита.

### Архитектурный стиль

**Event-Driven Architecture (EDA):** все взаимодействия между сервисами происходят через события в Apache Kafka. Сервисы не знают друг о друге (choreography pattern). События — источник истины, состояние сервиса восстанавливается из логов.

**CQRS (Command Query Responsibility Segregation):** команды (создание звонков, алерты) пишутся в Kafka, запросы (отчёты, статусы) читаются из PostgreSQL. Разделение путей записи и чтения обеспечивает независимую масштабируемость.

---

## 2. Архитектура (Container Level)

### 5 микросервисов

| Сервис | Порт | Роль |
|--------|------|------|
| **call-processor** | 8081 | REST API entry point, Kafka Producer, Topic Manager |
| **fraud-detector** | 8082 | Kafka Streams, детекция фрода в реальном времени |
| **transcription-analyzer** | 8083 | Synthetic transcription, summary generation, Kafka-first write + DLQ |
| **reporting-nps** | 8084 | Kafka consumers, REST API для отчётов, CQRS read side |
| **load-simulator** | 8087 | Генератор синтетической нагрузки, 4 fraud-сценария |

### Инфраструктура

| Компонент | Образ | Роль |
|-----------|-------|------|
| **Kafka Cluster** | 3× `cp-kafka:7.6.1` (KRaft, no ZooKeeper) | Event backbone |
| **PostgreSQL** | `postgres:15-alpine` | Analytics DB |
| **Schema Registry** | `cp-schema-registry:7.6.1` | JSON-схемы событий |
| **Kafka Connect** | custom build | JDBC Sink connector → PostgreSQL |
| **ksqlDB** | `cp-ksqldb-server:7.6.1` | SQL-агрегация потоков |
| **Prometheus** | `prom/prometheus:v2.51.0` | Сбор метрик |
| **Grafana** | `grafana/grafana:10.2.0` | 8 дашбордов, алерты |
| **Kafdrop** | `obsidiandynamics/kafdrop:4.0.1` | Kafka UI |
| **Kafka Exporter** | `danielqsj/kafka-exporter:v1.8.0` | Topic/partition metrics |
| **Postgres Exporter** | `prometheuscommunity/postgres-exporter:v0.15.0` | PostgreSQL metrics |

### Сетевая модель

- **Docker Network:** `call-platform-net` (bridge)
- **Profiles:** `full` (все сервисы), `core` (без мониторинга), `monitoring` (только мониторинг), `load-test` (для нагрузочного тестирования)
- **Kafka:** 3 брокера в KRaft mode (no ZooKeeper), SASL/PLAIN на EXTERNAL listener, PLAINTEXT на INTERNAL listener

---

## 3. Kafka Topics

### 10 топиков

| Топик | Тип | Ключ | Формат | Partition | Replication | Producer |
|-------|-----|------|--------|-----------|-------------|----------|
| `calls.completed` | Stream | callId | JSON | 6 | 3 | call-processor |
| `calls.metadata` | Compacted | callId | JSON | 6 | 3 | call-processor |
| `calls.fraud-alerts` | Stream | phone | JSON | 6 | 3 | fraud-detector |
| `transcription.raw` | Stream | callId | JSON | 6 | 3 | transcription-analyzer |
| `transcription.summary` | Stream | callId | JSON | 6 | 3 | transcription-analyzer |
| `transcription.enriched` | Stream | callId | Confluent JSON | 6 | 3 | transcription-analyzer |
| `transcription.enriched.dlq` | Stream | callId | JSON | 3 | 3 | transcription-analyzer |
| `calls.dlq` | Stream | callId | JSON | 6 | 3 | all services |
| `customers.profile` | Compacted | phone | JSON | 6 | 3 | bootstrap script |
| `calls.completed.agg` | Stream | agentId | JSON | 6 | 1 | ksqlDB |
| `calls.fraud-alerts.agg` | Stream | phone | JSON | 6 | 1 | ksqlDB |

### Конфигурация создания топиков

Скрипт `infrastructure/kafka/init-topics.sh` создаёт 10 топиков при запуске:
- Stream topics: 6 partitions, replication-factor=3
- Compacted topics: cleanup.policy=compact, 6 partitions, replication-factor=3
- DLQ topics: 3 partitions, replication-factor=3, retention=30 days

**Topic Manager** в call-processor (`TopicManager.java`) дополнительно создаёт 3 топика при старте сервиса (replication-factor=1 для локальной разработки).

---

## 4. Data Flow (End-to-End)

```
[External Caller / Load Simulator]
              │
              │  POST /api/calls {callId, phone, duration, agentId, npsScore}
              ▼
     ┌─────────────────┐
     │  call-processor   │  ← CallController.java + CallEventService.java
     │  (validate +     │  ← Bean Validation (JSR-380)
     │   produce)        │  ← @Retryable (3 attempts, 1s→2s→4s)
     └────────┬────────┘
              │
              ├──► calls.completed ──┐
              │                      │
              ├──► calls.metadata ───┼──► reporting-nps ──► PostgreSQL (call_metadata)
              │                      │
              ▼                      │
     ┌─────────────────┐             │
     │ fraud-detector   │             │
     │ (Kafka Streams)  │             │
     └────────┬────────┘             │
              │                       │
              ├──► calls.fraud-alerts ─┼──► reporting-nps ──► PostgreSQL (fraud_stats)
              │                       │
              ▼                       │
     ┌─────────────────┐             │
     │transcription-   │             │
     │ analyzer         │             │
     │ (consumer)       │             │
     └────────┬────────┘             │
              │                       │
              ├──► transcription.raw ──┐
              ├──► transcription.summary ┐
              └──► transcription.enriched ─┼──► Kafka Connect (JDBC Sink)
                                           │     └──► PostgreSQL (call_transcriptions)
                                           │     └──► reporting-nps ──► PostgreSQL
                                           │
                                           └──► DLQ (transcription.enriched.dlq)
```

---

## 5. Детали реализации: call-processor

### Роль
REST API entry point. Принимает события о завершённых звонках, валидирует и публикует в Kafka.

### Ключевые классы

| Класс | Роль |
|-------|------|
| `CallController.java` | REST endpoint: `POST /api/calls`, `GET /api/health` |
| `CallEventService.java` | Core logic: валидация, `@Retryable`, production to Kafka |
| `TopicManager.java` | `@PostConstruct initTopics()` — создаёт топики через AdminClient |
| `CallEventRequest.java` | DTO: callId, phone, duration, agentId, npsScore |
| `KafkaConfig.java` | Producer config: idempotent mode, acks=all |

### Валидация (CallEventRequest.java)

| Поле | Аннотация | Правило |
|------|-----------|---------|
| `callId` | `@NotBlank` + `@Pattern` | UUID format: `^[0-9a-f]{8}-...$` |
| `phone` | `@NotBlank` + `@Pattern` | E.164: `^\\+?[1-9]\\d{1,14}$` |
| `duration` | `@Min(1)` | >= 1 секунда |
| `agentId` | `@NotBlank` | Не пустой |
| `npsScore` | `@Min(0)` + `@Max(10)` | 0–10 |

### Механизм надёжности

- **Idempotent Producer:** `enable.idempotence=true`, `acks=all` — гарантировка без дублирования.
- **Spring Retry:** `@Retryable(retryFor = RuntimeException.class, maxAttempts=3, backoff=@Backoff(delay=1000ms, multiplier=2))` — 1s → 2s → 4s.
- **DLQ:** при исчерпании retry сообщение попадает в `calls.dlq`.
- **Topic Manager:** при старте проверяет наличие топиков через `AdminClient.listTopics()` и создаёт 3 топика (calls.completed, calls.metadata, calls.dlq) с replication-factor=1.

### Сериализация

`KafkaTemplate<String, String>` + `StringSerializer`/`StringDeserializer` + `ObjectMapper.writeValueAsString()`. JSON, не Avro.

---

## 6. Детали реализации: fraud-detector

### Роль
Реалтайм-детекция мошеннических паттернов через Kafka Streams. Потребляет `calls.completed`, генерирует алерты в `calls.fraud-alerts`.

### Ключевые классы

| Класс | Роль |
|-------|------|
| `StreamsTopologyConfig.java` | Строит Kafka Streams topology, конфигурирует 3 processor pipeline |
| `FrequentCallsProcessor.java` | DSL Hopping Window (60s size, 10s advance), RocksDB, > 5 calls/window |
| `AnomalousDurationProcessor.java` | Filter: duration > 300 сек |
| `NpsEscalationProcessor.java` | Processor API + KeyValueStore, 3x NPS < 2 за 24ч |
| `PhoneNormalizer.java` | Нормализация phone в E.164 формат |
| `FraudDetectorProperties.java` | Конфигурируемые параметры (window, thresholds) |
| `KafkaStreamsLifecycle.java` | `@PostConstruct` start, `@PreDestroy` graceful shutdown |

### 3 паттерна фрода

| Паттерн | Класс | Механизм | Threshold | Severity |
|---------|-------|----------|-----------|----------|
| **FREQUENT_CALLS** | FrequentCallsProcessor | DSL Hopping Window (RocksDB) | > 5 calls за 60s (advance 10s) | MEDIUM |
| **ANOMALOUS_DURATION** | AnomalousDurationProcessor | Simple filter | duration > 300 сек | LOW |
| **NPS_ESCALATION** | NpsEscalationProcessor | Processor API + KeyValueStore | 3x NPS < 2 за 24ч | HIGH |

### Конфигурация Kafka Streams

```properties
processing.guarantee = at_least_once
state.dir = /tmp/kafka-streams/fraud-detector
cache.max.bytes.buffering = 10485760 (10 MB)
```

### State Store

- **frequent-calls-count:** RocksDB state store через `Materialized.as()`.
- **nps-escalation-store:** persistent KeyValueStore, wall-clock punctuation every 1 мин для purging expired entries, TTL 24ч.

### Сериализация

`KStream<String, String>` — JSON через StringSerializer/StringDeserializer. Все поля извлекаются из JSON через ручной parsing (`indexOf`/`substring`).

---

## 7. Детали реализации: transcription-analyzer

### Роль
Генерация синтетической транскрипции, keyword-based суммаризация, обогащение с профилем клиента, dual-write в Kafka + PostgreSQL.

### Ключевые классы

| Класс | Роль |
|-------|------|
| `CallMetadataProcessor.java` | Orchestrator pipeline: consume → generate transcription → summary → enrich → dual-write |
| `TranscriptionProducer.java` | Template-based synthetic transcription (~150 words/min) |
| `SummaryGenerator.java` | Keyword-based heuristic: problem/solution/sentiment/urgency/confidence |
| `CustomerProfileManager.java` | Broadcast enrichment — in-memory `ConcurrentHashMap<String, Map<String, String>>` |
| `EnrichmentService.java` | Обогащение summary с segment/riskLevel/priority из CustomerProfileManager |
| `DualWriter.java` | Kafka-first write → DLQ при неудаче, PostgreSQL через Kafka Connect JDBC Sink |
| `MetadataManager.java` | Status lifecycle: PENDING → TRANSCRIBING → SUMMARIZING → COMPLETED |

### Synthetic Transcription (TranscriptionProducer.java)

- **Template-based:** pipe-separated templates с рандомными topics (card, loan, payment, balance, transfer, fraud, complaint, limit).
- **Word count:** `wordsPerMinute=150` (configurable), capped at 10–5000 words.
- **Quality score:** random 0.0–1.0.
- **Language:** random ru/en.

### Summary Generation (SummaryGenerator.java)

Keyword-based heuristic, **без LLM**:

| Keyword | Problem | Solution | Sentiment | Urgency |
|---------|---------|----------|-----------|---------|
| fraud/unauthorized/stolen | fraud_suspected | escalated | negative | critical |
| lost/stolen/card | card_loss | card_blocked | negative | high |
| complaint/escalat | complaint | escalation_created | negative | high |
| loan/credit/limit | credit_inquiry | info_provided | neutral | low |
| blocked/block | card_loss | card_blocked | neutral | medium |
| else | general_inquiry | info_provided | positive | low |

**Confidence formula:** `(matchedKeywords / totalKeywords) × 0.5 + (categoryMatch ? 0.5 : 0)`

### Customer Profile Enrichment (broadcast pattern)

- `CustomerProfileConsumer` слушает `customers.profile` через `@KafkaListener`.
- `CustomerProfileManager` хранит профили в `ConcurrentHashMap<String, Map<String, String>>`.
- **Не KTable join** — broadcast pattern из-за key mismatch (summary key=callId, profile key=phone).

### Kafka-first Write Strategy (DualWriter.java)

Порядок записи:
1. **Kafka:** produce to `transcription.enriched` в Confluent JSON format (`{"schema": {...}, "payload": {...}}`).
2. **DLQ:** если Kafka failed → `transcription.enriched.dlq`.
3. **PostgreSQL:** данные попадают через Kafka Connect JDBC Sink (не напрямую из сервиса).
4. **Metadata:** `INSERT ... ON CONFLICT DO NOTHING` для call_metadata record.

---

## 8. Детали реализации: reporting-nps

### Роль
CQRS read side. Потребляет события из 4 Kafka topics, хранит в PostgreSQL, предоставляет REST API для аналитики.

### Ключевые классы

| Класс | Роль |
|-------|------|
| `MetadataConsumer.java` | `@KafkaListener` на `calls.metadata`, `AckMode.MANUAL` |
| `EnrichedTranscriptionConsumer.java` | `@KafkaListener` на `transcription.enriched`, `AckMode.MANUAL`, cache invalidation only (data via Kafka Connect) |
| `FraudAlertConsumer.java` | `@KafkaListener` на `calls.fraud-alerts`, `AckMode.MANUAL`, correlates with call metadata |
| `CompletedEventConsumer.java` | `@KafkaListener` на `calls.completed`, `AckMode.MANUAL`, enriches metadata with agentId/duration/NPS |
| `ReportController.java` | `GET /api/reports/daily` (with from/to params) |
| `AgentReportController.java` | `GET /api/reports/agent/{id}` |
| `SentimentController.java` | `GET /api/sentiment/distribution` |
| `MetadataController.java` | `GET /api/metadata/{callId}`, `GET /api/metadata/status/{status}` |
| `HealthController.java` | `GET /api/health` |
| `CacheConfig.java` | `ConcurrentMapCacheManager` (metadataCache, reportCache, sentimentCache) |

### JPA Entities

| Entity | Table | Columns | PK |
|--------|-------|---------|-----|
| `CallMetadata.java` | call_metadata | 21 | call_id (UUID) |
| `CallTranscription.java` | call_transcriptions | 17 | transcription_id (UUID) |
| `FraudStats.java` | fraud_stats | 9 | id (BIGSERIAL) |

### Consumer Configuration

- **AckMode.MANUAL** для всех 3 consumers — подтверждение после успешной записи в PostgreSQL.
- Consumer group: `reporting-nps`.

### REST API

| Endpoint | Description |
|----------|-------------|
| `GET /api/reports/daily` | Дневная сводка (NPS, кол-во звонков) с from/to params |
| `GET /api/reports/agent/{id}` | Статистика по агенту |
| `GET /api/metadata/{callId}` | Актуальный статус звонка |
| `GET /api/metadata/status/{status}` | Звонки по статусу |
| `GET /api/sentiment/distribution` | Распределение тональности |
| `GET /api/health` | Health check |

### Кэширование

`ConcurrentMapCacheManager` с 3 кешами: metadataCache, reportCache, sentimentCache. **Не Caffeine** — простая in-memory ConcurrentMap.

---

## 9. Детали реализации: load-simulator

### Роль
Генератор синтетической нагрузки. Вызывает call-processor REST API с конфигурируемыми параметрами.

### Ключевые классы

| Класс | Роль |
|-------|------|
| `LoadController.java` | `POST /api/load/start`, `POST /api/load/stop`, `GET /api/load/status` |
| `LoadSchedulerManager.java` | Управляет жизненным циклом генерации (start/stop/status) |
| `LoadScheduler.java` | Планировщик генерации с configurable burst size и duration |
| `CallGenerator.java` | Базовый генератор normal calls |
| `FraudScenarioClassifier.java` | Классифицирует вызовы в fraud-сценарии |

### 4 Fraud-сценария (FraudScenario enum)

| Сценарий | Factory | Описание |
|----------|---------|----------|
| **NORMAL** | CallGenerator | Random phone, duration 30–300s, random agent, NPS 0–10 |
| **ANOMALOUS_DURATION** | AnomalousDurationCallFactory | Duration 301–600 секунд |
| **FREQUENT_CALLS** | FrequentCallsCallFactory | Burst 6–10 calls из одного phone, duration 30–150s |
| **NPS_ESCALATION** | NpsEscalationCallFactory | 3–5 calls из одного phone, NPS 0 или 1 |

### Распределение (FraudScenarioClassifier)

```
normalCalls = totalCalls × (100 - fraudPercent) / 100
fraudCalls = totalCalls × fraudPercent / 100
  ├── ANOMALOUS_DURATION: fraudPercent / 3
  ├── FREQUENT_CALLS: fraudPercent / 3
  └── NPS_ESCALATION: fraudPercent / 3
```

---

## 10. Инфраструктура

### Docker Compose (21 контейнер)

**Core services (profile `core`):**
- 3× Kafka broker (KRaft, no ZooKeeper)
- call-processor, fraud-detector, transcription-analyzer, reporting-nps, load-simulator
- PostgreSQL 15
- Schema Registry (port 8085)
- Kafka Connect (port 8086)
- ksqlDB Server (port 8088)
- kafka-init, kafka-bootstrap (batch jobs для инициализации)
- kafka-connect-init, ksqldb-init (batch jobs)

**Monitoring (profile `monitoring`):**
- Prometheus (port 9090)
- Grafana (port 3000)
- Kafdrop (port 9000)
- Kafka Exporter (port 9308)
- Postgres Exporter (port 9187)

### Kafka Cluster (KRaft, 3 brokers)

| Broker | External Port | Internal Port | Controller |
|--------|--------------|---------------|------------|
| kafka-1 | 9092 | 9092 | Yes |
| kafka-2 | 9094 | 9092 | Yes |
| kafka-3 | 9096 | 9092 | Yes |

- **KRaft mode:** no ZooKeeper, embedded controller quorum (3/3 voters).
- **SASL/PLAIN** на EXTERNAL listener, **PLAINTEXT** на INTERNAL listener (broker-to-broker).
- **Replication:** 3 для stream topics, 3 для compacted topics.
- **Partitions:** 6 для основных топиков, 3 для DLQ.

### PostgreSQL Schema

**Таблицы:**

| Table | Columns | Description |
|-------|---------|-------------|
| `call_metadata` | 21 | Метаинформация звонков (статус, сегмент, риск, приоритет) |
| `call_transcriptions` | 17 | Расшифровки с FK на call_metadata (CASCADE DELETE) |
| `fraud_stats` | 9 | Агрегированная статистика фрода |

**View:** `v_full_call_info` — LEFT JOIN call_metadata + call_transcriptions.

**Indexes:** 10 indexes covering customer_phone, agent_id, call_start_time, call_status, sentiment, urgency, processing_status, problem.

**User:** `kafka-connect-user` — minimal privileges: SELECT+INSERT на call_transcriptions.

---

## 11. Monitoring & Observability

### Prometheus Scrape Targets

| Job | Target | Metrics Path |
|-----|--------|-------------|
| `kafka` | kafka-1/2/3:5556 | JMX Exporter (Kafka broker metrics) |
| `microservices` | call-processor:8081, fraud-detector:8082, transcription-analyzer:8083, reporting-nps:8084 | `/actuator/prometheus` (Spring Boot Actuator + Micrometer) |
| `postgres` | postgres-exporter:9187 | PostgreSQL metrics |

### Grafana Dashboards (8 total)

| Dashboard | Description |
|-----------|-------------|
| `kafka-overview.json` | Throughput, Lag, Under-replicated partitions, Broker Disk |
| `postgres-overview.json` | PostgreSQL connections, queries, table stats |
| `call-processor.json` | HTTP requests, Kafka producer metrics, JVM, GC |
| `fraud-detector.json` | JVM, CPU, HTTP, Process Memory, GC Pause Count |
| `transcription-analyzer.json` | HTTP, Kafka consumer/producer, PostgreSQL queries |
| `reporting-nps.json` | HTTP, Kafka consumer lag, PostgreSQL queries, cache hits |
| `system-overview.json` | Cross-service summary, PostgreSQL metrics |
| `kafka-connect-jdbc-sink.json` | Connector status, task status, failed count, DLQ events |

### Spring Boot Actuator Metrics

| Category | Metrics |
|----------|---------|
| **Kafka Producer** | `spring_kafka_template_seconds_count`, `spring_kafka_template_failed_produce_events_total` |
| **Kafka Consumer** | `spring_kafka_listener_seconds_count`, `spring_kafka_listener_consumed_total` |
| **HTTP** | `http_server_requests_seconds_count` (by method, path, status) |
| **JVM** | `jvm_gc_pause_seconds`, `jvm_memory_bytes_used`, `jvm_threads_current` |
| **Process** | `process_cpu_usage`, `process_memory_resident_bytes` |
| **HikariCP** | `hikaricp_connections_active`, `hikaricp_connections_pending` |

### Алерты (alerting-rules.yml)

| Alert | Condition | Severity |
|-------|-----------|----------|
| `KafkaConnectDLQEvents` | `rate(kafka_connect_sink_task_offline_total[5m]) > 0` | WARNING |
| `KafkaConnectConnectorFailed` | `KafkaConnectConnectorStatus == FAILED` | CRITICAL |
| `KafkaConnectConnectorPaused` | `KafkaConnectConnectorStatus == PAUSED` | WARNING |
| `KafkaConnectTaskFailed` | `KafkaConnectTaskStatus == FAILED` | CRITICAL |

---

## 12. Ключевые архитектурные решения

### 1. JSON вместо Avro

**Decision:** Использовать JSON (StringSerializer/StringDeserializer) вместо Avro.

**Rationale:**
- Простота отладки — human-readable events в Kafka.
- Schema Registry хранит JSON-схемы в Confluent JSON format (`{"schema": {...}, "payload": {...}}`).
- BACKWARD compatibility для эволюции схем.
- Kafka Connect использует `JsonConverter` для JDBC Sink.

**Impact:** Нет необходимости в Avro code generation, проще CI/CD pipeline.

### 2. Kafka Streams: at_least_once

**Decision:** fraud-detector использует `processing.guarantee=at_least_once`.

**Rationale:**
- Для fraud detection дублирование алертов допустимо (false positive лучше false negative).
- Exactly-once в Kafka Streams 3.6.1 имеет overhead и ограничения.
- State Store (RocksDB) восстанавливается из changelog topics при rebalance.

**Impact:** reporting-nps может получать дубликаты fraud-alerts, но бизнес-логика это обрабатывает.

### 3. Broadcast Enrichment вместо KTable Join

**Decision:** CustomerProfileManager использует in-memory `ConcurrentHashMap` вместо Kafka Streams KTable join.

**Rationale:**
- Key mismatch: transcription.summary key=callId, customers.profile key=phone.
- KTable join требует одинаковый key для join operation.
- Broadcast pattern проще и быстрее для small lookup table (10 customers).

**Impact:** Customer profiles загружаются полностью в память при старте. При большом количестве клиентов потребуется стратегия KTable join.

### 4. Keyword-based Summary вместо LLM

**Decision:** SummaryGenerator.java использует эвристический анализ по ключевым словам.

**Rationale:**
- Детерминированная логика — предсказуемые результаты.
- Нет зависимости от внешних LLM API.
- Простота тестирования и отладки.
- Производительность: O(n) по длине текста.

**Impact:** Качество суммаризации ниже, чем у LLM, но система работает автономно и детерминированно.

### 5. Kafka-first Write (transcription-analyzer)

**Decision:** transcription-analyzer пишет enriched events только в Kafka, PostgreSQL заполняется через Kafka Connect JDBC Sink.

**Rationale:**
- Kafka как primary event source — для downstream consumers (reporting-nps, Kafka Connect).
- PostgreSQL как analytics store — заполняется автоматически через Kafka Connect JDBC Sink.
- DLQ при неудаче записи в Kafka.
- Исключает race condition и дублирование записей в PostgreSQL.

**Impact:** reporting-nps получает данные из PostgreSQL (Kafka Connect) + cache invalidation через `transcription.enriched` topic.

### 6. CQRS с Manual Acknowledgment

**Decision:** reporting-nps использует `AckMode.MANUAL` для всех Kafka consumers.

**Rationale:**
- Подтверждение только после успешной записи в PostgreSQL.
- Гарантия: сообщение не будет потеряно, если запись в БД не удалась.
- Orphan buffering для handle out-of-order events.

**Impact:** При падении PostgreSQL consumer не подтверждает offset — message будет redelivered при восстановлении.

### 7. Spring Security не реализован

**Decision:** Все REST endpoints открыты.

**Rationale:**
- Development/evaluation environment.
- Kafka SASL/PLAIN обеспечивает безопасность на уровне event backbone.
- PostgreSQL user-based permissions для analytics DB.

**Impact:** Любой клиент может вызывать REST API без аутентификации. Для production рекомендуется добавить Spring Security.

---

## 13. Testing Strategy

### Unit Tests
- **Frameworks:** JUnit 5, Mockito, AssertJ.
- **Coverage:** > 70% line coverage.
- **Key classes:** CallValidatorTest, LLMsimulatorTest, NPSCalculatorTest.
- **Embedded Kafka:** для тестов, связанных с Kafka producer/consumer.

### Integration Tests
- **Frameworks:** JUnit 5, Testcontainers, Spring Boot Test, Awaitility, RestAssured.
- **Containers:** Kafka, PostgreSQL, Schema Registry.
- **Coverage:** > 50% key scenarios.
- **Key tests:** CallProcessorIntegrationTest, FraudDetectorIntegrationTest, TranscriptionAnalyzerIntegrationTest.

### E2E Tests
- **Script:** `infrastructure/e2e-bootstrap.sh` — full pipeline verification.
- **Phases:** health check, topic creation, data flow, API verification, PostgreSQL data verification.
- **Make target:** `make e2e` (e2e-setup + e2e-verify).

### Load Testing
- **Tool:** k6 (HTTP load), kcat (Kafka load).
- **Scenarios:** basic (2h), peak (5min), soak (8h), error injection (30min).
- **Metrics:** p95 < 45s, consumer lag < 2000, CPU < 70%, memory < 80%.

### Chaos Testing
- **Scenarios:** broker failure, service failure, consumer failure, network partition, resource constraint, hard shutdown.
- **Metrics:** recovery time < 60s (broker), < 10s (service), 0% data loss.

---

## 14. Deployment

### Quick Start

```bash
# Полный стек
make setup          # deploy + health + init-topics

# Только core
make deploy-core

# Только мониторинг
make deploy-monitoring

# Проверка здоровья
make health

# Логи
make logs

# E2E verification
make e2e
```

### Resource Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| RAM | 12 GB | 16 GB |
| CPU | 4 cores | 6 cores |
| Disk | 50 GB | 100 GB |

### Profiles

| Profile | Services |
|---------|----------|
| `full` | All 21 containers |
| `core` | Kafka + 5 microservices + PostgreSQL + Schema Registry + Kafka Connect + ksqlDB |
| `monitoring` | Prometheus + Grafana + Kafdrop + Exporters |
| `load-test` | Load simulator + monitoring |

---

## 15. Key Metrics & Performance

### Throughput Targets

| Metric | Normal Load | Peak Load |
|--------|-------------|-----------|
| Calls processed | ~1.4 calls/sec (5000/hr) | ~100 calls/sec |
| End-to-end processing | < 45 sec (p95) | < 90 sec (p95) |
| Consumer lag | < 2000 | < 8000 |
| CPU usage | < 70% | < 90% |
| Memory usage | < 80% | < 95% |

### Data Flow Latency

| Stage | Expected Latency |
|-------|-----------------|
| REST → Kafka (call-processor) | < 100ms |
| Kafka → Fraud Detection | < 500ms |
| Kafka → Transcription Generation | < 1s |
| Kafka → Summary Generation | < 2s |
| Kafka → Dual-Write (PG) | < 3s |
| Kafka → Reporting-NPS (PG) | < 5s |

### State Store Recovery

| Component | Recovery Mechanism | Time |
|-----------|-------------------|------|
| fraud-detector RocksDB | Restore from changelog topics | ~30s for 1000 records |
| Kafka Streams | State Store rebuild from Kafka logs | Depends on data volume |
| PostgreSQL | Volume persistence (docker volume) | Instant on restart |

---

## 16. Known Limitations

### Что НЕ реализовано

1. **Spring Security** — все REST endpoints открыты, нет аутентификации/авторизации.
2. **Real transcription** — synthetic template-based generation, no speech-to-text.
3. **LLM Summary** — keyword-based heuristic, no actual LLM integration.
4. **Exactly-once for Streams** — fraud-detector использует at_least_once.
5. **Horizontal scaling** — docker-compose profiles, но no K8s manifests.
6. **CI/CD pipeline** — Makefile для локального запуска, no GitHub Actions/GitLab CI.

### Technical Debt

1. **Topic Manager replication-factor=1** — в dev mode, init-topics.sh использует replication-factor=3.
2. **Manual JSON parsing** — fraud-detector использует `indexOf`/`substring` вместо Serde.
3. **ConcurrentMapCacheManager** — нет TTL/TTL eviction, может расти неограниченно.
4. **No circuit breaker** — при падении PostgreSQL reporting-nps retry indefinitely.
5. **Hardcoded thresholds** — fraud detection thresholds в FraudDetectorProperties.java, no external config.

---

## 17. Summary

### Что работает

- ✅ Full event-driven pipeline: REST → Kafka → 3 parallel processors → PostgreSQL
- ✅ Real-time fraud detection (3 patterns: frequent calls, anomalous duration, NPS escalation)
- ✅ Synthetic transcription + keyword-based summary generation
- ✅ Broadcast enrichment with customer profiles
- ✅ Kafka-first write strategy (Kafka + DLQ fallback, PostgreSQL via Kafka Connect JDBC Sink)
- ✅ CQRS read side with REST API for analytics
- ✅ Kafka Connect JDBC Sink for automated PostgreSQL sync
- ✅ ksqlDB analytics layer (TUMBLING windows, GROUP BY aggregation)
- ✅ Full monitoring stack (Prometheus + Grafana + 8 dashboards + alerting)
- ✅ 10 Kafka topics with proper partitioning and replication
- ✅ E2E testing with Testcontainers
- ✅ Load testing with k6 (4 scenarios)
- ✅ Chaos testing (6 scenarios: broker, service, consumer, network, resource, hard shutdown)

### Архитектурные паттерны

- **Event-Driven Architecture** — choreography pattern через Kafka events
- **CQRS** — write path (Kafka) ≠ read path (PostgreSQL)
- **Broadcast Enrichment** — in-memory map для lookup (key mismatch solution)
- **Kafka-first Write** — Kafka-only from service, PostgreSQL via Kafka Connect JDBC Sink, DLQ on failure
- **Stateful Processing** — Kafka Streams + RocksDB State Store
- **Manual Acknowledgment** — at_least_once с гарантией не-потери сообщений
- **Keyword-based Heuristic** — детерминированная логика без внешних зависимостей

### Масштабирование

- **Horizontal:** docker-compose `--scale` для stateless services (call-processor, reporting-nps)
- **Vertical:** increase memory/CPU for stateful services (fraud-detector, transcription-analyzer)
- **Kafka:** 6 partitions = max 6 parallel consumers per consumer group
- **PostgreSQL:** connection pooling (HikariCP), read replicas for reporting-nps
