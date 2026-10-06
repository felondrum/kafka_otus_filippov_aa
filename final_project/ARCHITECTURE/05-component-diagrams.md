# 5. Component Diagrams (per service)

## 5.1. call-processor

```
┌──────────────────────────────────────────────────────────────┐
│                    call-processor                             │
│                    (Spring Boot REST API)                     │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              REST API Layer                              │ │
│  │                                                          │ │
│  │  POST /api/calls                                         │ │
│  │  GET  /api/health                                        │ │
│  │                                                          │ │
│  │  ┌─────────────┐   ┌──────────────────┐                 │ │
│  │  │  Controller │──▶│  Validator       │                 │ │
│  │  └─────────────┘   └──────────────────┘                 │ │
│  │                         │                                │ │
│  │                         ▼                                │ │
│  │                   ┌──────────────┐                       │ │
│  │                   │  DTO/Schema  │                       │ │
│  │                   │  Validation  │                       │ │
│  │                   └──────────────┘                       │ │
│  └─────────────────────────────────────────────────────────┘ │
│                              │                                │
│  ┌───────────────────────────┼─────────────────────────────┐ │
│  │                           ▼                             │ │
│  │              ┌────────────────────────┐                  │ │
│  │              │  Kafka Producer        │                  │ │
│  │              │                        │                  │ │
│  │              │  enable.idempotence=true│                  │ │
│  │              │  acks=all               │                  │ │
│  │              │  retries=3              │                  │ │
│  │              │  batch.size=16384       │                  │ │
│  │              └────────┬───────────────┘                  │ │
│  │                       │                                  │ │
│  │                       ├──────────────────────────────────┤ │
│  │                       │                                  │ │
│  │                       ▼                                  │ │
│  │              ┌─────────────────┐                         │ │
│  │              │  Topic Manager  │                         │ │
│  │              │  (Startup)      │                         │ │
│  │              │                 │                         │ │
│  │              │  - createTopics │                         │ │
│  │              │  - configureACL │                         │ │
│  │              └─────────────────┘                         │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              Error Handling                              │ │
│  │                                                          │ │
│  │  ┌─────────────┐   ┌──────────────┐   ┌─────────────┐  │ │
│  │  │  Retry      │──▶│  DLQ         │──▶│  Monitor    │  │ │
│  │  │  (3x)       │   │  (calls.dlq) │   │  (metrics)  │  │ │
│  │  └─────────────┘   └──────────────┘   └─────────────┘  │ │
│  └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

**Компоненты:**

| Компонент | Описание |
|-----------|----------|
| **REST Controller** | Обработка HTTP запросов (`POST /api/calls`), валидация входных данных |
| **Validator** | Bean Validation (JSR-380): UUID format, E.164 phone, duration >= 1, agentId not blank, NPS 0-10 |
| **Kafka Producer** | `KafkaTemplate<String, String>` с idempotent mode (`enable.idempotence=true`, `acks=all`) |
| **Topic Manager** | `@PostConstruct initTopics()` — создаёт 3 топика через AdminClient (replication-factor=1) |
| **Error Handler** | Spring Retry `@Retryable` (3 попытки, backoff 1s×2), DLQ на `calls.dlq` |

**REST API:**

| Метод | Endpoint | Описание |
|-------|----------|----------|
| POST | `/api/calls` | Принять событие о завершённом звонке |
| GET | `/api/health` | Health check (Spring Actuator) |

**Валидация (CallEventRequest.java):**

| Поле | Правило | Описание |
|------|---------|----------|
| `callId` | `@NotBlank` + `@Pattern` (UUID) | Уникальный идентификатор звонка |
| `phone` | `@NotBlank` + `@Pattern` (E.164) | Номер телефона |
| `duration` | `@Min(1)` | Длительность (1+ сек) |
| `agentId` | `@NotBlank` | Идентификатор агента |
| `npsScore` | `@Min(0)` + `@Max(10)` | NPS-оценка (0-10) |

## 5.2. fraud-detector

```
┌──────────────────────────────────────────────────────────────┐
│                    fraud-detector                             │
│              (Spring Boot + Kafka Streams)                    │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              Kafka Streams Pipeline                      │ │
│  │                                                          │ │
│  │  ┌─────────────┐    ┌──────────────┐                    │ │
│  │  │  Source      │──▶ │  Filter      │                    │ │
│  │  │  (calls.    │    │  (duration   │                    │ │
│  │  │   completed)│    │   < 5 sec)   │                    │ │
│  │  └─────────────┘    └──────────────┘                    │ │
│  │                            │                             │ │
│  │                            ▼                             │ │
│  │                     ┌──────────────┐                     │ │
│  │                     │  GroupBy     │                     │ │
│  │                     │  (phone)     │                     │ │
│  │                     └──────────────┘                     │ │
│  │                            │                             │ │
│  │                            ▼                             │ │
│  │                     ┌──────────────┐                     │ │
│  │                     │  Windowed    │                     │ │
│  │                     │  Count       │                     │ │
│  │                     │  (1 min)     │                     │ │
│  │                     └──────────────┘                     │ │
│  │                            │                             │ │
│  │                            ▼                             │ │
│  │                     ┌──────────────┐                     │ │
│  │                     │  Predicate   │                     │ │
│  │                     │  (count > 5) │                     │ │
│  │                     └──────────────┘                     │ │
│  │                            │                             │ │
│  │                    ┌───────┴───────┐                     │ │
│  │                    │               │                     │ │
│  │                    ▼               ▼                     │ │
│  │          ┌──────────────┐  ┌──────────────┐             │ │
│  │          │  Fraud Alert │  │  Pass-       │             │ │
│  │          │  (fraud-     │  │  through     │             │ │
│  │          │   alerts)    │  │              │             │ │
│  │          └──────────────┘  └──────────────┘             │ │
│  │                            │                             │ │
│  │                            ▼                             │ │
│  │                     ┌──────────────┐                     │ │
│  │                     │  Anomalous   │                     │ │
│  │                     │  Duration    │                     │ │
│  │                     │  (> 300 sec) │                     │ │
│  │                     └──────────────┘                     │ │
│  │                            │                             │ │
│  │                            ▼                             │ │
│  │          ┌──────────────────────────────────┐           │ │
│  │          │  Fraud Alert (ANOMALOUS_DURATION)│           │ │
│  │          └──────────────────────────────────┘           │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              Processor API (Escalation)                  │ │
│  │                                                          │ │
│  │  ┌─────────────┐    ┌──────────────┐   ┌─────────────┐  │ │
│  │  │  State       │──▶ │  Counter     │──▶ │  Threshold  │  │ │
│  │  │  Store       │    │  (3x NPS < 2)│   │  Check      │  │ │
│  │  │  (RocksDB)   │    └──────────────┘   └─────────────┘  │ │
│  │  └─────────────┘                                         │ │
│  │         │                                                │ │
│  │         ▼                                                │ │
│  │  ┌─────────────────────────────────┐                    │ │
│  │  │  Auto-cleanup (TTL: 24h)        │                    │ │
│  │  └─────────────────────────────────┘                    │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌────────────────────────────────────────��────────────────┐ │
│  │              Output                                      │ │
│  │                                                          │ │
│  │  ┌──────────────────────────────────────────────┐       │ │
│  │  │  calls.fraud-alerts (key=phone, JSON)        │       │ │
│  │  └──────────────────────────────────────────────┘       │ │
│  └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

**Компоненты:**

| Компонент | Описание |
|-----------|----------|
| **Source** | Чтение из `calls.completed` (String/String, JSON) |
| **PhoneNormalizer** | Нормализация phone number в E.164 формат |
| **Filter** | Отсев коротких звонков (< 5 сек, configurable) |
| **FrequentCallsProcessor** | DSL Hopping Window (60s size, 10s advance), RocksDB, > 5 calls/window → FREQUENT_CALLS (MEDIUM) |
| **AnomalousDurationProcessor** | Filter: duration > 300 сек → ANOMALOUS_DURATION (LOW) |
| **NpsEscalationProcessor** | Processor API + KeyValueStore, 3x NPS < 2 за 24ч → NPS_ESCALATION (HIGH) |
| **State Store** | RocksDB (frequent-calls-count) + persistent KeyValueStore (nps-escalation-store) |
| **Output** | Запись алертов в `calls.fraud-alerts` (JSON via StringSerializer) |

**Streams Configuration:**

```
processing.guarantee = at_least_once
state.dir = /tmp/kafka-streams/fraud-detector
cache.max.bytes.buffering = 10485760 (10 MB)
spring.application.name = fraud-detector
```

**FraudDetectorProperties.java (конфигурируемые параметры):**

| Параметр | Значение по умолчанию | Описание |
|----------|---------------------|----------|
| `window.size` | 60000ms (1 мин) | Hopping window для frequent calls |
| `window.advance` | 10000ms (10 сек) | Advance interval |
| `threshold` | 5 | Max calls per window |
| `nps.threshold` | 3 | Max negative NPS per 24h |
| `nps.score` | 2 | NPS < 2 = negative |
| `min.duration` | 5000ms (5 сек) | Минимальная длительность |
| `max.duration` | 300000ms (5 мин) | Максимальная длительность |

**Streams Configuration:**

```
processing.guarantee = exactly_once_v2
state.dir = /tmp/kafka-streams/fraud-detector
cache.max.bytes.buffering = 10485760
schema.registry.url = http://schema-registry:8085
spring.application.name = fraud-detector
```

**Health Check:**

```
Spring Boot Actuator: GET /actuator/health
- 200 OK when Kafka Streams topology is running
- 503 Service Unavailable when Kafka connection is lost
```

## 5.3. transcription-analyzer

```
┌──────────────────────────────────────────────────────────────┐
│                 transcription-analyzer                        │
│           (Spring Boot + Producer + Streams + Consumer)       │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              Producer Part                               │ │
│  │                                                          │ │
│  │  ┌─────────────┐    ┌──────────────┐   ┌─────────────┐  │ │
│  │  │  Simulate   │──▶ │  Generate    │──▶ │  Produce    │  │ │
│  │  │  Audio→Text │    │  Raw Text    │   │  to         │  │ │
│  │  │  (from      │    │  (from       │   │  transcription.│ │ │
│  │  │   duration) │    │    duration) │   │   raw)       │  │ │
│  │  └─────────────┘    └──────────────┘   └─────────────┘  │ │
│  └─────────────────────────────────────────────────────────┘ │
│                              │                                │
│  ┌───────────────────────────┼─────────────────────────────┐ │
│  │                           ▼                             │ │
│  │              ┌────────────────────────┐                  │ │
│  │              │  transcription.raw     │                  │ │
│  │              │  (key=callId, JSON)    │                  │ │
│  │              └────────────────────────┘                  │ │
│  └─────────────────────────────────────────────────────────┘ │
│                              │                                │
│  ┌───────────────────────────┼─────────────────────────────┐ │
│  │                           ▼                             │ │
│  │              ┌────────────────────────┐                  │ │
│  │              │  Streams Processing    │                  │ │
│  │              │                        │                  │ │
│  │              │  ┌──────────────────┐  │                  │ │
│  │              │  │  Read raw        │  │                  │ │
│  │              │  │  Read summary    │  │                  │ │
│  │              │  │  Join with       │  │                  │ │
│  │              │  │  customers.profile│ │                  │ │
│  │              │  └──────────────────┘  │                  │ │
│  │              │                        │                  │ │
│  │              │  ┌──────────────────┐  │                  │ │
│  │              │  │  Extract:        │  │                  │ │
│  │              │  │  - problem       │  │                  │ │
│  │              │  │  - solution      │  │                  │ │
│  │              │  │  - sentiment     │  │                  │ │
│  │              │  │  - urgency       │  │                  │ │
│  │              │  │  - confidence    │  │                  │ │
│  │              │  └──────────────────┘  │                  │ │
│  │              │                        │                  │ │
│  │              │  ┌──────────────────┐  │                  │ │
│  │              │  │  Enrich with:    │  │                  │ │
│  │              │  │  - segment       │  │                  │ │
│  │              │  │  - risk_level    │  │                  │ │
│  │              │  │  - priority      │  │                  │ │
│  │              │  └──────────────────┘  │                  │ │
│  │              └────────────────────────┘                  │ │
│  └─────────────────────────────────────────────────────────┘ │
│                              │                                │
│  ┌───────────────────────────┼─────────────────────────────┐ │
│  │                           ▼                             │ │
│  │              ┌────────────────────────┐                  │ │
│  │              │  dual-write strategy   │                  │ │
│  │              │                        │                  │ │
│  │              │  ├─[Kafka]──▶          │                  │ │
│  │              │  │  transcription.      │                  │ │
│  │              │  │  enriched            │                  │ │
│  │              │  │                      │                  │ │
│  │              │  └─[JDBC]──▶           │                  │ │
│  │              │     PostgreSQL          │                  │ │
│  │              │     call_transcriptions │                  │ │
│  │              └────────────────────────┘                  │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              Metadata Manager                            │ │
│  │                                                          │ │
│  │  ┌──────────────────────────────────────────────┐       │ │
│  │  │  Status Lifecycle:                           │       │ │
│  │  │  PENDING → TRANSCRIBING →                    │       │ │
│  │  │  SUMMARIZING → COMPLETED                     │       │ │
│  │  └──────────────────────────────────────────────┘       │ │
│  │                                                          │ │
│  │  ┌──────────────────────────────────────────────┐       │ │
│  │  │  Write to calls.metadata (compacted)         │       │ │
│  │  └──────────────────────────────────────────────┘       │ │
│  └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

**Компоненты:**

| Компонент | Описание |
|-----------|----------|
| **CallCompletedConsumer** | `@KafkaListener` на `calls.completed` — триггерит pipeline |
| **TranscriptionProducer** | Template-based synthetic transcription (~150 words/min, configurable) |
| **SummaryGenerator** | Keyword-based heuristic: извлечение problem/solution/sentiment/urgency/confidence |
| **CustomerProfileManager** | Broadcast enrichment — in-memory `ConcurrentHashMap<String, Map<String, String>>` |
| **EnrichmentService** | Обогащение summary с segment/riskLevel/priority из CustomerProfileManager |
| **DualWriter** | Kafka-first write: сначала `transcription.enriched`, потом PostgreSQL с retry (3 attempts, 1s→2s→4s), DLQ при неудаче |
| **MetadataManager** | Status lifecycle: PENDING → TRANSCRIBING → SUMMARIZING → COMPLETED |

**SummaryGenerator.java (keyword-based):**

| Keyword | Problem | Solution | Sentiment | Urgency |
|---------|---------|----------|-----------|---------|
| fraud/unauthorized/stolen | fraud_suspected | escalated | negative | critical |
| lost/stolen/card | card_loss | card_blocked | negative | high |
| complaint/escalat | complaint | escalation_created | negative | high |
| loan/credit/limit | credit_inquiry | info_provided | neutral | low |
| blocked/block | card_loss | card_blocked | neutral | medium |

**Customer Profile Enrichment (broadcast pattern):**
- `CustomerProfileConsumer` слушает `customers.profile` через `@KafkaListener`
- `CustomerProfileManager` хранит профили в `ConcurrentHashMap<String, Map<String, String>>`
- `EnrichmentService` выполняет lookup по phone при обработке каждого summary event
- **Не KTable join** — broadcast pattern из-за key mismatch (summary key=callId, profile key=phone)

## 5.4. reporting-nps

```
┌──────────────────────────────────────────────────────────────┐
│                    reporting-nps                              │
│              (Spring Boot + Consumer + REST API)              │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              Kafka Consumer Pipeline                     │ │
│  │                                                          │ │
│  │  ┌─────────────┐    ┌──────────────┐                    │ │
│  │  │  Source      │──▶ │  Aggregate   │                    │ │
│  │  │  (calls.    │    │  + Store     │                    │ │
│  │  │   metadata) │    │  (State)     │                    │ │
│  │  └─────────────┘    └──────────────┘                    │ │
│  │                            │                             │ │
│  │                            ▼                             │ │
│  │                     ┌──────────────┐                     │ │
│  │                     │  PostgreSQL  │                     │ │
│  │                     │  call_       │                     │ │
│  │                     │  metadata    │                     │ │
│  │                     └──────────────┘                     │ │
│  │                                                          │ │
│  │  ┌─────────────┐    ┌──────────────┐   ┌─────────────┐  │ │
│  │  │  Source      │──▶ │  Aggregate   │──▶ │  PostgreSQL │  │ │
│  │  │  (transcrip. │    │  + Store     │   │  call_      │  │ │
│  │  │   enriched) │    │  (State)     │   │  transcrip. │  │ │
│  │  └─────────────┘    └──────────────┘   └─────────────┘  │ │
│  │                                                          │ │
│  │  ┌─────────────┐    ┌──────────────┐   ┌─────────────┐  │ │
│  │  │  Source      │──▶ │  Store       │──▶ │  PostgreSQL │  │ │
│  │  │  (calls.    │    │  alerts      │   │  (fraud    │  │ │
│  │  │   fraud-    │    │              │   │   stats)    │  │ │
│  │  │   alerts)   │    └──────────────┘   └─────────────┘  │ │
│  │  └─────────────┘                                         │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              REST API Layer                              │ │
│  │                                                          │ │
│  │  ┌─────────────┐   ┌──────────────────┐                 │ │
│  │  │  Controller │◀──│  Service Layer   │                 │ │
│  │  └─────────────┘   └──────────────────┘                 │ │
│  │                         │                                │ │
│  │                         ├──▶ PostgreSQL (reports)        │ │
│  │                         ├──▶ State Store (fast query)    │ │
│  │                         └──▶ Cache (aggregations)        │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              Database Schema                             │ │
│  │                                                          │ │
│  │  ┌──────────────────────────────────────────────┐       │ │
│  │  │  call_metadata                               │       │ │
│  │  │  ┌──────────────────────────────────────┐    │       │ │
│  │  │  │ call_id (PK)                         │    │       │ │
│  │  │  │ phone                                │    │       │ │
│  │  │  │ duration                             │    │       │ │
│  │  │  │ agent_id                             │    │       │ │
│  │  │  │ nps_score                            │    │       │ │
│  │  │  │ segment                              │    │       │ │
│  │  │  │ risk_level                           │    │       │ │
│  │  │  │ priority                             │    │       │ │
│  │  │  │ status                               │    │       │ │
│  │  │  │ total_calls                          │    │       │ │
│  │  │  │ last_updated                         │    │       │ │
│  │  │  └──────────────────────────────────────┘    │       │ │
│  │  └──────────────────────────────────────────────┘       │ │
│  │                                                          │ │
│  │  ┌──────────────────────────────────────────────┐       │ │
│  │  │  call_transcriptions                         │       │ │
│  │  │  ┌──────────────────────────────────────┐    │       │ │
│  │  │  │ call_id (FK → call_metadata)        │    │       │ │
│  │  │  │ transcription_text                   │    │       │ │
│  │  │  │ sentiment                            │    │       │ │
│  │  │  │ urgency                              │    │       │ │
│  │  │  │ problem                              │    │       │ │
│  │  │  │ solution                             │    │       │ │
│  │  │  │ confidence                           │    │       │ │
│  │  │  └──────────────────────────────────────┘    │       │ │
│  │  └──────────────────────────────────────────────┘       │ │
│  │                                                          │ │
│  │  ┌──────────────────────────────────────────────┐       │ │
│  │  │  view v_full_call_info                       │       │ │
│  │  │  (JOIN call_metadata + call_transcriptions)  │       │ │
│  │  └──────────────────────────────────────────────┘       │ │
│  └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

**Компоненты:**

| Компонент | Описание |
|-----------|----------|
| **MetadataConsumer** | `@KafkaListener` на `calls.metadata`, `AckMode.MANUAL` — обновляет PostgreSQL call_metadata |
| **EnrichedTranscriptionConsumer** | `@KafkaListener` на `transcription.enriched`, `AckMode.MANUAL` — writes to call_transcriptions, orphan buffering |
| **FraudAlertConsumer** | `@KafkaListener` на `calls.fraud-alerts`, `AckMode.MANUAL` — aggregates fraud stats, correlates with call metadata |
| **ReportController** | `GET /api/reports/daily` (with from/to params) — daily reports with caching |
| **AgentReportController** | `GET /api/reports/agent/{id}` — per-agent performance |
| **SentimentController** | `GET /api/sentiment/distribution` — sentiment distribution |
| **MetadataController** | `GET /api/metadata/{callId}`, `GET /api/metadata/status/{status}` — call status queries |
| **CacheConfig** | `ConcurrentMapCacheManager` (metadataCache, reportCache, sentimentCache) |

**REST API:**

| Метод | Endpoint | Описание |
|-------|----------|----------|
| GET | `/api/reports/daily` | Дневная сводка (NPS, кол-во звонков) |
| GET | `/api/reports/agent/{id}` | Статистика по агенту |
| GET | `/api/metadata/{callId}` | Актуальный статус звонка |
| GET | `/api/metadata/status/{status}` | Звонки по статусу |
| GET | `/api/sentiment/distribution` | Распределение тональности за период |
| GET | `/api/health` | Health check |

**JPA Entities:**

| Entity | Table | Fields | Repositories |
|--------|-------|--------|-------------|
| `CallMetadata` | call_metadata (21 columns) | callId, customerPhone, agentId, callDuration, callStatus, sentiment, segment, riskLevel, priority, ... | CallMetadataRepository |
| `CallTranscription` | call_transcriptions (17 columns) | transcriptionId, callId(FK), transcriptionText, language, confidenceScore, problem, solution, sentiment, ... | CallTranscriptionRepository |
| `FraudStats` | fraud_stats (9 columns) | id, phone, callId, pattern, severity, count, lastAlertAt, agentId | FraudStatsRepository |

**REST API:**

| Метод | Endpoint | Описание |
|-------|----------|----------|
| GET | `/api/reports/daily` | Дневная сводка (NPS, кол-во звонков) |
| GET | `/api/reports/agent/{id}` | Статистика по агенту |
| GET | `/api/metadata/{callId}` | Актуальный статус звонка (из State Store) |
| GET | `/api/metadata/status/{status}` | Звонки по статусу |
| GET | `/api/sentiment/distribution` | Распределение тональности за период |
| GET | `/api/health` | Health check |
