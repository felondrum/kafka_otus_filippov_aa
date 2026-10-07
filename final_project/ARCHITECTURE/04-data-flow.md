# 4. Data Flow Architecture

## 4.1. Диаграмма потока данных

```
Caller (REST)
     │
     │  POST /api/calls {callId, phone, duration, agentId, npsScore}
     ▼
┌─────────────────┐
│  call-processor  │
│  (validate +     │
│   produce)       │
└────────┬────────┘
         │
         ├──────────────────────────────────────────────────────────┐
         │                                                          │
         ▼                                                          ▼
┌─────────────────────┐                                  ┌─────────────────────┐
│  calls.completed    │                                  │  calls.metadata     │
│  (Stream, callId)   │                                  │  (Compacted,       │
│  JSON               │                                  │   callId)           │
└────┬────────────────┘                                  └────────┬────────────┘
     │                                                             │
     │  fraud-detector                                            │  reporting-nps
     │  (Streams)                                                 │  (Consumer)
     ▼                                                             ▼
┌─────────────────────┐                                  ┌─────────────────────┐
│  calls.fraud-alerts │                                  │  PostgreSQL         │
│  (Stream, phone)    │                                  │  call_metadata      │
│  JSON               │                                  └─────────────────────┘
└─────────────────────┘                                           ▲
     │                                                            │
     │  reporting-nps                                             │
     │  (Consumer)                                                │
     ▼                                                            │
┌─────────────────────┐                                   ┌───────┴─────────────┐
│  reporting-nps      │                                   │ transcription-      │
│  (aggregations)     │                                   │ analyzer            │
└─────────────────────┘                                   │ (Producer +          │
                                                          │  Streams +           │
                                                          │  Consumer)           │
                                                          │  writes:             │
                                                          │  TRANSCRIBING        │
                                                          │  SUMMARIZING         │
                                                          │  COMPLETED           │
                                                          └───────┬─────────────┘
                                                                  │
                                              ┌───────────────────┼───────────────────┐
                                              │                       │                   │
                                              ▼                       ▼                   ▼
                                    ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
                                    │transcription.raw│   │transcription.   │   │customers.profile│
                                    │(Stream, callId) │   │summary          │   │(Compacted, phone) │
                                    │JSON              │   │(Stream, callId) │   │JSON              │
                                    └─────────────────┘   └─────────────────┘   └─────────────────┘
                                              │                       │
                                              └───────────┬───────────┘
                                                          │
                                                          ▼
                                                ┌─────────────────────┐
                                                │transcription.       │
                                                │enriched              │
                                                │(Stream, callId)      │
                                                │JSON                  │
                                                └───────────┬───────────┘
                                                            │
                                            ┌───────────────┼───────────────┐
                                            │               │               │
                                            ▼               ▼               ▼
                                   ┌────────────────┐  ┌──────────────┐  ┌──────────────┐
                                   │reporting-nps    │  │Kafka Connect │  │calls.dlq     │
                                   │(Consumer)       │  │JDBC Sink     │  │(Stream,      │
                                   │(aggregations)   │  │(primary      │  │ callId)      │
                                   └────────────────┘  │ writer)       │  │JSON          │
                                                       │               │  └──────────────┘
                                                       ▼               ▼
                                              ┌─────────────────────┐
                                              │  PostgreSQL         │
                                              │  call_transcriptions│
                                              └─────────────────────┘
```

## 4.2. Таблица топиков Kafka

| Топик | Тип | Ключ | Формат | Репликация | Партиции | Описание |
|-------|-----|------|--------|------------|----------|----------|
| `calls.completed` | Stream | `callId` | JSON (String/String) | 3 | 6 | Событие о завершённом звонке |
| `calls.metadata` | Compacted Table | `callId` | JSON (String/String) | 3 | 6 | Метаинформация звонка (жизненный цикл статусов). **Dual producer:** call-processor (PENDING) + transcription-analyzer (TRANSCRIBING → SUMMARIZING → COMPLETED) |
| `calls.fraud-alerts` | Stream | `phone` | JSON (String/String) | 3 | 6 | Алерты антифрод-модуля |
| `transcription.raw` | Stream | `callId` | JSON (String/String) | 3 | 6 | Сырая транскрипция диалога (синтетическая, template-based) |
| `transcription.summary` | Stream | `callId` | JSON (String/String) | 3 | 6 | Суммаризация (keyword-based heuristic, SummaryGenerator.java) |
| `transcription.enriched` | Stream | `callId` | Confluent JSON (`{"schema":{...},"payload":{...}}`) | 3 | 6 | Обогащённая суммаризация с профилем клиента |
| `calls.dlq` | Stream | `callId` | JSON (String/String) | 3 | 6 | Dead Letter Queue для битых сообщений |
| `customers.profile` | Compacted Table | `phone` | JSON (String/String) | 3 | 6 | Профили клиентов (сегмент, риск-уровень) |
| `transcription.enriched.dlq` | Stream | `callId` | JSON (String/String) | 3 | 3 | DLQ для failed enriched transcription events |
| `calls.completed.agg` | Stream | `agentId` | JSON (String/String) | 1 | 6 | **ksqlDB output**: агрегация завершённых звонков по agentId |
| `calls.fraud-alerts.agg` | Stream | `phone` | JSON (String/String) | 1 | 6 | **ksqlDB output**: агрегация фрод-алертов по phone |

## 4.3. Описание потоков данных

### 4.3.1. `calls.completed` → fraud-detector + transcription-analyzer

```
POST /api/calls
    │
    ▼
call-processor
    │  валидация (duration > 0, phone format, agentId not null)
    │  enrichment (lookup customers.profile via KTable join)
    │
    ├──[Kafka]──▶ calls.completed (key=callId, JSON)
    │       │
    │       ├─▶ fraud-detector (Streams)
    │       │       │
    │       │       ├─ фильтр: duration < 5 сек
    │       │       ├─ sliding window (1 мин): count(phone) > 5
    │       │       └─ Processor API: 3x негативный NPS за день
    │       │
    │       └─▶ transcription-analyzer (Streams)
    │               │
    │               ├─ генерация transcription.raw
    │               └─ enrichment с customers.profile
    │
    └──[Kafka]──▶ calls.metadata (key=callId, JSON, compacted)
            │
            └─▶ reporting-nps (Consumer)
                    │
                    └─▶ PostgreSQL (call_metadata)
```

### 4.3.2. `calls.metadata` → reporting-nps

```
call-processor
    │
    └──[Kafka]──▶ calls.metadata (compacted table)
            │
            │  key=callId (compaction по ключу)
            │  статус: PENDING (initial)
            │
            │  Dual Producer:
            │  transcription-analyzer (MetadataManager.java)
            │    ├── initialize() → TRANSCRIBING
            │    ├── onTranscriptionProduced() → TRANSCRIBING
            │    ├── onSummaryStarted() → SUMMARIZING
            │    └── onDualWriteComplete() → COMPLETED
            │
            │  Статусы: PENDING → TRANSCRIBING → SUMMARIZING → COMPLETED
            │
            └─▶ reporting-nps (Streams Consumer)
                    │
                    ├─ обновление PostgreSQL (call_metadata)
                    └─ кэширование State Store для fast query
```

### 4.3.3. `calls.fraud-alerts` → reporting-nps

```
fraud-detector
    │
    │  паттерны:
    │  - частые звонки: count(phone, 1min) > 5
    │  - эскалация: 3x NPS < 2 за 1 день
    │  - аномальная длительность: duration > 300 сек
    │
    └──[Kafka]──▶ calls.fraud-alerts (key=phone, JSON)
            │
            └─▶ reporting-nps (Consumer)
                    │
                    └─▶ PostgreSQL (aggregated fraud stats)
```

### 4.3.4. `transcription.raw` → transcription-analyzer (Streams)

```
transcription-analyzer (Producer)
    │
    │  симуляция аудио → текст:
    │  - генерация placeholder текста на основе duration
    │  - параметры: quality (0.0-1.0), language (ru/en)
    │
    └──[Kafka]──▶ transcription.raw (key=callId, JSON)
            │
            │  Streams processing:
            │  - извлечение ключевых слов (card, loan, fraud, complaint)
            │  - категоризация проблемы
            │
            └─▶ transcription.summary
```

### 4.3.5. `transcription.summary` → transcription-analyzer (Streams)

```
transcription.raw
    │
    │  Streams processing (summary-processor group):
    │  - synthetic summary generation (keyword-based, no LLM):
    │    - problem: извлечение проблемы (card_loss, credit_inquiry, complaint)
    │    - solution: извлечённое решение (card_blocked, info_provided)
    │    - sentiment: positive/neutral/negative
    │    - urgency: low/medium/high/critical
    │    - confidence: 0.0-1.0
    │
    └──[Kafka]──▶ transcription.summary (key=callId, JSON)
            │
            │  Streams processing (enrichment-processor group):
            │  - broadcast enrichment with customers.profile (in-memory map)
            │  - добавление: segment, risk_level, priority
            │
            └─▶ transcription.enriched
```

### 4.3.6. `transcription.enriched` → Kafka Connect JDBC Sink + reporting-nps

```
transcription-analyzer (Streams)
    │
    │  enrichment:
    │  - broadcast customers.profile (in-memory map, key=phone)
    │  - lookup phone in customers.profile map for each summary event
    │  - добавление: segment, risk_level, priority
    │
    └──[Kafka]──▶ transcription.enriched (key=callId, JSON)
            │
            ├─▶ Kafka Connect JDBC Sink (primary writer)
            │       │
            │       └─▶ PostgreSQL (call_transcriptions)
            │
            └─▶ reporting-nps (Consumer → Analytics)
                    │
                    └─▶ (Read-only access to PostgreSQL)
```

### 4.3.7. `customers.profile` → transcription-analyzer (Broadcast Enrichment)

```
Bootstrap (CSV script / kafka-console-producer)
    │
    └──[Kafka]──▶ customers.profile (key=phone, JSON, compacted)
                    │
                    │  Compacted topic (last value per phone):
                    │  - phone (key)
                    │  - segment (premium, standard, corporate)
                    │  - risk_level (low, medium, high)
                    │
                    └─▶ transcription-analyzer (Broadcast enrichment)
                            │
                            │  broadcast strategy (not KTable join):
                            │  - customers.profile loaded into in-memory map on startup
                            │  - key mismatch: transcription.summary key=callId, customers.profile key=phone
                            │  - each summary event enriched via broadcast lookup
                            │
                            └─ enrichment: segment + risk_level → priority → enriched event
```

### 4.3.8. `calls.dlq` → мониторинг ошибок

```
all services (error handlers)
    │
    │  ошибки:
    │  - deserialization failure
    │  - validation error
    │  - processing timeout
    │  - max retries exceeded
    │
    └──[Kafka]──▶ calls.dlq (key=callId, JSON)
            │
            │  monitoring:
            │  - Grafana alert: dlq_rate > threshold
            │  - manual review via Kafdrop
            │  - automated reprocessing (future)
            │
            └─▶ reporting-nps (Consumer)
                    │
                    └─▶ PostgreSQL (error statistics)
```

## 4.4. ksqlDB Analytics Layer

### 4.4.1. Архитектура

```
calls.completed (Kafka topic)
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│                    ksqlDB Server                             │
│                    (port 8088, REST API)                     │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ External Stream: calls_completed_ext                  │  │
│  │ (callId, phone, duration, agentId, npsScore,          │  │
│  │  completedAt)                                         │  │
│  └──────────────────────┬───────────────────────────────┘  │
│                         │                                   │
│                         ▼                                   │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Persistent Table: calls_completed_agg                 │  │
│  │   - TUMBLING window: 5 MINUTES, GRACE: 1 MINUTE       │  │
│  │   - GROUP BY: agentId                                 │  │
│  │   - Metrics: call_count, avg_nps_score, last_call_at  │  │
│  └──────────────────────┬───────────────────────────────┘  │
│                         │                                   │
│                         ▼                                   │
│            calls.completed.agg (Kafka topic)                │
└─────────────────────────────────────────────────────────────┘
    │
    ├─▶ reporting-nps (optional: consume pre-aggregated data)
    └─▶ external analytics tools (via REST API)


calls.fraud-alerts (Kafka topic)
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│                    ksqlDB Server (continued)                 │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ External Stream: calls_fraud_alerts_ext                │  │
│  │ (phone, pattern, severity, count)                     │  │
│  └──────────────────────┬───────────────────────────────┘  │
│                         │                                   │
│                         ▼                                   │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Persistent Table: fraud_alerts_filtered                │  │
│  │   - GROUP BY: phone                                   │  │
│  │   - Metric: alert_count                               │  │
│  └──────────────────────┬───────────────────────────────┘  │
│                         │                                   │
│                         ▼                                   │
│            calls.fraud-alerts.agg (Kafka topic)             │
└─────────────────────────────────────────────────────────────┘
    │
    ├─▶ reporting-nps (optional: consume pre-aggregated data)
    └─▶ external analytics tools (via REST API)
```

### 4.4.2. Автоматическая инициализация

При запуске `docker compose --profile full up -d`:

1. **ksqldb-server** запускается и подключается к Kafka (PLAINTEXT, port 9092)
2. **ksqldb-init** (batch job) выполняет инициализацию:
   ```bash
   # Creates external streams
   CREATE STREAM calls_completed_ext ...
   CREATE STREAM calls_fraud_alerts_ext ...
   
   # Creates persistent aggregation tables
   CREATE TABLE calls_completed_agg ... AS
     SELECT agentId, COUNT(*) AS call_count,
            AVG(npsScore) AS avg_nps_score,
            MAX(completedAt) AS last_call_at
     FROM calls_completed_ext
     WINDOW TUMBLING (SIZE 5 MINUTES, GRACE PERIOD 1 MINUTE)
     GROUP BY agentId EMIT CHANGES;
   
   CREATE TABLE fraud_alerts_filtered ... AS
     SELECT phone, COUNT(*) AS alert_count
     FROM calls_fraud_alerts_ext
     GROUP BY phone EMIT CHANGES;
   ```

Скрипт: `infrastructure/kafka/ksqldb-init.sh`

### 4.4.3. REST API для ad-hoc запросов

ksqlDB предоставляет REST API на порту 8088:

```bash
# Get ksqlDB info
curl http://localhost:8088/info

# Show all streams
curl -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW STREAMS;"}'

# Show all tables
curl -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW TABLES;"}'

# Show active queries
curl -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW QUERIES;"}'

# Ad-hoc query (requires websocket endpoint)
# curl -X POST http://localhost:8088/ksql \
#   -H "Content-Type: application/vnd.ksql.v1+json" \
#   -d '{"ksql": "SELECT agentId, call_count FROM CALLS_COMPLETED_AGG ORDER BY call_count DESC LIMIT 10;"}'
```

### 4.4.4. Output Topics Schemas

**`calls.completed.agg`:**
| Field | Type | Description |
|-------|------|-------------|
| `agentId` | STRING | Agent identifier |
| `call_count` | BIGINT | Count of completed calls in window |
| `avg_nps_score` | DOUBLE | Average NPS score in window |
| `last_call_at` | LONG | Timestamp of last completed call |

**`calls.fraud-alerts.agg`:**
| Field | Type | Description |
|-------|------|-------------|
| `phone` | STRING | Phone number |
| `alert_count` | BIGINT | Count of fraud alerts in window |

### 4.4.5. Быстрая проверка и демонстрация

```bash
# Run quick verification script
./infrastructure/kafka/ksqldb-verify.sh

# This script will:
# 1. Check ksqlDB server status
# 2. Show external streams and persistent tables
# 3. Show active queries
# 4. Generate 5 test calls via call-processor API
# 5. Verify aggregation results
# 6. Display demo query examples
```

### 4.4.6. Data Flow Summary

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA FLOW SUMMARY                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  calls.completed ──▶ ksqlDB ──▶ calls_completed_agg ──▶     │
│                          │            │                      │
│                          │            └─▶ calls.completed.agg│
│                          │                     │             │
│                          │                     ├─▶ reporting- │
│                          │                     │   nps        │
│                          │                     └─▶ external   │
│                          │                           tools    │
│                                                              │
│  calls.fraud-alerts ──▶ ksqlDB ──▶ fraud_alerts_filtered ──▶│
│                              │            │                   │
│                              │            └─▶ calls.fraud-   │
│                              │               alerts.agg       │
│                              │                     │         │
│                              │                     ├─▶       │
│                              │                     │         │
│                              └─────────────────────┘         │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```
