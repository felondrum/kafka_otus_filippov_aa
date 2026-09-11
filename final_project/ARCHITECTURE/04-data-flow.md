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
│  Avro               │                                  │   callId)           │
└────┬────────────────┘                                  └────────┬────────────┘
     │                                                             │
     │  fraud-detector                                            │  reporting-nps
     │  (Streams)                                                 │  (Consumer)
     ▼                                                             ▼
┌─────────────────────┐                                  ┌─────────────────────┐
│  calls.fraud-alerts │                                  │  PostgreSQL         │
│  (Stream, phone)    │                                  │  call_metadata      │
│  Avro               │                                  └─────────────────────┘
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
                                                          └───────┬─────────────┘
                                                                  │
                                              ┌───────────────────┼───────────────────┐
                                              │                       │                   │
                                              ▼                       ▼                   ▼
                                    ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
                                    │transcription.raw│   │transcription.   │   │customers.profile│
                                    │(Stream, callId) │   │summary          │   │(Compacted, phone) │
                                    │Avro              │   │(Stream, callId) │   │Avro              │
                                    └─────────────────┘   └─────────────────┘   └─────────────────┘
                                              │                       │
                                              └───────────┬───────────┘
                                                          │
                                                          ▼
                                                ┌─────────────────────┐
                                                │transcription.       │
                                                │enriched              │
                                                │(Stream, callId)      │
                                                │Avro                  │
                                                └───────────┬───────────┘
                                                            │
                                            ┌───────────────┼───────────────┐
                                            │               │               │
                                            ▼               ▼               ▼
                                   ┌────────────────┐  ┌──────────────┐  ┌──────────────┐
                                   │reporting-nps    │  │PostgreSQL    │  │calls.dlq     │
                                   │(Consumer)       │  │call_         │  │(Stream,      │
                                   │(aggregations)   │  │transcriptions│  │ callId)      │
                                   └────────────────┘  │               │  │JSON          │
                                                       └──────────────┘  └──────────────┘
```

## 4.2. Таблица топиков Kafka

| Топик | Тип | Ключ | Формат | Репликация | Партиции | Описание |
|-------|-----|------|--------|------------|----------|----------|
| `calls.completed` | Stream | `callId` | Avro | 3 | 6 | Событие о завершённом звонке |
| `calls.metadata` | Compacted Table | `callId` | Avro | 3 | 6 | Метаинформация звонка (жизненный цикл статусов) |
| `calls.fraud-alerts` | Stream | `phone` | Avro | 3 | 6 | Алерты антифрод-модуля |
| `transcription.raw` | Stream | `callId` | Avro | 3 | 6 | Сырая транскрипция диалога (симуляция) |
| `transcription.summary` | Stream | `callId` | Avro | 3 | 6 | Суммаризация от LLM (проблема, решение, сентимент) |
| `transcription.enriched` | Stream | `callId` | Avro | 3 | 6 | Обогащённая суммаризация с профилем клиента |
| `calls.dlq` | Stream | `callId` | JSON | 3 | 6 | Dead Letter Queue для битых сообщений |
| `customers.profile` | Compacted Table | `phone` | Avro | 3 | 6 | Профили клиентов (сегмент, риск-уровень) |

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
    ├──[Kafka]──▶ calls.completed (key=callId, Avro)
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
    └──[Kafka]──▶ calls.metadata (key=callId, Avro, compacted)
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
            │  статусы: PENDING → TRANSCRIBING → SUMMARIZING → COMPLETED
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
    └──[Kafka]──▶ calls.fraud-alerts (key=phone, Avro)
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
    └──[Kafka]──▶ transcription.raw (key=callId, Avro)
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
    └──[Kafka]──▶ transcription.summary (key=callId, Avro)
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
    └──[Kafka]──▶ transcription.enriched (key=callId, Avro)
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
    └──[Kafka]──▶ customers.profile (key=phone, Avro, compacted)
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
