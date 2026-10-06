# Architecture & Data Flow Diagrams

> **Source of truth:** actual source code (`src/main/java/`), `docker-compose.yml`, `init-topics.sh`.
> All topic names, ports, endpoints, and data flows verified against implementation.

---

## 1. Container Architecture (C4 Level 2)

```mermaid
graph TB
    subgraph Input["Входные данные"]
        LS["Load Simulator\n:8087\nPOST /api/load/start\nfire-and-forget HttpClient"]
        DEV["Developer / Test\nPOST /api/calls directly"]
    end

    subgraph Services["Микросервисы (Spring Boot 3, Java 21)"]
        CP["call-processor\n:8081\nREST + Kafka Producer\nValidation + Topic Manager\nidempotent, acks=all, LZ4"]
        FD["fraud-detector\n:8082\nKafka Streams ONLY\n(no REST API)\nat_least_once\nRocksDB state"]
        TA["transcription-analyzer\n:8083\nConsumer + Producer + Streams\nKafka-first write + DLQ fallback\nSynthetic transcription\nKeyword-based summary"]
        RN["reporting-nps\n:8084\n4 Kafka consumers\nREST API + CQRS\nConcurrentMap cache\nPostgreSQL JPA"]
    end

    subgraph KafkaLayer["Kafka Cluster (KRaft, 3 brokers)"]
        KAFKA["SASL_PLAINTEXT external\nPLAINTEXT internal :9092\n3 brokers, replication=3\npartitions=6"]
    end

    subgraph Infra["Инфраструктура"]
        SR["Schema Registry\n:8085\nJSON schemas\nConfluent format"]
        KC["Kafka Connect\n:8086\nJDBC Sink connector"]
        PG["PostgreSQL 15-alpine\n:5432\n3 tables + 1 view"]
        KSQL["ksqlDB Server\n:8088\nStreams + Tables\n5min tumbling windows"]
        KSQLI["ksqlDB-init\nbatch job\nauto-creates streams/tables"]
        PROM["Prometheus\n:9090\nscrape /actuator/prometheus"]
        GRAF["Grafana\n:3000\nDashboards + Alerts"]
        KAFD["Kafdrop\n:9000\nKafka UI"]
        KEXP["kafka-exporter\n:9308\nPrometheus metrics"]
        PGEX["postgres-exporter\n:9187\nPostgreSQL metrics"]
    end

    LS -->|HTTP POST| CP
    DEV -->|HTTP POST| CP

    CP -->|PLAINTEXT, JSON String/String| KAFKA
    FD -->|PLAINTEXT, StringSerde| KAFKA
    TA -->|PLAINTEXT, JSON| KAFKA
    RN -->|PLAINTEXT, JSON| KAFKA

    KC -->|JDBC| PG

    RN -->|JPA| PG

    KSQL -->|PLAINTEXT:9092| KAFKA
    KSQLI -->|HTTP REST| KSQL
    SR -->|HTTP| KAFKA

    PROM -->|HTTP scrape| CP
    PROM -->|HTTP scrape| FD
    PROM -->|HTTP scrape| TA
    PROM -->|HTTP scrape| RN
    PROM -->|HTTP scrape| KAFKA
    PROM -->|HTTP| KEXP
    PROM -->|HTTP| PGEX
    GRAF -->|API| PROM
    KAFD -->|API| KAFKA

    style Input fill:#e8f5e9
    style Services fill:#e1f5fe
    style KafkaLayer fill:#fff3e0
    style Infra fill:#f3e5f5
```

### Profiles (docker-compose.yml)

| Profile | What's included |
|---------|----------------|
| **`full`** | ALL: Kafka cluster + 5 services + PostgreSQL + Schema Registry + Kafka Connect + ksqlDB + Prometheus + Grafana + Kafdrop + exporters |
| **`core`** | Kafka + 4 services (no load-simulator) + PostgreSQL + Schema Registry + Kafka Connect + ksqlDB |
| **`load-test`** | Kafka + PostgreSQL + load-simulator + monitoring stack |
| **`monitoring`** | Prometheus + Grafana + Kafdrop + kafka-exporter + postgres-exporter |

---

## 2. Data Flow

```mermaid
flowchart LR
    LS["load-simulator :8087\nPOST /api/load/start"]
    DIRECT["Direct POST\n/api/calls"]

    CP["call-processor :8081\nPOST /api/calls\nvalidate + produce\nacks=all, idempotent"]

    TC["calls.completed\nkey=callId, JSON"]
    TM["calls.metadata\nkey=callId, compacted"]
    CFA["calls.fraud-alerts\nkey=phone, JSON"]
    TR["transcription.raw\nkey=callId, JSON"]
    TS["transcription.summary\nkey=callId, JSON"]
    TE["transcription.enriched\nkey=callId, Confluent JSON"]
    DLQ["calls.dlq\nkey=callId, JSON"]
    EDLQ["enriched.dlq\nkey=callId, JSON"]
    CPROF["customers.profile\nkey=phone, compacted"]
    CP_AGG["calls.completed.agg\nkey=agentId, ksqlDB"]
    FA_AGG["calls.fraud-alerts.agg\nkey=phone, ksqlDB"]

    FD["fraud-detector :8082\nKafka Streams\nno REST API\n\n3 patterns:\n  FrequentCalls\n  NpsEscalation\n  AnomalousDuration"]

    TA["transcription-analyzer :8083\nConsumer + Producer\n\n1. synthetic text\n2. keyword summary\n3. broadcast enrich\n4. Kafka-first write + DLQ"]

    RN["reporting-nps :8084\n4 consumers\n\nREST API:\n  /reports/daily\n  /reports/agent/{id}\n  /sentiment/distribution\n  /metadata/{callId}"]

    KSQL["ksqlDB :8088\n5min tumbling\nagg + filtered"]

    PG["PostgreSQL :5432\n3 tables + 1 view\n\ncall_metadata\ncall_transcriptions\nfraud_stats"]

    KC["Kafka Connect :8086\nJDBC Sink"]

    LS -->|HTTP POST| CP
    DIRECT --> CP
    CP --> TC
    CP --> TM
    TC --> FD
    TC --> TA
    FD --> CFA
    TA --> TR
    TA --> TS
    TA --> TE
    CPROF -.->|cache| TA
    CFA --> RN
    TM --> RN
    TE --> KC
    TE --> RN
    KC --> PG
    RN --> PG
    CP_AGG -.-> RN
    FA_AGG -.-> RN
    CP -.->|DLQ| DLQ
    TA -.->|DLQ| DLQ
    TA -.->|DLQ| EDLQ
    RN -.->|DLQ| DLQ

    style LS fill:#e8f5e9
    style DIRECT fill:#e8f5e9
    style CP fill:#e1f5fe
    style FD fill:#ffebee
    style TA fill:#fff8e1
    style RN fill:#f3e5f5
    style PG fill:#fce4ec
    style KC fill:#e0f2f1
    style DLQ fill:#ffebee
    style EDLQ fill:#ffebee
```

---

## 3. Topic Reference (verified from `init-topics.sh`)

| Topic | Type | Key | Format | Replication | Partitions | Retention | Producer | Consumer(s) |
|-------|------|-----|--------|-------------|------------|-----------|----------|-------------|
| `calls.completed` | Stream | `callId` | JSON String/String | 3 | 6 | 7 days | call-processor | fraud-detector, transcription-analyzer |
| `calls.metadata` | Compacted Table | `callId` | JSON String/String | 3 | 6 | 30 days | call-processor | reporting-nps |
| `calls.fraud-alerts` | Stream | `phone` | JSON String/String | 3 | 6 | default | fraud-detector | reporting-nps |
| `transcription.raw` | Stream | `callId` | JSON String/String | 3 | 6 | default | transcription-analyzer | summary-processor (internal) |
| `transcription.summary` | Stream | `callId` | JSON String/String | 3 | 6 | default | transcription-analyzer | enrichment-processor (internal) |
| `transcription.enriched` | Stream | `callId` | Confluent JSON | 3 | 6 | default | transcription-analyzer | reporting-nps, Kafka Connect |
| `calls.dlq` | Stream | `callId` | JSON String/String | 3 | 6 | 30 days | all services | — |
| `transcription.enriched.dlq` | Stream | `callId` | JSON String/String | 3 | 3 | default | transcription-analyzer | — |
| `customers.profile` | Compacted Table | `phone` | JSON String/String | 3 | 6 | default | bootstrap script | transcription-analyzer |
| `calls.completed.agg` | Stream | `agentId` | JSON String/String | 3 | 6 | default | ksqlDB | reporting-nps (optional) |
| `calls.fraud-alerts.agg` | Stream | `phone` | JSON String/String | 3 | 6 | default | ksqlDB | reporting-nps (optional) |

---

## 4. Key Implementation Details (from source code)

### Serialization
- **All topics:** `StringSerializer` / `StringDeserializer` (JSON payload as string)
- **transcription.enriched:** Confluent JSON format (`{"schema": {...}, "payload": {...}}`) — resolved via Schema Registry

### Fraud Detection Patterns (fraud-detector)
| Pattern | Logic | Threshold | Severity |
|---------|-------|-----------|----------|
| FREQUENT_CALLS | Hopping window 60s/10s, count by phone | >5 calls/window | MEDIUM |
| NPS_ESCALATION | Processor API + RocksDB KeyValueStore, 3x NPS<2 in 24h | 3 negative NPS / 24h | HIGH |
| ANOMALOUS_DURATION | Filter: duration > 300s | >300 seconds | LOW |

### Transcription Generation (transcription-analyzer)
- **No LLM** — keyword-based heuristic via `SummaryGenerator.java`
- Synthetic text: ~150 words/min, configurable
- Keyword mapping: `fraud/unauthorized/stolen` → problem=fraud_suspected, urgency=critical
- Broadcast enrichment: `CustomerProfileConsumer` + `ConcurrentHashMap` (key mismatch: summary key=callId, profile key=phone → cannot use KTable join)

### Dual Writer (transcription-analyzer)
1. Write to Kafka (`transcription.enriched`) in Confluent JSON format — primary output
2. On Kafka failure → write to `transcription.enriched.dlq` (DLQ fallback)
3. PostgreSQL writes handled exclusively by Kafka Connect JDBC Sink (not direct JDBC from service)

### PostgreSQL Schema (from `init-db.sql`)
| Table | Columns | Key Fields |
|-------|---------|------------|
| `call_metadata` | 21 | call_id PK, customer_phone, agent_id, call_duration, call_status, segment, risk_level, priority, sentiment... |
| `call_transcriptions` | 20 | transcription_id PK, call_id FK UNIQUE, transcription_text, problem, solution, sentiment, urgency, confidence... |
| `fraud_stats` | 9 | phone, call_id, pattern, severity, count, last_alert_at, agent_id |
| `v_full_call_info` | view | LEFT JOIN call_metadata + call_transcriptions |
