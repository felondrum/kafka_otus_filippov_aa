# Call Platform — Kafka Infrastructure

Полный стек микросервисов на базе Apache Kafka в KRaft mode (без ZooKeeper).

## Архитектура

**5 микросервисов + инфраструктура:**

### Микросервисы

| Сервис | Port | Роль |
|--------|------|------|
| **call-processor** | 8081 | REST API entry point, Kafka Producer (idempotent), Topic Manager |
| **fraud-detector** | 8082 | Kafka Streams, детекция фрода (3 pattern: FrequentCalls, NpsEscalation, AnomalousDuration) |
| **transcription-analyzer** | 8083 | Synthetic transcription, keyword-based summary, Kafka-first write + DLQ |
| **reporting-nps** | 8084 | 4 Kafka consumers, REST API для отчётов, CQRS read side |
| **load-simulator** | 8087 | Генератор синтетической нагрузки, 4 fraud-сценария |

### Инфраструктура

| Компонент | Port | Описание |
|-----------|------|----------|
| **Kafka Cluster** | 9092-9097 | 3 брокера (KRaft, SASL/PLAIN EXTERNAL, PLAINTEXT INTERNAL) |
| **Schema Registry** | 8085 | JSON-схемы, BACKWARD compatibility |
| **PostgreSQL 15** | 5432 | Analytics DB: `call_metadata`, `call_transcriptions`, `fraud_stats` |
| **Kafka Connect** | 8086 | JDBC Sink connector → PostgreSQL |
| **ksqlDB** | 8088 | SQL-агрегация потоков (TUMBLING windows) |
| **Prometheus** | 9090 | Сбор метрик (JMX, JVM, Spring Boot Actuator) |
| **Grafana** | 3000 | 8 дашбордов, алерты (admin/admin-secret) |
| **Kafdrop** | 9000 | Kafka UI |
| **Kafka Exporter** | 9308 | Topic/partition metrics |
| **Postgres Exporter** | 9187 | PostgreSQL metrics |

### Data Flow (End-to-End)

```
[External Caller / Load Simulator]
              │
              │  POST /api/calls {callId, phone, duration, agentId, npsScore}
              ▼
     ┌─────────────────┐
     │  call-processor   │  ← REST API + Kafka Producer (idempotent)
     └────────┬────────┘
              │
              ├──► calls.completed ──┬──► fraud-detector ──► calls.fraud-alerts
              │                      │                      │
              │                      │                      ├──► reporting-nps ──► PostgreSQL
              │                      │
              │                      └──► transcription-analyzer
              │                               │
              │                               ├──► transcription.raw
              │                               ├──► transcription.summary
              │                               └──► transcription.enriched ──┬──► Kafka Connect
              │                                                               │   └──► PostgreSQL
              │                                                               │
              │                                                               └──► DLQ (transcription.enriched.dlq)
              │
              └──► calls.metadata ───────────────────────────────────────────┘
                                                                              │
                                                                              ▼
                                                                     reporting-nps ──► PostgreSQL
```

### Kafka Topics Reference

| Topic | Type | Key | Format | Producer | Consumer(s) |
|-------|------|-----|--------|----------|-------------|
| `calls.completed` | Stream | callId | JSON | call-processor | fraud-detector, transcription-analyzer |
| `calls.metadata` | Compacted | callId | JSON | call-processor | reporting-nps |
| `calls.fraud-alerts` | Stream | phone | JSON | fraud-detector | reporting-nps, ksqlDB |
| `transcription.raw` | Stream | callId | JSON | transcription-analyzer | — |
| `transcription.summary` | Stream | callId | JSON | transcription-analyzer | — |
| `transcription.enriched` | Stream | callId | Confluent JSON | transcription-analyzer | Kafka Connect, reporting-nps |
| `transcription.enriched.dlq` | Stream | callId | JSON | transcription-analyzer | — |
| `calls.dlq` | Stream | callId | JSON | all services | — |
| `customers.profile` | Compacted | phone | JSON | bootstrap script | transcription-analyzer |
| `calls.completed.agg` | Stream | agentId | JSON | ksqlDB | reporting-nps (optional) |
| `calls.fraud-alerts.agg` | Stream | phone | JSON | ksqlDB | reporting-nps (optional) |

## Быстрый старт

### 1. Развернуть весь стек
```bash
make deploy
```
Или с конкретным profile:
```bash
make PROFILE=core deploy    # Только Kafka + Schema Registry + PostgreSQL
make PROFILE=monitoring deploy  # Только Prometheus + Grafana + Kafdrop
make PROFILE=load-test deploy  # Полный стек для load testing
```

### 2. Проверить здоровье сервисов
```bash
make health
```

### 3. Инициализировать Kafka темы

Создаёт 10 тем:

**Stream topics** (partitions=6, replication=3):
- `calls.completed` — события о завершённых звонках (producer: call-processor)
- `calls.fraud-alerts` — алерты антифрода (producer: fraud-detector)
- `transcription.raw` — синтетические транскрипции (producer: transcription-analyzer)
- `transcription.summary` — суммаризации (producer: transcription-analyzer)
- `transcription.enriched` — обогащённые транскрипции, Confluent JSON (producer: transcription-analyzer)
- `calls.dlq` — dead letter queue (producer: all services)

**Compacted topics** (cleanup.policy=compact):
- `calls.metadata` — статус-лайфцикл звонков (producer: call-processor)
- `customers.profile` — профили клиентов (producer: bootstrap script)

**ksqlDB output topics** (partitions=6):
- `calls.completed.agg` — агрегация по агентам (5min TUMBLING window)
- `calls.fraud-alerts.agg` — агрегация фрода по телефонам

**DLQ topic** (partitions=3, retention=30 days):
- `transcription.enriched.dlq` — failed enriched events

### 4. Посмотреть логи
```bash
make logs
```

### 5. Остановить стек
```bash
make stop
```

### 6. Полная очистка (удалить volumes)
```bash
make clean
```

## Сервисы и порты

### Микросервисы

| Сервис | Port | REST API |
|--------|------|----------|
| call-processor | 8081 | `POST /api/calls`, `GET /api/health` |
| fraud-detector | 8082 | Нет REST API (Kafka Streams only) |
| transcription-analyzer | 8083 | `POST /api/calls/process`, `GET /api/health` |
| reporting-nps | 8084 | `GET /api/reports/daily`, `/reports/agent/{id}`, `/sentiment/distribution`, `/metadata/{callId}`, `/health` |
| load-simulator | 8087 | `POST /api/load/start`, `/api/load/stop`, `/api/load/status` |

### Инфраструктура

| Компонент | Port | URL |
|-----------|------|-----|
| Kafka (broker-1) | 9092 (INTERNAL), 9093 (EXTERNAL/SASL) | `localhost:9092` (internal), `localhost:9093` (external) |
| Kafka (broker-2) | 9094 (INTERNAL), 9095 (EXTERNAL) | `localhost:9094` |
| Kafka (broker-3) | 9096 (INTERNAL), 9097 (EXTERNAL) | `localhost:9096` |
| Schema Registry | 8085 | http://localhost:8085 |
| PostgreSQL | 5432 | `postgres:5432` (internal), `localhost:5432` (external) |
| Kafka Connect | 8086 | http://localhost:8086 |
| ksqlDB | 8088 | http://localhost:8088 |
| Prometheus | 9090 | http://localhost:9090 |
| Grafana | 3000 | http://localhost:3000 (admin/admin-secret) |
| Kafdrop | 9000 | http://localhost:9000 |
| Kafka Exporter | 9308 | http://localhost:9308 |
| Postgres Exporter | 9187 | http://localhost:9187 |

## SASL аутентификация

EXTERNAL listener использует SASL/PLAIN:
- **Username**: `admin`
- **Password**: `admin-secret`

Для подключения извне используйте port 9093 с SASL credentials.

## Grafana дашборды

- **Kafka Cluster Overview** — метрики брокеров, throughput, lag, under-replicated partitions, broker disk
- **PostgreSQL Overview** — подключения, запросы, размеры таблиц, индексы
- **call-processor** — HTTP requests, Kafka producer metrics, JVM, GC
- **fraud-detector** — JVM, CPU, HTTP, Process Memory, GC Pause Count
- **transcription-analyzer** — HTTP, Kafka consumer/producer, PostgreSQL queries
- **reporting-nps** — HTTP, Kafka consumer lag, PostgreSQL queries, cache hits
- **system-overview** — Cross-service summary, PostgreSQL metrics
- **kafka-connect-jdbc-sink** — Connector status, task status, failed count, DLQ events

Доступ: http://localhost:3000 (admin/admin-secret)

## Kafka Connect

Kafka Connect запущен в distributed mode на port 8086 (external).

Пат для плагинов: `/etc/kafka-connect/jars`

### JDBC Sink Connector

Автозаполняет PostgreSQL из Kafka:
- `transcription.enriched` → `call_transcriptions` (Confluent JSON format)
- Потребляет enriched транскрипции и пишет в PostgreSQL

Темы Connect:
- `connect-configs` (compact)
- `connect-offsets` (compact)
- `connect-status` (compact)

## Мониторинг

Prometheus собирает метрики с:
- **Kafka brokers** (JMX Exporter) — throughput, lag, under-replicated partitions, broker disk
- **PostgreSQL** (postgres_exporter) — подключения, запросы, размеры таблиц
- **Микросервисы** (Spring Boot Actuator + Micrometer):
  - call-processor:8081 — HTTP requests, Kafka producer metrics, JVM, GC
  - fraud-detector:8082 — JVM, CPU, HTTP, Process Memory, GC Pause Count
  - transcription-analyzer:8083 — HTTP, Kafka consumer/producer, PostgreSQL queries
  - reporting-nps:8084 — HTTP, Kafka consumer lag, PostgreSQL queries, cache hits
- **Infrastructure** — Kafka Exporter (9308), Postgres Exporter (9187)

Конфигурация: `infrastructure/monitoring/prometheus.yml`

## Development

### Пересоздать темы
```bash
make init-topics
```

### Проверить темы в Kafdrop
Открыть http://localhost:9000

### Посмотреть метрики в Prometheus
- http://localhost:9090
- Query: `up` (статус сервисов), `kafka_server_replicamanager_partitioncount` (количество партиций)

### Подключиться к PostgreSQL
```bash
docker exec -it postgres psql -U postgres -d call_platform
```

### Проверить Kafka Connect
```bash
curl http://localhost:8086/connectors
```

### Проверить ksqlDB
```bash
curl http://localhost:8088/queries
```

## Профили Docker Compose

| Profile | Services |
|---------|----------|
| **full** | Kafka + 5 микросервисов + PostgreSQL + Schema Registry + Kafka Connect + ksqlDB + Prometheus + Grafana + Kafdrop + Exporters |
| **core** | Kafka + 5 микросервисов + PostgreSQL + Schema Registry + Kafka Connect + ksqlDB |
| **monitoring** | Prometheus + Grafana + Kafdrop + Kafka Exporter + Postgres Exporter |
| **load-test** | Kafka + PostgreSQL + load-simulator + monitoring stack |

### Использование

```bash
# Полный стек (по умолчанию)
make deploy

# Только core
make PROFILE=core deploy

# Только мониторинг
make PROFILE=monitoring deploy

# Для нагрузочного тестирования
make PROFILE=load-test deploy
```
