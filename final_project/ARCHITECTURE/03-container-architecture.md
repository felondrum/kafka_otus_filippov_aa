# 3. Container Architecture (C4 Level 2)

## 3.1. Описание

На уровне контейнеров система состоит из 5 микросервисов, брокера Kafka (3 узла, KRaft), PostgreSQL, Schema Registry, Kafka Connect, ksqlDB и инструментов мониторинга.

## 3.2. ASCII-диаграмма

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    DOCKER NETWORK: call-platform-net                         │
│                                                                              │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐          │
│  │  call-processor │    │  fraud-detector │    │transcription-   │          │
│  │  (Spring Boot)  │    │  (Spring Boot)  │    │  analyzer       │          │
│  │                 │    │                 │    │  (Spring Boot)  │          │
│  │  REST API       │    │  Kafka Streams  │    │                 │          │
│  │  Kafka Producer │    │  Processor API  │    │  Producer       │          │
│  │  Topic Manager  │    │  State Store    │    │  Streams        │          │
│  │                 │    │  (RocksDB)      │    │  Consumer       │          │
│  └────────┬────────┘    └────────┬────────┘    │  JDBC Writer    │          │
│           │                     │             └────────┬────────┘          │
│           │                     │                      │                    │
│           ▼                     ▼                      ▼                    │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │                   APACHE KAFKA (KRaft, 3 brokers)               │        │
│  │                                                                 │        │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │        │
│  │  │ Kafka 1     │  │ Kafka 2     │  │ Kafka 3     │            │        │
│  │  │ :9092       │  │ :9092       │  │ :9092       │            │        │
│  │  │ SASL/PLAIN  │  │ SASL/PLAIN  │  │ SASL/PLAIN  │            │        │
│  │  └─────────────┘  └─────────────┘  └─────────────┘            │        │
│  │                                                                 │        │
│  │  Topics (replication=3, partitions=6):                          │        │
│  │  calls.completed, calls.metadata, calls.fraud-alerts            │        │
│  │  transcription.raw, transcription.summary, transcription.       │        │
│  │    enriched, calls.dlq, customers.profile                       │        │
│  └─────────────────────────────────────────────────────────────────┘        │
│           ▲                     ▲                      ▲                    │
│           │                     │                      │                    │
│  ┌────────┴────────┐    ┌────────┴────────┐    ┌────────┴────────┐        │
│  │  Schema         │    │  PostgreSQL     │    │  Prometheus     │        │
│  │  Registry       │    │  15 (ARM64)     │    │                 │        │
│  │  :8081          │    │  :5432          │    │  :9090          │        │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘        │
│                                                                              │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐        │
│  │  Grafana        │    │  Kafdrop        │    │  ksqlDB         │        │
│  │  :3000          │    │  :9000          │    │  :8088          │        │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘        │
│  ┌─────────────────┐                                                        │
│  │  ksqlDB-init    │                                                        │
│  │  (init script)  │                                                        │
│  └─────────────────┘                                                        │
│                                                                              │
│  ┌─────────────────┐                                                        │
│  │  Kafka Connect  │                                                        │
│  │  :8086          │                                                        │
│  └─────────────────┘                                                        │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## 3.3. Контейнеры

### Микросервисы

| Контейнер | Технология | Роль | Порт |
|-----------|-----------|------|------|
| **call-processor** | Spring Boot 3, Java 21 | REST API (POST /api/calls), Kafka Producer (idempotent), Topic Manager, Spring Retry (@Retryable) | 8081 |
| **fraud-detector** | Spring Boot 3, Kafka Streams | Потоковая обработка calls.completed, детекция фрода (3 pattern processor), at_least_once | 8082 |
| **transcription-analyzer** | Spring Boot 3, Kafka Consumer + Producer | Producer (raw transcription), Consumer (enriched), Kafka-first write + DLQ, synthetic transcription | 8083 |
| **reporting-nps** | Spring Boot 3, Spring Data JPA | 4 Kafka consumers, REST API для отчётов, CQRS read side, ConcurrentMapCacheManager | 8084 |
| **load-simulator** | Spring Boot 3 | Load generator, Factory pattern, 4 fraud scenarios (NORMAL, ANOMALOUS_DURATION, FREQUENT_CALLS, NPS_ESCALATION) | 8087 |

### Брокер и инфраструктура

| Контейнер | Образ | Роль | Порт |
|-----------|-------|------|------|
| **Kafka 1** | `confluentinc/cp-kafka:latest` | Брокер Kafka (KRaft, controller) | 9092, 9093 |
| **Kafka 2** | `confluentinc/cp-kafka:latest` | Брокер Kafka (controller) | 9092, 9093 |
| **Kafka 3** | `confluentinc/cp-kafka:latest` | Брокер Kafka | 9092, 9093 |
| **Schema Registry** | `confluentinc/cp-schema-registry:7.6.1` | Хранение JSON-схем | 8085 |
| **Kafka Connect** | custom build (Dockerfile в infrastructure/kafka-connect/) | JDBC Sink connector для PostgreSQL | 8086 |
| **PostgreSQL** | `postgres:15-alpine` | Аналитическое хранилище | 5432 |
| **Prometheus** | `prom/prometheus:latest` | Сбор метрик | 9090 |
| **Grafana** | `grafana/grafana:latest` | Визуализация | 3000 |
| **Kafdrop** | `obsidiandynamics/kafdrop:latest` | UI для Kafka | 9000 |
| **ksqlDB** | `confluentinc/cp-ksqldb-server:7.6.1` | SQL-обработка потоков + REST API | 8088 |
| **ksqlDB-init** | `confluentinc/cp-kafka:7.6.1` | Инициализация streams и tables (автоматический запуск) | — |

## 3.4. Связи между контейнерами

```
call-processor ──[HTTP]──▶ Swagger UI (документация API)
call-processor ──[Kafka SASL]──▶ Kafka Cluster
call-processor ──[HTTP]──▶ Schema Registry (сериализация схем)

fraud-detector ──[Kafka SASL]──▶ Kafka Cluster
fraud-detector ──[RocksDB]──▶ локальный volume (state store)

transcription-analyzer ──[Kafka SASL]──▶ Kafka Cluster

reporting-nps ──[Kafka SASL]──▶ Kafka Cluster
reporting-nps ──[JDBC]──▶ PostgreSQL

Prometheus ──[HTTP /actuator/prometheus]──▶ все сервисы + Kafka
Grafana ──[HTTP API]──▶ Prometheus

Kafdrop ──[Kafka API]──▶ Kafka Cluster

ksqlDB ──[Kafka PLAINTEXT]──▶ Kafka Cluster (internal:9092)
ksqlDB ──[HTTP]──▶ Schema Registry (JSON schema resolution)
ksqlDB-init ──[HTTP]──▶ ksqlDB (REST API) — creates streams & tables on startup

Kafka Connect ──[Kafka SASL]──▶ Kafka Cluster
Kafka Connect ──[HTTP]──▶ Schema Registry (JSON schema resolution)
Kafka Connect ──[JDBC]──▶ PostgreSQL
```

## 3.5. Автоматическая инициализация ksqlDB

При запуске `docker compose --profile full up -d` автоматически выполняется:

1. **ksqldb-server** запускается и ожидает подключения к Kafka
2. **ksqldb-init** (batch job) запускается после старта ksqldb-server:
   - Создаёт external streams для `calls.completed` и `calls.fraud-alerts`
   - Создаёт persistent tables для агрегации:
     - `calls_completed_agg` — TUMBLING window 5min, GROUP BY agentId
     - `fraud_alerts_filtered` — GROUP BY phone
   - Проверяет статус всех queries (должны быть RUNNING)

Скрипт инициализации: `infrastructure/kafka/ksqldb-init.sh`

## 3.6. Сетевая модель

- **Docker Network:** `call-platform-net` (bridge)
- Все контейнеры подключены к единой сети
- Порты сервисов (8081-8084, 8088) доступны только внутри сети
- Для локального доступа: маппинг портов на `localhost` через docker-compose
