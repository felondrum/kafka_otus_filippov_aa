# 3. Container Architecture (C4 Level 2)

## 3.1. Описание

На уровне контейнеров система состоит из 4 микросервисов, брокера Kafka (3 узла, KRaft), PostgreSQL, Schema Registry и инструментов мониторинга.

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
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## 3.3. Контейнеры

### Микросервисы

| Контейнер | Технология | Роль | Порт |
|-----------|-----------|------|------|
| **call-processor** | Spring Boot 3, Java 21 | REST API (POST /api/calls), Kafka Producer, Topic Manager | 8081 |
| **fraud-detector** | Spring Boot 3, Kafka Streams | Потоковая обработка calls.completed, детекция фрода | 8082 |
| **transcription-analyzer** | Spring Boot 3, Kafka Streams | Producer (raw transcription), Streams (enrichment), Consumer (JDBC) | 8083 |
| **reporting-nps** | Spring Boot 3, Spring Data JPA | Consumer, REST API для отчётов, CQRS read side | 8084 |

### Брокер и инфраструктура

| Контейнер | Образ | Роль | Порт |
|-----------|-------|------|------|
| **Kafka 1** | `confluentinc/cp-kafka:latest` | Брокер Kafka (KRaft, controller) | 9092, 9093 |
| **Kafka 2** | `confluentinc/cp-kafka:latest` | Брокер Kafka (controller) | 9092, 9093 |
| **Kafka 3** | `confluentinc/cp-kafka:latest` | Брокер Kafka | 9092, 9093 |
| **Schema Registry** | `confluentinc/cp-schema-registry:latest` | Хранение Avro-схем | 8085 |
| **PostgreSQL** | `postgres:15-alpine` | Аналитическое хранилище | 5432 |
| **Prometheus** | `prom/prometheus:latest` | Сбор метрик | 9090 |
| **Grafana** | `grafana/grafana:latest` | Визуализация | 3000 |
| **Kafdrop** | `obsidiandynamics/kafdrop:latest` | UI для Kafka | 9000 |
| **ksqlDB** | `confluentinc/cp-ksqldb-server:latest` | SQL-обработка потоков + REST API | 8088 |

## 3.4. Связи между контейнерами

```
call-processor ──[HTTP]──▶ Swagger UI (документация API)
call-processor ──[Kafka SASL]──▶ Kafka Cluster
call-processor ──[HTTP]──▶ Schema Registry (сериализация схем)

fraud-detector ──[Kafka SASL]──▶ Kafka Cluster
fraud-detector ──[RocksDB]──▶ локальный volume (state store)

transcription-analyzer ──[Kafka SASL]──▶ Kafka Cluster
transcription-analyzer ──[JDBC]──▶ PostgreSQL

reporting-nps ──[Kafka SASL]──▶ Kafka Cluster
reporting-nps ──[JDBC]──▶ PostgreSQL

Prometheus ──[HTTP /actuator/prometheus]──▶ все сервисы + Kafka
Grafana ──[HTTP API]──▶ Prometheus

Kafdrop ──[Kafka API]──▶ Kafka Cluster

ksqlDB ──[Kafka SASL]──▶ Kafka Cluster
ksqlDB ──[HTTP]──▶ Schema Registry (Avro schema resolution)
```

## 3.5. Сетевая модель

- **Docker Network:** `call-platform-net` (bridge)
- Все контейнеры подключены к единой сети
- Порты сервисов (8081-8084) доступны только внутри сети
- Для локального доступа: маппинг портов на `localhost` через docker-compose
