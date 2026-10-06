# 1. Overview

## 1.1. Описание системы

Платформа для обработки звонков банковского колл-центра — событийно-ориентированная система реального времени, построенная на Apache Kafka.

**Технологический стек:** Java 21, Spring Boot 3.2.3, Apache Kafka 7.6.1 (KRaft), PostgreSQL 15, JSON, Docker Compose.

**Пакет кода:** `com.example.*` (4 микросервиса + инфраструктура).

**Бизнес-цели:**

- Обработка звонков в реальном времени с мгновенной реакцией на негативные сценарии.
- Автоматическая классификация проблематики обращений (потери карт, кредиты, жалобы).
- Выявление мошеннических паттернов (частые звонки, подозрительные сценарии).
- Автоматическая суммаризация диалогов для архивации и быстрого поиска.
- Менеджерские дашборды с актуальной статистикой (NPS, загрузка агентов, тональность).
- Полная история звонков с привязкой метаинформации к расшифровкам для аудита.

## 1.2. Архитектурные принципы

### Event-Driven Architecture (EDA)

- Все взаимодействия между сервисами происходят через события в Apache Kafka.
- Сервисы не знают друг о друге (choreography pattern).
- События — источник истины, состояние сервиса восстанавливается из логов.

### CQRS (Command Query Responsibility Segregation)

- **Записи:** команды (создание звонков, алерты) пишутся в Kafka.
- **Чтения:** запросы (отчёты, статусы) читаются из PostgreSQL.
- Разделение путей записи и чтения обеспечивает независимую масштабируемость.

### Exactly-Once Semantics

- **call-processor:** `enable.idempotence=true`, `acks=all` (idempotent producer).
- **fraud-detector:** `processing.guarantee=at_least_once` (State Store с RocksDB, пересоздание состояния из Kafka при rebalance).
- **reporting-nps:** manual acknowledgment (`AckMode.MANUAL`) — подтверждение после успешной записи в PostgreSQL.
- **transcription-analyzer:** dual-write с retry (3 попытки, exponential backoff) и отправкой в DLQ при неудаче.

### Schema Evolution

- Все события сериализованы в **JSON** (StringSerializer/StringDeserializer).
- Confluent Schema Registry хранит JSON-схемы с форматом Confluent JSON (`{"schema": {...}, "payload": {...}}`).
- Совместимость схем: `BACKWARD` — новые версии совместимы со старыми потребителями.
- Kafka Connect использует `JsonConverter` для чтения/записи JSON-данных в PostgreSQL.

### Resilience by Design

- Dead Letter Queue (DLQ) для битых сообщений (`calls.dlq`, `transcription.enriched.dlq`).
- Retry с exponential backoff: call-processor (@Retryable, 3 попытки, 1s→2s→4s), transcription-analyzer (3 попытки, 1s→2s→4s), reporting-nps (orphan retry, 3 попытки, 5s интервал).
- State Store (RocksDB) для быстрого восстановления состояния fraud-detector после перезапуска.
- Cyclic status lifecycle: PENDING → TRANSCRIBING → SUMMARIZING → COMPLETED.

## Service Port Mapping

| Service | Internal Port | External Port |
| :--- | :--- | :--- |
| call-processor | 8081 | 8081 |
| fraud-detector | 8082 | 8082 |
| transcription-analyzer | 8083 | 8083 |
| reporting-nps | 8084 | 8084 |
| load-simulator | 8087 | 8087 |
| Schema Registry | 8085 | 8085 |
| Kafka Connect | 8086 | 8086 |
| ksqlDB Server | 8088 | 8088 |
| Prometheus | 9090 | 9090 |
| Grafana | 3000 | 3000 |
| Kafdrop | 9000 | 9000 |
| Kafka Exporter | 9308 | 9308 |
| Postgres Exporter | 9187 | 9187 |
| PostgreSQL | 5432 | 5432 |
