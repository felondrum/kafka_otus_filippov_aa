# ARCHITECTURE — Архитектура платформы обработки звонков колл-центра

## Описание

Архитектурная документация событийно-ориентированной платформы для обработки звонков банковского колл-центра на базе Apache Kafka.

**Стек:** Java 21, Spring Boot 3.2.3, Apache Kafka 7.6.1 (KRaft, 3 брокера), PostgreSQL 15, JSON, Docker Compose.

**Уровень C4:** Container (Level 2).

## Структура документов

| № | Файл | Раздел | Описание |
|---|------|--------|----------|
| 1 | [01-overview.md](./01-overview.md) | Overview | Бизнес-цели, архитектурные принципы (EDA, CQRS, at_least_once, JSON, resilience) |
| 2 | [02-system-context.md](./02-system-context.md) | System Context (C4 Level 1) | Внешние акторы, системы, связи |
| 3 | [03-container-architecture.md](./03-container-architecture.md) | Container Architecture (C4 Level 2) | 4 микросервиса, Kafka Cluster, PostgreSQL, мониторинг |
| 4 | [04-data-flow.md](./04-data-flow.md) | Data Flow | Топики Kafka (8 шт), потоки данных, таблицы |
| 5 | [05-component-diagrams.md](./05-component-diagrams.md) | Component Diagrams | Детальные диаграммы по каждому сервису |
| 6 | [06-infrastructure.md](./06-infrastructure.md) | Infrastructure | Docker Compose, ресурсы, сеть, volumes |
| 7 | [07-security.md](./07-security.md) | Security | SASL/PLAIN, ACL, Schema Registry, Spring Security |
| 8 | [08-monitoring.md](./08-monitoring.md) | Monitoring & Observability | Prometheus, Grafana, JSON-логирование, Correlation ID |
| 9 | [09-cicd.md](./09-cicd.md) | CI/CD Pipeline | Makefile, Gradle, bash-скрипты, quality gates |
| 10 | [10-deployment.md](./10-deployment.md) | Deployment Model | Local (docker-compose), profiles, scaling |
| 11 | [11-unit-testing.md](./11-unit-testing.md) | Unit Testing | JUnit 5, Mockito, Embedded Kafka |
| 12 | [12-integration-testing.md](./12-integration-testing.md) | Integration Testing | Testcontainers, Kafka, PostgreSQL |
| 13 | [13-load-testing.md](./13-load-testing.md) | Load Testing | k6, сценарии, критерии успеха |
| 14 | [14-chaos-testing.md](./14-chaos-testing.md) | Chaos Testing | Сценарии сбоев, восстановление |

## Быстрый навигатор

### Микросервисы

| Сервис | Порт | Описание |
|--------|------|----------|
| call-processor | 8081 | REST API, Kafka Producer (idempotent), Topic Manager, Spring Retry |
| fraud-detector | 8082 | Kafka Streams (at_least_once), 3 fraud pattern processors, RocksDB State Store |
| transcription-analyzer | 8083 | Producer + Consumer + Kafka-first write (Kafka + DLQ, PG via Kafka Connect), synthetic transcription, keyword-based summary |
| reporting-nps | 8084 | 4 Kafka consumers, REST API, Caffeine-like cache (ConcurrentMapCacheManager), CQRS read side |
| load-simulator | 8087 | Load generator, Factory pattern, 4 fraud scenarios (NORMAL, ANOMALOUS_DURATION, FREQUENT_CALLS, NPS_ESCALATION) |

### Топики Kafka (10 шт)

| Топик | Тип | Ключ | Описание |
|-------|-----|------|----------|
| calls.completed | Stream | callId | Событие о завершённом звонке |
| calls.metadata | Compacted Table | callId | Метаинформация (статус-лайфцикл) |
| calls.fraud-alerts | Stream | phone | Алерты антифрод-модуля |
| transcription.raw | Stream | callId | Сырая транскрипция (синтетическая) |
| transcription.summary | Stream | callId | Суммаризация (keyword-based) |
| transcription.enriched | Stream | callId | Обогащённая суммаризация |
| transcription.enriched.dlq | Stream | callId | DLQ для enriched events |
| calls.dlq | Stream | callId | Dead Letter Queue |
| customers.profile | Compacted Table | phone | Профили клиентов (сегмент, риск) |
| calls.completed.agg / calls.fraud-alerts.agg | Stream | agentId / phone | ksqlDB output topics |

### Инструменты мониторинга

| Инструмент | Порт | Назначение |
|------------|------|------------|
| Prometheus | 9090 | Сбор метрик (JMX, JVM, Spring Boot Actuator) |
| Grafana | 3000 | Визуализация, 8 дашбордов, алерты |
| Kafdrop | 9000 | UI для Kafka |
| Kafka Exporter | 9308 | Topic/partition metrics |
| Postgres Exporter | 9187 | PostgreSQL metrics |

## Быстрый старт

```bash
# Полный пайплайн
make all

# Только развёртывание
make deploy

# Проверка здоровья
make health

# Логи
make logs

# Остановка
make down
```

## Тестирование

### Unit-тесты
```bash
make test-unit
```
- Покрытие > 70%
- JUnit 5, Mockito, Embedded Kafka
- Тесты всех public методов

### Integration-тесты
```bash
make test-integration
```
- Testcontainers (Kafka, PostgreSQL)
- End-to-end сценарии
- Покрытие ключевых сценариев > 50%

### Нагрузочное тестирование
```bash
make load-test-basic    # Базовая нагрузка (2 часа)
make load-test-peak     # Пиковая нагрузка (5 мин)
make load-test-soak     # Длительный тест (8 часов)
make load-test-errors   # Тест с ошибками (30 мин)
```
- k6 для HTTP нагрузки
- kcat для Kafka нагрузки
- Целевые показатели: p95 < 45 сек, lag < 2000

### Хаос-тестирование
```bash
make chaos-broker       # Отказ брокера Kafka
make chaos-service      # Отказ сервиса
make chaos-consumer     # Отказ потребителя
make chaos-network      # Сбой сети
make chaos-resources    # Ограничение ресурсов
make chaos-hard         # Hard Shutdown
```
- Отказ брокера: восстановление < 60 сек
- Отказ сервиса: восстановление < 10 сек
- Отказ потребителя: rebalance < 45 сек
- Потеря данных: 0%

## Ключевые архитектурные решения

| Решение | Обоснование |
|---------|-------------|
| Apache Kafka (KRaft) | Отказ от ZooKeeper, упрощение инфраструктуры |
| JSON + Schema Registry | Простота, Confluent JSON format, BACKWARD совместимость |
| Kafka Streams (at_least_once) | Stateful processing, RocksDB, восстановление из changelog |
| CQRS | Независимое масштабирование записи (Kafka) и чтения (PostgreSQL) |
| Docker Compose | Единая среда разработки, ARM64 совместимость |
| SASL/PLAIN (EXTERNAL listener) | Безопасность Kafka, разделение прав по ACL |
| Keyword-based summary | Отсутствие LLM — детерминированная логика на ключевых словах |
| Broadcast enrichment | In-memory ConcurrentMap для customers.profile (key mismatch: callId vs phone) |
| Kafka-first write (Kafka → PG via Kafka Connect) | Kafka как primary event source, PG заполняется через Kafka Connect JDBC Sink, DLQ при неудаче |
| Gradle (Kotlin DSL) | Build system для всех 5 подпроектов |

## Связанные документы

- [draft.md](draft/draft.md) — Описание проекта и бизнес-требования
- [load_and_chaos_tests.md](draft/load_and_chaos_tests.md) — Детали нагрузочного и хаос-тестирования
