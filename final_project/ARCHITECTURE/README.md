# ARCHITECTURE — Архитектура платформы обработки звонков колл-центра

## Описание

Архитектурная документация событийно-ориентированной платформы для обработки звонков банковского колл-центра на базе Apache Kafka.

**Стек:** Java 21, Spring Boot 3, Apache Kafka (KRaft), PostgreSQL 15, Avro, Docker Compose.

**Уровень C4:** Container (Level 2).

## Структура документов

| № | Файл | Раздел | Описание |
|---|------|--------|----------|
| 1 | [01-overview.md](./01-overview.md) | Overview | Бизнес-цели, архитектурные принципы (EDA, CQRS, EDA) |
| 2 | [02-system-context.md](./02-system-context.md) | System Context (C4 Level 1) | Внешние акторы, системы, связи |
| 3 | [03-container-architecture.md](./03-container-architecture.md) | Container Architecture (C4 Level 2) | 4 микросервиса, Kafka Cluster, PostgreSQL, мониторинг |
| 4 | [04-data-flow.md](./04-data-flow.md) | Data Flow | Топики Kafka (8 шт), потоки данных, таблицы |
| 5 | [05-component-diagrams.md](./05-component-diagrams.md) | Component Diagrams | Детальные диаграммы по каждому сервису |
| 6 | [06-infrastructure.md](./06-infrastructure.md) | Infrastructure | Docker Compose, ресурсы, сеть, volumes |
| 7 | [07-security.md](./07-security.md) | Security | SASL/PLAIN, ACL, Schema Registry, Spring Security |
| 8 | [08-monitoring.md](./08-monitoring.md) | Monitoring & Observability | Prometheus, Grafana, JSON-логирование, Correlation ID |
| 9 | [09-cicd.md](./09-cicd.md) | CI/CD Pipeline | Makefile, bash-скрипты, quality gates |
| 10 | [10-deployment.md](./10-deployment.md) | Deployment Model | Local (docker-compose), profiles, scaling |
| 11 | [11-unit-testing.md](./11-unit-testing.md) | Unit Testing | JUnit 5, Mockito, Embedded Kafka |
| 12 | [12-integration-testing.md](./12-integration-testing.md) | Integration Testing | Testcontainers, Kafka, PostgreSQL |
| 13 | [13-load-testing.md](./13-load-testing.md) | Load Testing | k6, сценарии, критерии успеха |
| 14 | [14-chaos-testing.md](./14-chaos-testing.md) | Chaos Testing | Сценарии сбоев, восстановление |

## Быстрый навигатор

### Микросервисы

| Сервис | Порт | Описание |
|--------|------|----------|
| call-processor | 8081 | REST API, Kafka Producer, Topic Manager |
| fraud-detector | 8082 | Kafka Streams, детекция фрода |
| transcription-analyzer | 8083 | Producer + Streams + Consumer, dual-write |
| reporting-nps | 8084 | Consumer + REST API, CQRS read side |

### Топики Kafka (8 шт)

| Топик | Тип | Ключ |
|-------|-----|------|
| calls.completed | Stream | callId |
| calls.metadata | Compacted Table | callId |
| calls.fraud-alerts | Stream | phone |
| transcription.raw | Stream | callId |
| transcription.summary | Stream | callId |
| transcription.enriched | Stream | callId |
| calls.dlq | Stream | callId |
| customers.profile | Compacted Table | phone |

### Инструменты мониторинга

| Инструмент | Порт | Назначение |
|------------|------|------------|
| Prometheus | 9090 | Сбор метрик |
| Grafana | 3000 | Визуализация, алерты |
| Kafdrop | 9000 | UI для Kafka |

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
| Avro + Schema Registry | Типобезопасность, эволюция схем |
| Kafka Streams | Exactly-once, stateful processing, RocksDB |
| CQRS | Независимое масштабирование записи и чтения |
| Docker Compose | Единая среда разработки, ARM64 совместимость |
| SASL/PLAIN + ACL | Безопасность Kafka, разделение прав |

## Связанные документы

- [draft.md](draft/draft.md) — Описание проекта и бизнес-требования
- [load_and_chaos_tests.md](draft/load_and_chaos_tests.md) — Детали нагрузочного и хаос-тестирования
