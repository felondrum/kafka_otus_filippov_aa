## Context

Проект — событийно-ориентированная платформа обработки звонков банковского колл-центра. Текущее состояние: репозиторий пустой, архитектура описана в `ARCHITECTURE/`. Change `infrastructure` развёртывает Kafka-кластер и Schema Registry. Следующий шаг — реализация call-processor как входной точки платформы.

## Goals / Non-Goals

**Goals:**
- Spring Boot 3 микросервис на Java 21, сборка через Gradle (Kotlin DSL)
- REST API: POST /api/calls, GET /api/health
- Kafka Producer с idempotent delivery (enable.idempotence=true, acks=all)
- Topic Manager: автоматическое создание топиков при старте
- Schema Registry integration для Avro-сериализации
- Валидация входных данных (phone, duration, agentId, npsScore)
- DLQ для битых сообщений
- Retry с exponential backoff (3 попытки)
- Correlation ID для трейсинга
- Unit-тесты (JUnit 5, Mockito, Embedded Kafka)
- Integration-тесты (Testcontainers)
- Docker build и deployment

**Non-Goals:**
- Обработка событий от Kafka (это делают fraud-detector, transcription-analyzer, reporting-nps)
- Аутентификация/авторизация клиентов REST API (вне скоупа для MVP)
- Batch processing (только single event per request)
- Async response / WebFlux (используем synchronous Spring MVC)
- OpenAPI / Swagger документация (упоминается в draft.md, но не в приоритете)

## Decisions

### Decision 1: Gradle (Kotlin DSL) vs Maven

**Выбор:** Gradle с Kotlin DSL (build.gradle.kts).

**Альтернативы:**
- Maven (pom.xml) — проще для новичков, но Gradle быстрее
- Gradle с Groovy DSL — менее типобезопасен, Kotlin DSL предпочтительнее

**Обоснование:**
- Gradle быстрее Maven, особенно incremental builds
- Kotlin DSL — type-safe, better IDE support, less boilerplate
- Spring Boot 3 + Java 21 отлично работает с Gradle
- pluginManagement и version catalogs для управления зависимостями

### Decision 2: Embedded Kafka vs Testcontainers для тестов

**Выбор:** Embedded Kafka для unit-тестов, Testcontainers для integration-тестов.

**Альтернативы:**
- Только Testcontainers — медленнее, но ближе к production
- Только Embedded Kafka — быстрее, но не проверяет реальное поведение Kafka

**Обоснование:**
- Unit-тесты: Embedded Kafka быстрый (msec), покрывает 70%+ логики
- Integration-тесты: Testcontainers (Kafka 7.6.1) проверяет реальное взаимодействие
- Соответствует `ARCHITECTURE/11-unit-testing.md` и `12-integration-testing.md`

### Decision 3: Avro vs JSON для Kafka events

**Выбор:** Avro для calls.completed и calls.metadata, JSON для calls.dlq.

**Альтернативы:**
- JSON everywhere — проще, но нет schema validation
- Avro everywhere — лучше типобезопасность, но сложнее для DLQ

**Обоснование:**
- Архитектура требует Avro (см. `ARCHITECTURE/01-overview.md`, Schema Evolution)
- DLQ использует JSON для максимальной совместимости (битые сообщения могут не иметь Avro-схемы)
- Schema Registry обеспечивает backward compatibility

### Decision 4: Topic Manager — встроенный vs отдельный скрипт

**Выбор:** Встроенный Topic Manager в call-processor (при старте через Admin API).

**Альтернативы:**
- Отдельный скрипт (bash/Python) — требует отдельного шага в CI/CD
- Kafka CLI (kafka-topics.sh) — ручной запуск, легко забыть

**Обоснование:**
- Topic Manager запускается автоматически при старте сервиса
- Не требует отдельного шага в docker-compose
- Проверяет существование топиков перед созданием (безопасно при повторных запусках)
- Соответствует `ARCHITECTURE/05-component-diagrams.md`

### Decision 5: Retry strategy

**Выбор:** 3 попытки с exponential backoff (1s, 2s, 4s).

**Альтернативы:**
- Фиксированный interval (3x 1s) — проще, но менее эффективно при перегрузке
- Infinite retry — риск бесконечного цикла, нужно ограничивать

**Обоснование:**
- 3 попытки — баланс между надёжностью и скоростью обнаружения ошибок
- Exponential backoff снижает нагрузку при временных сбоях
- После 3 попыток — DLQ для ручного разбора
- Соответствует `ARCHITECTURE/01-overview.md` (Resilience by Design)

### Decision 6: Producer throughput properties

**Выбор:** batch.size=16384, linger.ms=5, compression.type=lz4.

**Альтернативы:**
- Значения по умолчанию Spring Kafka — проще, но ниже throughput
- Агрессивные значения (batch.size=65536, linger.ms=50) — выше throughput, но больше latency

**Обоснование:**
- batch.size=16384 (16KB) — баланс между размером batch и memory usage
- linger.ms=5 — небольшая задержка для накопления batch без заметного увеличения latency
- compression.type=lz4 — низкая CPU overhead, хорошее сжатие для Avro-сообщений
- Обеспечивает >1000 events/sec без перегрузки producer
- Соответствует секции Risks/Trade-offs в design.md

### Decision 7: Correlation ID generation approach

**Выбор:** UUID v4 (случайный UUID) для каждого запроса.

**Альтернативы:**
- UUID v7 (time-ordered) — сортируемые UUID, полезны для индексации в Kafka и логах
- Custom ID (callId-timestamp) — человекочитаемый формат, но не уникален автоматически

**Обоснование:**
- UUID v4 — стандарт для correlation ID в Spring Boot (java.util.UUID.randomUUID())
- Полная уникальность без координации между инстансами
- Для MVP time-ordered UUID не нужен (нет распределённой генерации)
- В будущем при horizontal scaling можно перейти на UUID v7 для лучшего порядка в Kafka
- Correlation ID включается в лог (task 10.1), ответ 202 (task 3.3) и метрики (task 10.2)

## Risks / Trade-offs

| Risk | Mitigation |
|------|-----------|
| Topic Manager блокирует старт при недоступности Kafka | Health check возвращает 503; startup timeout 30s; service starts даже если топики не созданы (проверяются async) |
| Schema Registry недоступен при старте | Retry на уровне Spring Boot (spring-retry); fallback to JSON с логированием ошибки |
| Валидация отклоняет легитимные данные | Comprehensive test coverage; clear error messages в ответе 400 |
| DLQ переполняется при системных ошибках | Monitoring: Grafana alert на DLQ rate > threshold; manual review через Kafdrop |
| High throughput (>1000 events/sec) может перегрузить producer | spring.kafka.producer.properties.batch.size=16384; linger.ms=5; compression.type=lz4 |

## Migration Plan

**Шаги развёртывания:**
1. Убедиться, что infrastructure запущена (`make deploy` из change/infrastructure)
2. Собрать JAR: `cd call-processor && ./gradlew clean build`
3. Запустить сервис: `docker compose up call-processor`
4. Проверить health: `curl http://localhost:8081/api/health`
5. Проверить топики в Kafdrop: http://localhost:9000

**Rollback:**
- `docker compose down call-processor`
- Вернуть предыдущую версию JAR (если есть)

## Open Questions

- **M-1: Compaction rationale для calls.metadata** — calls.metadata использует compaction, так как metadata обновляется downstream-сервисами (transcription-analyzer, fraud-detector) в процессе lifecycle звонка (PENDING → TRANSCRIBING → COMPLETED). Compaction гарантирует, что consumer (reporting-nps) всегда читает актуальный статус. Rationale задокументировано в spec.md.
- **M-2: npsScore range 0-10** — осознанный выбор для MVP (0 = полная неудовлетворённость, 10 = полная удовлетворённость). В отличие от классического NPS (0-9 или -5..+5), диапазон 0-10 проще для восприятия российскими операторами колл-центра.
- **L-2: Avro schema** — единая схема CallEvent используется для обоих topics (calls.completed и calls.metadata). calls.metadata добавляет дополнительное поле `status` (PENDING/TRANSCRIBING/COMPLETED), совместимое BACKWARD с основной схемой.
- **C-1: Schema Registry port** — изменён на 8085 для избежания конфликта с call-processor (8081). Исправлено в infrastructure change.
