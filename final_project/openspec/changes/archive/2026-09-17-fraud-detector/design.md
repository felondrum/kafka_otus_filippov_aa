## Context

Проект — событийно-ориентированная платформа обработки звонков банковского колл-центра. Текущее состояние: репозиторий пустой, архитектура описана в `ARCHITECTURE/`. Change `infrastructure` развёртывает Kafka-кластер, change `call-processor` реализует входную точку. Следующий шаг — fraud-detector: Kafka Streams микросервис для детекции мошеннических паттернов.

## Goals / Non-Goals

**Goals:**
- Spring Boot 3 микросервис на Java 21, сборка через Gradle (Kotlin DSL)
- Kafka Streams pipeline (DSL + Processor API)
- Hopping window (1 мин размер, 10 сек advance): подсчёт звонков с одного номера
- Processor API: детекция эскалации NPS (3x NPS < 2 за 24ч)
- RocksDB State Store с per-phone TTL 24ч (manual timestamp tracking)
- Output: calls.fraud-alerts (Avro via Schema Registry)
- Schema Registry integration для Avro сериализации
- Phone number normalization to E.164 before processing
- Exactly-once semantics
- Health check endpoint via Spring Boot Actuator
- Unit-тесты (JUnit 5, Mockito, Embedded Kafka)
- Integration-тесты (Testcontainers)
- Docker build и deployment

**Non-Goals:**
- Complex REST API (only minimal health endpoint via Actuator)
- Аутентификация/авторизация
- Batch processing
- LLM / ML модели (правила детекции, не ML)
- Real-time dashboard (это делает reporting-nps)

## Decisions

### Decision 1: Kafka Streams DSL vs Processor API

**Выбор:** DSL для hopping window (frequent calls), Processor API для NPS escalation.

**Альтернативы:**
- Только DSL — не поддерживает stateful processing с кастомной логикой
- Только Processor API — более verbose, сложнее для простых окон

**Обоснование:**
- DSL отлично подходит для GroupBy + Windowed Count (frequent calls)
- Processor API нужен для NPS escalation (сложная логика с счётчиками и TTL)
- Архитектура требует Processor API (см. `ARCHITECTURE/05-component-diagrams.md`)
- Команда знакомится с обоими подходами

### Decision 2: Hopping Window vs Tumbling Window

**Выбор:** Hopping window (1 мин размер, 10 сек advance).

**Альтернативы:**
- Tumbling window (1 мин, без перекрытия) — проще, но можно пропустить паттерн на границе окон
- Session window (gap-based) — не подходит для time-based частотного анализа

**Обоснование:**
- Hopping window перекрывает интервалы (10 сек advance), снижает риск пропуска паттерна на границе окна
- Шаг 10 сек — баланс между точностью и производительностью
- 5 звонков за 1 мин — быстрый паттерн, hopping window лучше ловит
- Kafka Streams DSL поддерживает hopping window через `.windowedBy(TimeWindows.ofSizeAndAdvance(...))`
- **Примечание:** в Kafka Streams нет "sliding window" — sliding window это математическая концепция, hopping window — ближайшая реализация в DSL

### Decision 3: RocksDB vs In-Memory State

**Выбор:** RocksDB (встроенное хранилище Kafka Streams).

**Альтернативы:**
- In-memory (ConcurrentHashMap) — быстро, но теряется при рестарте
- External (Redis) — персистентность, но добавляет зависимость

**Обоснование:**
- RocksDB — нативное хранилище Kafka Streams, не требует внешних зависимостей
- Персистентность на диске, восстановление после рестарта
- TTL cleanup встроен в Kafka Streams
- Соответствует `ARCHITECTURE/01-overview.md` (State Store RocksDB)

### Decision 4: Window size for NPS escalation

**Выбор:** 24 часа.

**Альтернативы:**
- 1 час — слишком коротко, можно пропустить эскалацию
- 7 дней — слишком долго, шумные алерты

**Обоснование:**
- NPS эскалация — дневной паттерн (клиент звонит несколько раз за день)
- 24 часа — естественный бизнес-цикл
- TTL 24ч автоматически чистит state store

### Decision 5: Fraud alert severity levels

**Выбор:** HIGH (NPS_ESCALATION), MEDIUM (FREQUENT_CALLS), LOW (ANOMALOUS_DURATION).

**Обоснование:**
- NPS escalation — самый критичный (клиент в конфликте, риск churn)
- Frequent calls — средний приоритет (возможный фрод или техническая проблема)
- Anomalous duration — низкий (может быть legitimate, но требует проверки)
- reporting-nps использует severity для приоритизации

## Risks / Trade-offs

| Risk | Mitigation |
|------|-----------|
| RocksDB занимает диск (~100MB для active phones) | tmpfs для /tmp/kafka-streams; TTL 24ч автоматически чистит; monitoring disk usage |
| Hopping window может создавать ложные алерты при legitimate spike | Threshold > 5 (например, 10) для production; Grafana alert tuning |
| NPS escalation требует точного знания phone number | phone number normalization to E.164 before processing; contract with call-processor guarantees E.164 format |
| Exactly-once semantics увеличивает latency | processing.guarantee=exactly_once_v2; benchmark latency; target < 30 sec |
| State store recovery при large state | RocksDB compression; state.dir на SSD; startup health check с timeout |

## Migration Plan

**Шаги развёртывания:**
1. Убедиться, что infrastructure и call-processor запущены
2. Собрать JAR: `cd fraud-detector && ./gradlew clean build`
3. Запустить сервис: `docker compose up fraud-detector`
4. Проверить топики в Kafdrop: http://localhost:9000
5. Отправить тестовый звонок через POST /api/calls
6. Проверить, что fraud-alerts topic содержит событие (при симуляции фрода)

**Rollback:**
- `docker compose down fraud-detector`
- Вернуть предыдущую версию JAR (если есть)

## Open Questions

- **O-1: Schema Registry client library** — confluent-kafka vs spring-kafka SchemaRegistrySerializer. Confluent library имеет более широкие возможности, но spring-kafka проще для интеграции. Решение будет принято на этапе реализации.
- **O-2: Anomalous duration detection placement** — where in the topology to branch off for >300 sec detection. Options: (a) after short call filter as parallel branch, (b) in the same stream with a separate KStream. Decision needed.
- **O-3: Per-key TTL implementation** — Kafka Streams RocksDB state store doesn't support per-key TTL natively. Need to implement manual timestamp tracking and cleanup. Approach: store `(count, lastActivityTimestamp)` as value, cleanup on each access + periodic purge task.
- **O-4: Health check depth** — Spring Boot Actuator provides basic health. Should we add Kafka Streams-specific health (e.g., stream lag, state store status)? MVP: basic Actuator only. Future: custom StreamsHealthIndicator.
