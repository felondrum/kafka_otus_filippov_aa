## Context

fraud-detector change реализовал упрощённую версию с in-memory counters из-за API-ограничений Kafka Streams 3.6.1 (R-42). Schema Registry и Avro не использовались для output (R-45). State store отсутствует. Все 67 tasks помечены как completed с "functional equivalents".

Текущий state: service работает, health check pass, но:
- Нет persistence (ConcurrentHashMap)
- Нет DSL windowing (manual counter)
- Нет Avro output (pipe-delimited String)
- Нет short call filter
- Нет severity levels
- Тесты — smoke tests (assertNotNull)

## Goals / Non-Goals

**Goals:**
- DSL Hopping Window для frequent calls (1 min size, 10s advance)
- Processor API с StateStore для NPS escalation
- RocksDB state store (Kafka Streams built-in, state.dir config уже есть)
- Avro FraudAlert output через Schema Registry (schema already exists)
- Short call filter (< 5s)
- Severity levels (HIGH/MEDIUM/LOW)
- 5+ integration tests с EmbeddedKafka + TopologyTestDriver

**Non-Goals:**
- Изменение Avro schema (FraudAlert.java уже полный)
- Изменение Kafka topics (calls.completed → calls.fraud-alerts)
- REST API (только health endpoint)
- ML/LLM модели

## Decisions

### Decision 1: DSL Hopping Window для Frequent Calls

**Выбор:** `KStream.groupByKey()` → `.windowedBy(TimeWindows.ofSizeAndAdvance(60000, 10000))` → `.count(Materialized.as("frequent-calls-count"))`

**Альтернативы:**
- Processor API с кастомным windowing — более verbose, сложнее
- In-memory counter (текущее) — нет persistence

**Обоснование:**
- DSL — нативная поддержка windowing в Kafka Streams
- RocksDB автоматически для Materialized state store
- `Grouped.with(Serdes.String(), Serdes.String())` — required для Kafka Streams 3.6.1 (R-42)

### Decision 2: Processor API для NPS Escalation

**Выбор:** Кастомный `Processor<String, String>` с `KeyValueStore<String, NpsState>`

**Альтернативы:**
- DSL `.aggregate()` — сложно для per-phone TTL logic
- In-memory map (текущее) — нет persistence

**Обоснование:**
- Processor API даёт полный контроль над state store access
- `init()` получает reference на StateStore
- `purgeExpired()` для TTL cleanup
- Соответствует original design (Decision 1 в fraud-detector change)

### Decision 3: Avro Serialization для Output

**Выбор:** `KafkaAvroSerializer` + `CachedSchemaRegistryClient` для output topic

**Альтернативы:**
- JSON serialization — проще, но spec требует Avro
- Manual Avro encoding — verbose, error-prone

**Обоснование:**
- FraudAlert Avro class already exists (R-43)
- Schema Registry already running (infrastructure change)
- `kafka-avro-serializer` already in dependencies (R-41)
- reporting-nps consumer expects Avro format

### Decision 4: Short Call Filter в Topology Entry Point

**Выбор:** Единый `.filter((key, value) -> extractDuration(value) >= 5)` перед branching на processors

**Альтернативы:**
- Filter в каждом processor — дублирование logic
- Filter после detection — wasted processing

**Обоснование:**
- DRY: один filter для всех трёх detection pipelines
- Performance: early exit для short calls
- Spec requirement: "event is not processed for fraud detection"

### Decision 5: Test Framework

**Выбор:** `EmbeddedKafka` (spring-kafka-test) + `TopologyTestDriver` (kafka-streams-test-utils)

**Альтернативы:**
- Testcontainers (Kafka) — медленнее, но real Kafka
- Unit tests only — не покрывают stream topology

**Обоснование:**
- EmbeddedKafka — быстрый, in-JVM Kafka
- TopologyTestDriver — testing topology без real Kafka
- Testcontainers для full integration (optional, slower)

## Risks / Trade-offs

| Risk | Mitigation |
|------|-----------|
| Kafka Streams 3.6.1 API changes (groupBy needs Grouped.with) | Already documented in R-42, use explicit Serdes |
| Schema Registry unavailable in tests | Mock Schema Registry client or use EmbeddedKafka with test schema |
| RocksDB state dir permissions in Docker | Already configured: state.dir=/tmp/kafka-streams/fraud-detector |
| Avro serialization adds latency | Benchmark with realistic payload; target < 30ms per event |
| Integration tests increase build time | Use EmbeddedKafka (fast), Testcontainers only for critical path |
| Breaking change: output format String → Avro | reporting-nps consumer must update to Avro deserializer |

## Migration Plan

**Шаги развёртывания:**
1. Обновить spec/design/tasks в openspec
2. Собрать JAR: `cd fraud-detector && ./gradlew clean build`
3. Запустить service: `docker compose up fraud-detector`
4. Проверить health: `curl localhost:8082/actuator/health`
5. Отправить тестовый звонок через Kafka
6. Проверить Avro output в calls.fraud-alerts (Kafdrop или console-consumer)

**Rollback:**
- `docker compose down fraud-detector`
- Вернуть предыдущую версию JAR (если есть)
- reporting-nps consumer временно поддерживать оба формата (String + Avro)

## Open Questions

- **O-1: reporting-nps consumer** — нужно ли обновить consumer для Avro format? Или он уже поддерживает Avro?
- **O-2: Schema Registry registration** — нужно ли регистрировать FraudAlert schema на startup (idempotent) или schema already registered (R-41)?
- **O-3: Test coverage target** — какой % coverage требуется? Original spec said >70%.
