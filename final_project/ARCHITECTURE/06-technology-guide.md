# Технологический справочник

Документ описывает, **как работают** ключевые технологии проекта на уровне кода, конфигурации и архитектуры.

---

## Оглавление

1. [Kafka Streams: архитектура и работа](#1-kafka-streams-архитектура-и-работа)
2. [ksqlDB: от запроса до результата](#2-ksqldb-от-запроса-до-результата)
3. [Гарантии доставки и идемпотентность](#3-гарантии-доставки-и-идемпотентность)
4. [Schema Registry: управление схемами](#4-schema-registry-управление-схемами)
5. [JSON-сериализация в Kafka](#5-json-сериализация-в-kafka)
6. [Dual-Write Pattern (Kafka + PostgreSQL)](#6-dual-write-pattern-kafka--postgresql)
7. [Manual Ack в Kafka Consumer](#7-manual-ack-в-kafka-consumer)
8. [RocksDB State Store](#8-rocksdb-state-store)
9. [Сводная таблица гарантий](#9-сводная-таблица-гарантий)

---

## 1. Kafka Streams: архитектура и работа

### 1.1 Что это

Kafka Streams — клиентская библиотека для обработки потоков данных в реальном времени. Работает **внутри JVM** приложения, читает из Kafka-топиков, обрабатывает данные и пишет результаты обратно в Kafka.

### 1.2 Архитектура fraud-detector

```
┌─────────────────────────────────────────────────────────┐
│                    fraud-detector JVM                    │
│                                                         │
│  ┌──────────────────────────────────────────────────┐   │
│  │              StreamThread-1                       │   │
│  │                                                   │   │
│  │  calls.completed ──▶ KStream                      │   │
│  │                       │                           │   │
│  │          ┌────────────┼────────────┐              │   │
│  │          ▼            ▼            ▼              │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐         │   │
│  │  │ Frequent │ │ NPS      │ │ Anomalous│         │   │
│  │  │ Calls    │ │ Escalation│ │ Duration │         │   │
│  │  │ Processor│ │ Processor │ │ Processor│         │   │
│  │  └────┬─────┘ └────┬─────┘ └────┬─────┘         │   │
│  │       │            │            │                │   │
│  │       └────────────┴────────────┘                │   │
│  │                    │                              │   │
│  │                    ▼                              │   │
│  │           calls.fraud-alerts                      │   │
│  └──────────────────────────────────────────────────┘   │
│                                                         │
│  ┌──────────────────────────────────────────────────┐   │
│  │              State Stores (RocksDB)               │   │
│  │  • frequent-calls-count                          │   │
│  │  • nps-escalation-store                          │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### 1.3 Как работает StreamThread

Каждый `StreamThread` — это отдельный поток, который:

1. **poll()** — запрашивает новые записи из Kafka через `Consumer.poll()`
2. **process()** — пропускает каждую запись через топологию (filter → map → transform)
3. **commit()** — сохраняет offset в `__consumer_offsets`
4. **punctuate()** — выполняет scheduled-задачи (например, purge expired entries)

```text
// StreamsTopologyConfig.java
@Bean
public KafkaStreams kafkaStreams(StreamsBuilder streamsBuilder, ...) {
    Map<String, Object> props = new HashMap<>();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("application.id", appId);
    props.put("processing.guarantee", "at_least_once");
    props.put("state.dir", "/tmp/kafka-streams/fraud-detector");
    props.put("max.poll.interval.ms", 600000);  // 10 минут
    props.put("max.poll.records", 500);

    // Build topology
    KStream<String, String> callStream = streamsBuilder
        .stream("calls.completed", Consumed.with(Serdes.String(), Serdes.String()));

    // Three independent pipelines
    frequentCallsProcessor.detect(callStream, "calls.fraud-alerts");
    npsEscalationProcessor.detect(callStream, "calls.fraud-alerts");
    anomalousDurationProcessor.detect(callStream, "calls.fraud-alerts");

    return new KafkaStreams(streamsBuilder.build(), props);
}
```

### 1.4 Hopping Window (FrequentCallsProcessor)

```text
// Hopping Window: 60s size, 10s advance
callStream
    .groupBy((phone, event) -> phone)
    .windowedBy(TimeWindows.ofSizeAndGrace(
        Duration.ofMillis(60000),   // окно 60 секунд
        Duration.ofMillis(10000)))  // сдвиг 10 секунд
    .count(Materialized.as("frequent-calls-count"))
    .filter((windowedKey, count) -> count > 5)
    .map((windowedKey, count) -> {
        // Emit fraud alert
        return KeyValue.pair(windowedKey.key(), alertJson);
    })
    .to("calls.fraud-alerts", Produced.with(Serdes.String(), Serdes.String()));
```

**Как работает window:**

```
Время:  0s        10s       20s       30s       40s       50s       60s
        |---------|---------|---------|---------|---------|---------|

Window [0,60):    ████████████████████████████████████████████
Window [10,70):         ████████████████████████████████████████
Window [20,80):              ████████████████████████████████████
Window [30,90):                   ████████████████████████████████
Window [40,100):                        ████████████████████████████
Window [50,110):                             ████████████████████████

Вызов в t=5s попадает в 6 окон: [0,60), [10,70), [20,80), [30,90), [40,100), [50,110)
```

**Почему hopping?** Потому что звонок в начале окна должен учитываться и в следующем окне — иначе при частых звонках можно пропустить аномалию.

### 1.5 NPS Escalation (Processor API)

Использует **Processor API** вместо DSL, потому нужен доступ к `ProcessorContext` для scheduled-задач:

```java
private static class NpsEscalationProcessorImpl 
    implements Transformer<String, String, KeyValue<String, String>> {
    
    private ProcessorContext context;
    private KeyValueStore<String, String> store;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        
        // Punctuation: каждые 1 минуту очищаем просроченные записи
        this.context.schedule(
            Duration.ofMinutes(1),
            PunctuationType.WALL_CLOCK_TIME,
            timestamp -> purgeExpiredEntries()
        );
        
        this.store = (KeyValueStore<String, String>) 
            context.getStateStore("nps-escalation-store");
    }

    @Override
    public KeyValue<String, String> transform(String phone, String jsonEvent) {
        int npsScore = parseNpsScore(jsonEvent);
        
        // Read state from RocksDB
        String stateStr = store.get(phone);
        NpsState state = stateStr != null ? deserialize(stateStr) : new NpsState(0, 0);
        
        if (npsScore < 2) {
            state.negativeCount++;
            state.lastActivity = System.currentTimeMillis();
            store.put(phone, serialize(state));
            
            // Threshold reached → emit alert
            if (state.negativeCount >= 3) {
                store.delete(phone);  // Clean up state
                return createAlert(phone);
            }
        }
        
        return null;  // Suppress event
    }

    private void purgeExpiredEntries() {
        long cutoff = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(24);
        store.all().forEachRemaining(entry -> {
            NpsState state = deserialize(entry.value);
            if (state.lastActivity < cutoff) {
                store.delete(entry.key);
            }
        });
    }
}
```

### 1.6 Lifecycle

```java
@Component
public class KafkaStreamsLifecycle {
    private final KafkaStreams kafkaStreams;

    @PostConstruct
    public void start() {
        kafkaStreams.start();  // Запускает StreamThread
    }

    @PreDestroy
    public void stop() {
        kafkaStreams.close();  // Graceful shutdown: flush state, commit offsets
    }
}
```

---

## 2. ksqlDB: от запроса до результата

### 2.1 Что это

ksqldb — сервер для обработки потоков данных с помощью SQL. Работает как REST API, выполняет SQL-запросы и автоматически читает/пишет в Kafka-топики.

### 2.2 Архитектура

```
┌──────────────────────────────────────────────────────┐
│                    ksqldb-server                       │
│                                                      │
│  ┌────────────────────────────────────────────────┐  │
│  │  External Streams (metadata-only)              │  │
│  │  • calls_completed_ext  → calls.completed      │  │
│  │  • calls_fraud_alerts_ext → calls.fraud-alerts │  │
│  └──────────────────────┬─────────────────────────┘  │
│                         │                            │
│  ┌──────────────────────▼─────────────────────────┐  │
│  │  Persistent Queries (long-running)             │  │
│  │                                                │  │
│  │  CREATE TABLE calls_completed_agg AS           │  │
│  │    SELECT agentId, COUNT(*), AVG(npsScore)     │  │
│  │    FROM calls_completed_ext                    │  │
│  │    WINDOW TUMBLING (SIZE 5 MINUTES)            │  │
│  │    GROUP BY agentId EMIT CHANGES;              │  │
│  │        │                                       │  │
│  │        └──▶ writes to → calls.completed.agg    │  │
│  │                                                │  │
│  │  CREATE TABLE fraud_alerts_filtered AS         │  │
│  │    SELECT phone, COUNT(*)                      │  │
│  │    FROM calls_fraud_alerts_ext                 │  │
│  │    GROUP BY phone EMIT CHANGES;                │  │
│  │        │                                       │  │
│  │        └──▶ writes to → calls.fraud-alerts.agg │  │
│  └────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────┘
```

### 2.3 Как работает init-скрипт

```bash
# Step 1: Create external streams (metadata-only)
CREATE STREAM calls_completed_ext (
    callId VARCHAR, phone VARCHAR, duration BIGINT, 
    agentId VARCHAR, npsScore INT, completedAt BIGINT
) WITH (KAFKA_TOPIC='calls.completed', VALUE_FORMAT='JSON');

# Step 2: Create persistent aggregation tables
CREATE TABLE calls_completed_agg 
    WITH (KAFKA_TOPIC='calls.completed.agg', PARTITIONS=6) AS
SELECT agentId, COUNT(*) AS call_count, AVG(npsScore) AS avg_nps_score,
       MAX(completedAt) AS last_call_at
FROM calls_completed_ext
WINDOW TUMBLING (SIZE 5 MINUTES, GRACE PERIOD 1 MINUTE)
GROUP BY agentId EMIT CHANGES;
```

**Что происходит:**

1. `CREATE STREAM` — создаёт **метаданные** (schema). Не читает данные, не создаёт топики.
2. `CREATE TABLE ... AS SELECT ...` — запускает **persistent query**:
   - Потребляет данные из `calls_completed_ext`
   - Поддерживает internal state (aggregation windows)
   - Пишет результаты в `calls.completed.agg`
   - Создаёт internal changelog-топики для recovery

### 2.4 Tumbling Window vs Hopping Window

| Параметр | Tumbling (ksqlDB) | Hopping (Kafka Streams) |
|----------|-------------------|------------------------|
| **Размер** | 5 минут | 60 секунд |
| **Overlap** | Нет | 10 секунд (6 окон) |
| **Grace Period** | 1 минута | — |
| **Использование** | `calls_completed_agg` | `frequent-calls-count` |

**Tumbling Window:**

```
Время:  0s        5min      10min     15min
        |---------|---------|---------|---------|

Window [0, 5min):   ████████████████████████████
Window [5min, 10min):                          ████████████████████████████
Window [10min, 15min):                                         ████████████████████████████

Каждый event попадает ровно в 1 окно.
```

### 2.5 Output Topics

ksqldb **автоматически создаёт** output-топики:

```
calls.completed.agg  ← ksqlDB persistent query (PARTITIONS=6)
calls.fraud-alerts.agg ← ksqlDB persistent query (PARTITIONS=6)
```

Эти топики содержат **агрегированные** данные, которые можно читать через REST API ksqlDB или потреблять другими consumers.

---

## 3. Гарантии доставки и идемпотентность

### 3.1 Idempotent Producer (call-processor)

```java
// KafkaConfig.java
@Bean
public KafkaTemplate<String, String> kafkaTemplate() {
    Map<String, Object> configProps = new HashMap<>();
    configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);  // ✅
    configProps.put(ProducerConfig.ACKS_CONFIG, "all");               // ✅
    configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configProps));
}
```

**Как работает:**

1. **Producer ID** — при подключении broker выдаёт уникальный `producerId`
2. **Sequence Number** — каждая запись получает `sequenceNumber` (инкремент для каждого partition)
3. **Broker deduplication** — broker хранит `(producerId, partition, sequenceNumber)` и отбрасывает дубликаты

```
Producer → Record(seq=5) → Broker
                              │
                              ├── Если seq=5 уже есть → DROP (duplicate)
                              └── Если seq=5 новый → ACCEPT, increment to 6
```

**`acks=all`** — лидер и все ISR (in-sync replicas) должны подтвердить запись. Гарантирует, что данные не потеряются при failover.

### 3.2 Retry + Idempotency (CallEventService)

```java
@Retryable(
    retryFor = {RuntimeException.class},
    maxAttempts = 3,
    backoff = @Backoff(delay = 1000, multiplier = 2)  // 1s, 2s, 4s
)
public void produceToTopic(Map<String, Object> event, String topic, String key) {
    String json = objectMapper.writeValueAsString(event);
    CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, json);
    SendResult<String, String> result = future.get(10, TimeUnit.SECONDS);
}
```

**Сценарий:**

```
Attempt 1: send(seq=5) → Broker acks → Client timeout → RuntimeException
Attempt 2: send(seq=5) → Broker sees duplicate → DROP → Success
```

**Результат:** at-least-once на уровне приложения, exactly-once на уровне брокера.

### 3.3 Kafka Streams: at_least_once

```text
props.put("processing.guarantee", "at_least_once");
```

**Что это значит:**

- Kafka Streams использует **transactional writes** при записи в output-топики
- Если обработка fails mid-batch, output-записи не коммитятся
- Offset consumer'а коммитится **после** успешной обработки батча
- При restart — records могут быть обработаны повторно

```
Batch 1: consume(10 records) → process() → write(output topic) → commit offset
                                          ↓
                                    FAIL (exception)
                                          ↓
                              output topic NOT written
                              offset NOT committed
                              при restart: reprocess 10 records
```

### 3.4 Manual Ack (reporting-nps)

```java
@KafkaListener(topics = "calls.fraud-alerts", groupId = "reporting-nps",
        containerFactory = "kafkaListenerContainerFactory")
public void consume(String jsonEvent, Acknowledgment ack) {
    try {
        // 1. Parse JSON
        // 2. Lookup call metadata
        // 3. Save to PostgreSQL
        fraudStatsRepository.save(stats);
        
        // 4. Commit offset AFTER successful processing
        ack.acknowledge();
    } catch (Exception e) {
        // Exception = no ack = message redelivered
        throw new RuntimeException(e);
    }
}
```

**Конфигурация:**

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: false
      auto-offset-reset: earliest
```

```text
// KafkaListenerConfig.java
factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
```

**Сценарий:**

```
Message 1: consume → save to PG → ack.acknowledge() → offset committed ✅
Message 2: consume → save to PG → CRASH before ack → offset NOT committed
                                                    ↓
                                           at restart: reprocess Message 2
```

---

## 4. Schema Registry: управление схемами

### 4.1 Что это

Schema Registry — централизованное хранилище схем для Kafka-сообщений. Гарантирует совместимость схем между producer и consumer.

### 4.2 Subjects

```
calls.completed-value
calls.fraud-alerts-value
transcription.raw-value
transcription.enriched-value
customers.profile-value
```

Subject name convention: `{topic-name}-value`

### 4.3 Idempotent Registration

```bash
# register-schemas.sh
register_schema() {
    local subject="$topic_name-value"
    
    # Check if subject exists
    if curl -sf "$SCHEMA_REGISTRY_URL/subjects/$subject" > /dev/null 2>&1; then
        # Compare schemas
        local existing_schema=$(curl -sf "$SCHEMA_REGISTRY_URL/subjects/$subject/versions/latest")
        local new_schema=$(cat "$schema_file" | jq -c '.')
        
        if [ "$existing_schema" = "$new_schema" ]; then
            echo "Schema is identical, skipping"
            return 0
        fi
    fi
    
    # Register new schema
    curl -X POST ... --data '{"subject": "$subject", "schema": ...}'
}
```

### 4.4 Совместимость

```
Compatibility: BACKWARD

Schema v1: { "name": "phone", "type": "string" }
Schema v2: { "name": "phone", "type": "string" },
           { "name": "agentId", "type": "string", "default": "" }

✅ Backward compatible: new schema can read old data (default values)
❌ Forward incompatible: old schema can't read new data (new fields)
```

---

## 5. JSON-сериализация в Kafka

### 5.1 StringSerializer / StringDeserializer

Все 4 сервиса используют **plain JSON text** с String keys и String values:

```text
// call-processor (producer)
configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

// reporting-nps (consumer)
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
```

**Data in Kafka:**

```
Topic: calls.completed
Partition 0, Offset 5:
  Key: "abc-123"
  Value: {"phone":"+79001234567","duration":120,"npsScore":5,"agentId":"agent-1"}

Topic: calls.fraud-alerts
Partition 2, Offset 12:
  Key: "+79001234567"
  Value: {"phone":"+79001234567","pattern":"FREQUENT_CALLS","severity":"MEDIUM","count":8}
```

### 5.2 Confluent JSON (DualWriter)

Единственное исключение — `DualWriter` использует **Confluent JSON format** для Kafka Connect JDBC Sink:

```java
// DualWriter.java
private Map<String, Object> buildConfluentJson(Map<String, Object> payload) {
    Map<String, Object> schema = new HashMap<>();
    schema.put("type", "struct");
    schema.put("field", Arrays.asList(
        new HashMap<String, Object>() {{ put("name", "call_id"); put("type", "string"); }},
        new HashMap<String, Object>() {{ put("name", "status"); put("type", "string"); }}
    ));
    
    Map<String, Object> result = new HashMap<>();
    result.put("schema", schema);
    result.put("payload", payload);
    return result;
}

// Output:
// {"schema":{"type":"struct","field":[...]}, "payload":{"call_id":"abc","status":"COMPLETED"}}
```

---

## 6. Dual-Write Pattern (Kafka + PostgreSQL)

### 6.1 Что это

Dual-Write — pattern, при котором данные пишутся одновременно в Kafka и PostgreSQL. Kafka — system of record, PostgreSQL — downstream consumer.

### 6.2 Реализация (transcription-analyzer)

```text
// DualWriter.java
public void write(Map<String, Object> enriched) {
    String callId = (String) enriched.get("call_id");
    
    // Step 1: Write to Kafka FIRST (blocking)
    Map<String, Object> confluentJson = buildConfluentJson(enriched);
    String json = objectMapper.writeValueAsString(confluentJson);
    CompletableFuture<SendResult<String, String>> future = 
        kafkaTemplate.send("transcription.enriched", callId, json);
    
    try {
        future.get(10, TimeUnit.SECONDS);  // Block until ack
    } catch (Exception e) {
        log.error("Kafka write failed for callId: {}", callId, e);
        return;  // Stop if Kafka fails
    }
    
    // Step 2: Write to PostgreSQL with retry
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            jdbcTemplate.update(
                "INSERT INTO call_transcriptions (call_id, status, ...) VALUES (?, ?, ...)",
                callId, enriched.get("status"), ...
            );
            return;  // Success
        } catch (Exception e) {
            if (attempt == MAX_RETRIES) throw e;
            Thread.sleep(INITIAL_BACKOFF_MS * (long) Math.pow(2, attempt - 1));
        }
    }
}
```

**Порядок операций:**

```
1. Enriched transcription ready
2. Write to Kafka (transcription.enriched) ← System of Record
3. Kafka Connect JDBC Sink reads from Kafka
4. Write to PostgreSQL (call_transcriptions) ← Downstream
```

**Почему Kafka сначала?** Если PostgreSQL fails, Kafka всё ещё содержит данные — Kafka Connect retry'ет. Если PostgreSQL сначала — Kafka может потерять данные при crash.

---

## 7. Manual Ack в Kafka Consumer

### 7.1 Как работает

```
Consumer poll() → Message 1 → Process → ack.acknowledge() → commit offset
                   ↓
                 Message 2 → Process → CRASH → offset NOT committed
                                              ↓
                                     at next poll: re-deliver Message 2
```

### 7.2 AckMode.MANUAL vs AUTO

| Mode | Когда коммитится offset | Когда redeliver |
|------|------------------------|-----------------|
| `AUTO` | Immediately after poll() | При crash |
| `MANUAL` | После `ack.acknowledge()` | При exception |
| `RECORD` | После каждой записи | При exception |
| `BATCH` | После всего батча | При exception |

### 7.3 Конфигурация в проекте

```text
// reporting-nps
factory.setAckMode(ContainerProperties.AckMode.MANUAL);
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

// transcription-analyzer
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
// ack-mode: record (Spring Kafka listener config)
```

---

## 8. RocksDB State Store

### 8.1 Что это

RocksDB — embedded key-value store, встроенный в Kafka Streams. Используется для сохранения state (окна, счётчики, агрегации).

### 8.2 Типы state stores

```text
// Persistent (на диске) — survives restart
Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("nps-escalation-store"),
    Serdes.String(), Serdes.String()
)

// In-memory — lost on restart
Stores.keyValueStoreBuilder(
    Stores.inMemoryKeyValueStore("frequent-calls-count"),
    Serdes.String(), Serdes.Long()
)
```

### 8.3 Как работает

```
┌─────────────────────────────────────┐
│         RocksDB (state.dir)         │
│                                     │
│  /tmp/kafka-streams/fraud-detector  │
│  ├── nps-escalation-store          │
│  │   ├── +79001000005 → "3,1790859"│
│  │   ├── +79001000004 → "2,1790858"│
│  │   └── +79001000001 → "1,1790857"│
│  │                                 │
│  ├── frequent-calls-count          │
│  │   ├── +79001000005 → 80         │
│  │   ├── +79001000004 → 102        │
│  │   └── ...                       │
│  │                                 │
│  └── changelog topics              │
│      ├── nps-escalation-store-changelog
│      └── frequent-calls-count-changelog
└─────────────────────────────────────┘
```

**Changelog topics:**

Каждое изменение state store записывается в internal changelog-topic. При restart ksqlDB/fraud-detector восстанавливает state из changelog.

```
nps-escalation-store-changelog:
  Key: "+79001000005" → Value: "3,1790859"
  Key: "+79001000004" → Value: "2,1790858"
```

---

## 9. Сводная таблица гарантий

| Компонент | Guarantee | Как реализовано |
|-----------|-----------|-----------------|
| **call-processor** (producer) | **Exactly-once** per partition | `enable.idempotence=true` + `acks=all` |
| **fraud-detector** (Kafka Streams) | **At-least-once** | `processing.guarantee=at_least_once` |
| **reporting-nps** (consumer) | **At-least-once** | `AckMode.MANUAL` + manual `ack.acknowledge()` |
| **transcription-analyzer** (consumer) | **At-least-once** | `enable.auto.commit=true` + `ack-mode: record` |
| **ksqldb-server** | **At-least-once** | Internal state recovery from changelog topics |
| **Kafka Connect** | **At-least-once** | `auto.offset.reset=earliest` + offset commits |

### 9.1 End-to-End Pipeline

```
call-processor ──[exactly-once]──▶ Kafka (calls.completed)
                                      │
                                      ▼
                              fraud-detector
                              [at-least-once]
                                      │
                                      ▼
                              Kafka (calls.fraud-alerts)
                                      │
                          ┌───────────┴───────────┐
                          ▼                       ▼
                  reporting-nps              ksqldb
                  [at-least-once]            [at-least-once]
                          │                       │
                          ▼                       ▼
                  PostgreSQL              Kafka (output topics)
                  (fraud_stats)
```

**Почему нет end-to-end exactly-once?**

1. call-processor гарантирует exactly-once на producer level
2. fraud-detector использует at-least-once — записи могут дублироваться
3. reporting-nps использует manual ack — при crash сообщения redeliver'ятся
4. Для end-to-end exactly-once нужен `processing.guarantee=exactly_once_v2` на всех этапах + transactional joins

В текущем проекте **дубликаты допустимы** — fraud_stats инкрементирует count, PostgreSQL primary key предотвращает дубликаты в call_metadata.
