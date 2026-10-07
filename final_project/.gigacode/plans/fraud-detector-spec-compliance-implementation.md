# Fraud-Detector Spec Compliance — Implementation Plan

## Overview

Доводим fraud-detector до полной spec compliance. Все 4 недостающие задачи в одном change:

1. **DSL Hopping Window** — FrequentCallsProcessor с DSL windowing + RocksDB
2. **Processor API + StateStore** — NpsEscalationProcessor с KeyValueStore
3. **Avro serialization** — KafkaAvroSerializer для output topic
4. **Integration tests** — EmbeddedKafka + TopologyTestDriver, >70% coverage

## Implementation Order

```
Phase 1: Avro Serde (основа для всех)
    ↓
Phase 2: DSL Hopping Window (FrequentCalls)
    ↓
Phase 3: Processor API (NPS Escalation)
    ↓
Phase 4: Integration Tests (валидация всего)
```

## Phase 1: Avro Serialization

**Files to modify:**
- `StreamsTopologyConfig.java` — добавить AvroSerde для output
- `application.yml` — убедиться что Schema Registry URL настроен
- Все processors — использовать AvroSerde вместо Serdes.String()

**Key changes:**
```java
// In StreamsTopologyConfig
SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(
    schemaRegistryUrl, 1);
AvroSerde<FraudAlert> avroSerde = new AvroSerde<>(schemaRegistry);

// In processors
.to(fraudAlertsTopic, Produced.with(Serdes.String(), avroSerde));
```

**Verification:**
- Schema Registry subject `calls.fraud-alerts-value` содержит FraudAlert schema
- calls.fraud-alerts topic получает Avro-serialized events
- Console consumer может десериализовать Avro

## Phase 2: DSL Hopping Window (FrequentCallsProcessor)

**Files to modify:**
- `FrequentCallsProcessor.java` — полностью переписать с DSL windowing

**API для Kafka Streams 3.6.1:**
```java
callStream
    .map((key, value) -> {
        String phone = extractPhone(value);
        String normalizedPhone = phoneNormalizer.normalize(phone);
        return KeyValue.pair(normalizedPhone, value);
    })
    .filter((phone, value) -> value != null)
    .groupBy((phone, value) -> phone, Grouped.with(Serdes.String(), Serdes.String()))
    .windowedBy(TimeWindows.ofSizeAndAdvance(60000, 10000))
    .count(Materialized.as("frequent-calls-count"))
    .toStream()
    .filter((windowedKey, count) -> count > 5)
    .map((windowedKey, count) -> {
        FraudAlert alert = new FraudAlert(
            "freq-" + windowedKey.key(),
            windowedKey.key(),
            FraudAlert.FraudPattern.FREQUENT_CALLS,
            count.intValue(),
            Instant.now().toString(),
            FraudAlert.AlertSeverity.MEDIUM
        );
        return KeyValue.pair(windowedKey.key(), alert);
    })
    .mapValues(alert -> serializeAvro(alert))
    .to(fraudAlertsTopic, Produced.with(Serdes.String(), avroSerde));
```

**Key API notes:**
- `TimeWindows.ofSizeAndAdvance(size, advance)` — работает в 3.6.1
- `Grouped.with(Serdes.String(), Serdes.String())` — required для 3.6.1
- `Materialized.as("frequent-calls-count")` — RocksDB state store
- `toStream()` — конвертирует WindowedKStream в KStream

**Verification:**
- RocksDB state store "frequent-calls-count" создаётся
- 6+ calls from same phone in 1 min → FREQUENT_CALLS alert
- 5 calls → no alert
- Overlapping windows work correctly

## Phase 3: Processor API (NpsEscalationProcessor)

**Files to modify:**
- `NpsEscalationProcessor.java` — полностью переписать с Processor API

**Key implementation:**
```java
public class NpsEscalationProcessor implements Processor<String, String> {
    private ProcessorContext<String, FraudAlert> context;
    private KeyValueStore<String, NpsState> store;
    
    @Override
    public void init(ProcessorContext ctx) {
        this.context = ctx;
        this.store = (KeyValueStore) ctx.getStateStore("nps-escalation-store");
    }
    
    @Override
    public void process(String phone, String value) {
        Integer npsScore = extractNpsScore(value);
        if (npsScore != null && npsScore < 2) {
            NpsState state = store.get(phone);
            if (state == null) {
                state = new NpsState(0, System.currentTimeMillis());
            }
            state.negativeCount++;
            state.lastActivity = System.currentTimeMillis();
            store.put(phone, state);
            
            if (state.negativeCount >= 3) {
                FraudAlert alert = new FraudAlert(
                    "nps-" + phone,
                    phone,
                    FraudAlert.FraudPattern.NPS_ESCALATION,
                    state.negativeCount,
                    Instant.now().toString(),
                    FraudAlert.AlertSeverity.HIGH
                );
                context.forward("fraud-alerts", phone, alert);
                store.delete(phone);
            }
        }
    }
}
```

**Topology config:**
```java
// In StreamsTopologyConfig
Topology topology = new Topology();
topology.addSource("source", Serdes.String().deserializer(), Serdes.String().deserializer(), 
    properties.getCompletedTopic());
topology.addProcessor("nps-processor", NpsEscalationProcessor::new, "source");
topology.addStateStore(
    StoreBuilders.keyValueStoreBuilder(
        WrappedKeyValueStores.serde("nps-escalation-store", 
            Serdes.String(), npsStateSerde),
        Serdes.String()),
    "nps-processor");
topology.addSink("sink", properties.getFraudAlertsTopic(), 
    Serdes.String().serializer(), avroSerde);
```

**Verification:**
- StateStore "nps-escalation-store" создаётся
- 3x NPS < 2 in 24h → NPS_ESCALATION alert
- 2x NPS < 2 → no alert
- State persists to RocksDB

## Phase 4: Integration Tests

**Files to create:**
- `FrequentCallsIntegrationTest.java` — DSL windowing test
- `NpsEscalationIntegrationTest.java` — Processor API test
- `AnomalousDurationIntegrationTest.java` — filter + Avro test
- `SchemaRegistryIntegrationTest.java` — Avro serialization test

**Test framework:**
- `@EmbeddedKafka` from spring-kafka-test
- `TopologyTestDriver` from kafka-streams-test-utils
- Verify Avro output format
- Verify state persistence

**Verification:**
- `./gradlew test` — all tests pass
- `./gradlew testCoverage` — >70% coverage

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| DSL Hopping Window API incompatibility | Use TimeWindows.ofSizeAndAdvance (verified in 3.6.1) |
| Processor API StateStore registration | Use Topology.addStateStore() in StreamsTopologyConfig |
| AvroSerde requires Schema Registry URL | Pass URL from application.yml |
| Integration tests slow | Use EmbeddedKafka (in-JVM), not Testcontainers |
| Build time increases | Gradle parallel execution, cached dependencies |

## Files Modified

| File | Action |
|------|--------|
| `FrequentCallsProcessor.java` | Rewrite: DSL Hopping Window |
| `NpsEscalationProcessor.java` | Rewrite: Processor API + StateStore |
| `AnomalousDurationProcessor.java` | Update: use AvroSerde for output |
| `StreamsTopologyConfig.java` | Add: AvroSerde, StateStore registration |
| `*Test.java` (4 files) | Replace with real integration tests |
| `build.gradle.kts` | Add: kafka-streams-test-utils |

## Success Criteria

- [ ] Build: `./gradlew clean build` passes
- [ ] DSL windowing: FrequentCalls uses TimeWindows.ofSizeAndAdvance
- [ ] Processor API: NPS uses Processor<String, String> + KeyValueStore
- [ ] Avro output: calls.fraud-alerts receives Avro-serialized FraudAlert
- [ ] Schema Registry: FraudAlert schema registered at calls.fraud-alerts-value
- [ ] Tests: 5+ integration tests, >70% coverage
- [ ] Docker: image builds, container healthy
- [ ] E2E: all 3 fraud patterns produce correct alerts
