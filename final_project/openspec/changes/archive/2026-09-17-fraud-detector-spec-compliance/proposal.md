## Why

fraud-detector change реализовал упрощённую версию (in-memory counters, pipe-delimited output) из-за API-ограничений Kafka Streams 3.6.1. Спецификация требует полного функционала: DSL windowing, RocksDB state store, Avro сериализация через Schema Registry, short call filter. Текущая реализация не соответствует spec — нет persistence, нет windowing, нет Avro output.

## What Changes

- **FrequentCallsProcessor**: Заменить in-memory counter на DSL Hopping Window (1 мин размер, 10 сек advance) с `.windowedBy()` + `.count(Materialized.as(...))`
- **NpsEscalationProcessor**: Заменить in-memory map на Processor API с RocksDB StateStore для per-phone `(negativeCount, lastActivityTimestamp)`
- **Avro Serialization**: Заменить pipe-delimited String на Avro FraudAlert (schema already exists) через KafkaAvroSerializer + Schema Registry
- **Short Call Filter**: Добавить `.filter(duration >= 5s)` в topology entry point
- **Severity Levels**: Добавить enum severity (HIGH/MEDIUM/LOW) в FraudAlert
- **Integration Tests**: Заменить smoke-тесты на реальные integration-тесты с EmbeddedKafka + TopologyTestDriver

## Capabilities

### Modified Capabilities

- `fraud-detector`: Полная реализация Kafka Streams pipeline — DSL windowing, RocksDB state store, Avro output, short call filter, severity levels

## Impact

- **Modified code**: `FrequentCallsProcessor.java`, `NpsEscalationProcessor.java`, `AnomalousDurationProcessor.java`, `StreamsTopologyConfig.java`
- **Tests**: 4 test file → 5+ integration tests
- **Dependencies**: `kafka-streams-test-utils` для тестов, `kafka-avro-serializer` уже есть
- **No API changes**: Output topic `calls.fraud-alerts` format changes (String → Avro), downstream `reporting-nps` needs Avro consumer
