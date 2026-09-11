## Why

fraud-detector — антифрод-модуль платформы обработки звонков. Он выявляет мошеннические паттерны в реальном времени: частые звонки с одного номера, эскалация конфликтов (серия негативных NPS), аномальная длительность. Без него невозможно обеспечить безопасность клиентов банка и оперативное реагирование на подозрительную активность.

## What Changes

- Spring Boot 3 микросервис на Java 21, сборка через Gradle (Kotlin DSL)
- Kafka Streams pipeline для обработки событий calls.completed
- Hopping window (1 мин, 10 сек advance): подсчёт звонков с одного номера, детекция > 5 звонков/мин
- Processor API: детекция эскалации (3x негативный NPS < 2 за день)
- State Store (RocksDB) с per-phone TTL 24 часа для счётчиков
- Output: calls.fraud-alerts (key=phone, Avro) via Schema Registry
- Schema Registry integration для Avro сериализации
- Phone number normalization to E.164 before processing
- Exactly-once semantics (processing.guarantee=exactly_once_v2)
- Health check endpoint via Spring Boot Actuator
- Unit-тесты (JUnit 5, Mockito, Embedded Kafka)
- Integration-тесты (Testcontainers)
- Docker build и deployment

## Capabilities

### New Capabilities

- `fraud-detector`: Kafka Streams pipeline для детекции мошеннических паттернов (частые звонки, эскалация NPS, аномальная длительность), Schema Registry integration, phone normalization

### Modified Capabilities

<!-- None yet -->

## Impact

- **Зависимости:** change/infrastructure (Kafka, Schema Registry), change/call-processor (топики calls.completed, E.164 phone format contract)
- **Kafka Topics:** input: calls.completed; output: calls.fraud-alerts
- **Зависимые changes:** reporting-nps зависит от fraud-alerts
- **Порт:** 8082
