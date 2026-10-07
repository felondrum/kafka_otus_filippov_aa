## Why

reporting-nps — сервис отчётности и аналитики платформы. Он реализует паттерн CQRS: команды пишутся в Kafka, запросы читаются из PostgreSQL. Без него невозможно предоставить менеджерам дашборды с актуальной статистикой (NPS, загрузка агентов, тональность, статистика фрода). Это последний компонент, замыкающий полный цикл обработки звонков.

## What Changes

- Spring Boot 3 микросервис на Java 21, сборка через Gradle (Kotlin DSL)
- Kafka Consumer: чтение calls.metadata, transcription.enriched, calls.fraud-alerts
- CQRS read side: обновление PostgreSQL (call_metadata, call_transcriptions, fraud stats)
- REST API: GET /api/reports/daily, /api/reports/agent/{id}, /api/metadata/{callId}, /api/metadata/status/{status}, /api/sentiment/distribution
- Database schema: call_metadata, call_transcriptions, view v_full_call_info
- Aggregations + caching for fast queries
- Unit-тесты (JUnit 5, Mockito, Embedded Kafka)
- Integration-тесты (Testcontainers с PostgreSQL)
- Docker build и deployment

## Capabilities

### New Capabilities

- `reporting-nps`: Kafka Consumer (metadata, enriched, fraud-alerts), CQRS read side (PostgreSQL), REST API для отчётов и метаданных, агрегации и кэширование

### Modified Capabilities

<!-- None yet -->

## Impact

- **Зависимости:** change/infrastructure (Kafka, PostgreSQL), change/call-processor (calls.metadata), change/fraud-detector (calls.fraud-alerts), change/transcription-analyzer (transcription.enriched)
- **Kafka Topics:** input: calls.metadata, transcription.enriched, calls.fraud-alerts
- **PostgreSQL:** tables call_metadata, call_transcriptions, view v_full_call_info
- **REST API:** GET /api/reports/daily, /api/reports/agent/{id}, /api/metadata/{callId}, /api/metadata/status/{status}, /api/sentiment/distribution, GET /api/health
- **Порт:** 8084
