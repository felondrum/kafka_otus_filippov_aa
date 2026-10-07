## Why

transcription-analyzer — модуль обработки транскрипции и суммаризации диалогов. Он генерирует синтетический текст транскрипции (фиктивный, на основе duration звонка), извлекает ключевую информацию (проблема, решение, сентимент, срочность) через keyword matching, обогащает данные профилем клиента и сохраняет результаты в PostgreSQL. Без него невозможно обеспечить архивацию диалогов, аналитику тональности и контекстуальную отчётность.

## What Changes

- Spring Boot 3 микросервис на Java 21, сборка через Gradle (Kotlin DSL)
- Producer часть: генерация синтетического текста транскрипции (фиктивный текст на основе duration звонка), отправка в transcription.raw
- Kafka Streams часть: чтение transcription.raw + transcription.summary, enrichment с customers.profile (KTable join)
- Synthetic summary generation: извлечение problem, solution, sentiment, urgency, confidence через keyword matching (фиктивная, без реальной LLM)
- Dual-write strategy: Kafka (transcription.enriched) + JDBC (PostgreSQL)
- Metadata lifecycle management: PENDING → TRANSCRIBING → SUMMARIZING → COMPLETED
- Обновление calls.metadata топика со статусами
- Unit-тесты (JUnit 5, Mockito, Embedded Kafka)
- Integration-тесты (Testcontainers с PostgreSQL)
- Docker build и deployment

## Capabilities

### New Capabilities

- `transcription-analyzer`: producer (синтетическая генерация транскрипции), Kafka Streams (enrichment, synthetic summary generation), consumer (JDBC writer в PostgreSQL), dual-write strategy, metadata lifecycle

### Modified Capabilities

<!-- None yet -->

## Impact

- **Зависимости:** change/infrastructure (Kafka, PostgreSQL), change/call-processor (топики calls.completed, calls.metadata)
- **Kafka Topics:** input: calls.completed, transcription.raw, transcription.summary, customers.profile; output: transcription.enriched, calls.metadata
- **PostgreSQL:** tables call_metadata, call_transcriptions
- **Зависимые changes:** reporting-nps зависит от transcription.enriched (consumes для call_transcriptions upsert) и PostgreSQL данных (читает call_transcriptions для v_full_call_info view)
- **Порт:** 8083
