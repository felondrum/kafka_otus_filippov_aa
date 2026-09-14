## Why

call-processor — это входная точка платформы обработки звонков колл-центра. Без него невозможна отправка событий в Kafka, на которой строится вся событийно-ориентированная архитектура. Он принимает REST-запросы о завершённых звонках, валидирует данные и надёжно доставляет события в Kafka-кластер.

## What Changes

- Spring Boot 3 микросервис на Java 21 с REST API
- POST /api/calls — приём события о завершённом звонке
- Валидация входных данных (phone format, duration > 0, agentId not null)
- Kafka Producer с idempotent mode и acks=all для надёжной доставки
- Topic Manager — автоматическое создание топиков при старте через Admin API
- Интеграция с Schema Registry для Avro-сериализации
- Dead Letter Queue (DLQ) для битых сообщений
- Retry с exponential backoff при временных сбоях
- Health check endpoint для мониторинга

## Capabilities

### New Capabilities

- `call-processor`: REST API для приёма звонков, Kafka Producer, Topic Manager, валидация, DLQ

### Modified Capabilities

<!-- None yet -->

## Impact

- **Зависимости:** change/infrastructure (Kafka, Schema Registry должны быть запущены)
- **API:** REST POST /api/calls (JSON input), GET /api/health
- **Kafka Topics:** calls.completed (Avro), calls.metadata (Avro, compacted), calls.dlq (JSON)
- **Зависимые changes:** fraud-detector, transcription-analyzer, reporting-nps зависят от этого service
- **Порт:** 8081
