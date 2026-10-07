## Why

Текущая инфраструктура не имеет инструмента для генерации синтетической нагрузки, имитирующей внешнюю telephony-систему. Это необходимо для полноценного тестирования pipeline'а (call-processor → fraud-detector → reporting-nps) под реалистичной нагрузкой, включая проверку срабатываний fraud-detector.

## What Changes

- Добавляется новый сервис `load-simulator` (Java/Spring/Gradle) для генерации синтетических вызовов
- Сервис предоставляет REST API с двумя эндпоинтами: запуск и остановка генерации нагрузки
- Swagger UI для ручного управления через браузер
- Генерация звонков с burst-паттерном и настраиваемым распределением фрод-сценариев
- Интеграция в существующий `docker-compose.yml`

## Capabilities

### New Capabilities

- `load-simulator`: Генерация синтетической нагрузки на call-processor с burst-паттерном и распределением фрод-сценариев

### Modified Capabilities

<!-- None — no existing spec-level behavior changes -->

## Impact

- **Новый сервис**: `load-simulator` (port 8087)
- **Зависимости**: Spring Boot, Spring Web, springdoc-openapi, Lombok
- **Интеграция**: HTTP POST → `call-processor:8081/api/calls` (fire-and-forget)
- **Docker Compose**: Добавить сервис в существующий `docker-compose.yml` (профиль `load-test`)
- **Kafka**: Не требует прямой зависимости (использует HTTP API call-processor)
