## Why

Банковский колл-центр нуждается в событийно-ориентированной платформе для обработки звонков в реальном времени. Первый шаг — развёртывание инфраструктуры: Kafka-кластер, PostgreSQL, Schema Registry и инструменты мониторинга. Без этого невозможна работа ни одного микросервиса.

## What Changes

- Docker Compose конфигурация для локальной разработки с ARM64-совместимыми образами
- Kafka-кластер из 3 брокеров (KRaft режим, SASL/PLAIN аутентификация)
- Schema Registry для хранения и валидации Avro-схем
- PostgreSQL 15 для аналитического хранилища
- Prometheus + Grafana + Kafdrop для мониторинга и визуализации
- 8 Kafka-топиков с репликацией 3 и 6 партициями
- Docker Compose profiles для разных сценариев (full, core, monitoring, load-test)
- Makefile для управления жизненным циклом инфраструктуры
- Health checks и зависимости между сервисами

## Capabilities

### New Capabilities

- `infrastructure`: развёртывание и управление инфраструктурой платформы (Kafka, PostgreSQL, мониторинг)

### Modified Capabilities

<!-- None yet -->

## Impact

- **Зависимости:** Java 21, Spring Boot 3, Docker, Docker Compose, Apache Kafka 7.6.1, PostgreSQL 15
- **API:** HTTP endpoints для Schema Registry (8085), Prometheus (9090), Grafana (3000), Kafdrop (9000)
- **Системы:** Docker Network `call-platform-net`, 6 volumes для персистентных данных
- **Следующие changes:** call-processor, fraud-detector, transcription-analyzer, reporting-nps зависят от этого change
