## Why

Текущий e2e-bootstrap.sh тестирует call-processor, reporting-nps и fraud-detector, но полностью покрывает только HTTP API и PostgreSQL через прямой INSERT. Kafka Connect JDBC Sink — primary writer для `transcription.enriched` → PostgreSQL — не имеет e2e тестов: нет проверки JSON-конвертации, transformations (ExtractField, ReplaceString), upsert semantics, retry/DLQ, offset recovery. Без тестов невозможно гарантировать что Kafka Connect корректно синхронизирует данные.

## What Changes

- **Изолированный e2e тестовый скрипт** `infrastructure/kafka-connect-e2e-test.sh` — 22 теста в 7 фазах
- **Прямая генерация JSON-событий** через `kafka-console-producer` с `--property parse.key=true --property key.separator=:` — не зависит от transcription-analyzer
- **7 фаз тестирования**:
  1. Connector Health & Config (5 тестов)
  2. Data Flow: JSON → PostgreSQL (4 теста)
  3. Transformations: ExtractField + ReplaceString (3 теста)
  4. Upsert Semantics (2 теста)
  5. Retry & DLQ (3 теста)
  6. Offset Recovery (2 теста)
  7. DualWriter Fallback (2 теста)
- **Интеграция в Makefile** — новый target `make kafka-connect-e2e`
- **Возврат exit code 0/1** для CI/CD

## Capabilities

### New Capabilities

- `kafka-connect-e2e-tests`: Полное e2e тестирование Kafka Connect JDBC Sink connector — health checks, data flow, transformations, upsert, retry/DLQ, offset recovery, DualWriter fallback

### Modified Capabilities

<!-- No existing spec requirements are changed. This is a pure addition of test infrastructure. -->

## Impact

**Affected files:**
- `infrastructure/kafka-connect-e2e-test.sh` — новый скрипт e2e тестов (~400 строк)
- `Makefile` — добавление target `kafka-connect-e2e`
- `openspec/changes/kafka-connect-e2e-tests/specs/kafka-connect-e2e-tests/spec.md` — spec для тестов
- `openspec/changes/kafka-connect-e2e-tests/design.md` — design decisions
- `openspec/changes/kafka-connect-e2e-tests/tasks.md` — implementation tasks

**Dependencies:**
- Зависит от `kafka-connect-jdbc-sink` change (connector должен быть развёрнут)
- Зависит от `infrastructure` change (Kafka, PostgreSQL, Schema Registry запущены)
- Требует Confluent Platform CLI tools (kafka-console-producer)

**Breaking changes:** None. Pure addition of test infrastructure.
