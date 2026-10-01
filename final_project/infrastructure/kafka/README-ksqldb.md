# ksqlDB — Quick Reference

## Быстрый старт

```bash
# Полная проверка статуса
./infrastructure/kafka/ksqldb-checks.sh all

# Показать агрегацию звонков (processing status)
./infrastructure/kafka/ksqldb-checks.sh agg-calls

# Показать агрегацию фрода (processing status)
./infrastructure/kafka/ksqldb-checks.sh agg-fraud

# Показать активные queries
./infrastructure/kafka/ksqldb-checks.sh queries
```

## curl-команды (copy & paste)

Смотря `infrastructure/kafka/ksqldb-curl-examples.sh` — все команды с комментариями.

### Самые популярные:

```bash
# Server info
curl http://localhost:8088/info

# Streams
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW STREAMS;"}' | python3 -m json.tool

# Tables
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW TABLES;"}' | python3 -m json.tool

# Queries
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW QUERIES;"}' | python3 -m json.tool

# Extended query info (with offsets)
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW QUERIES EXTENDED;"}' | python3 -m json.tool
```

## Важное примечание

**PRINT и SELECT запросы НЕ работают через REST API!**

ksqlDB требует WebSocket endpoint (`ws://localhost:8088/query`) для:
- `PRINT` — просмотр данных в реальном времени
- `SELECT` — ad-hoc запросы к таблицам

### Альтернативы через REST API:

| Что хотите проверить | REST API команда |
|---------------------|------------------|
| Список streams | `SHOW STREAMS;` |
| Список tables | `SHOW TABLES;` |
| Статус queries | `SHOW QUERIES;` |
| Processing offsets | `SHOW QUERIES EXTENDED;` |
| Список topics | `SHOW TOPICS;` |

### Для PRINT/SELECT используйте:

**WebSocket клиент (wscat):**
```bash
npm install -g wscat
wscat -c ws://localhost:8088/query
> {"ksql": "SELECT * FROM CALLS_COMPLETED_AGG;"}
```

**Или используйте convenience script:**
```bash
./infrastructure/kafka/ksqldb-checks.sh agg-calls
./infrastructure/kafka/ksqldb-checks.sh agg-fraud
```

## Файлы

| Файл | Назначение |
|------|-----------|
| `infrastructure/kafka/ksqldb-checks.sh` | Интерактивный скрипт с меню команд |
| `infrastructure/kafka/ksqldb-curl-examples.sh` | Все curl-команды для копирования |
| `infrastructure/kafka/ksqldb-init.sh` | Автоинициализация streams/tables |
| `infrastructure/kafka/ksqldb-verify.sh` | Полный сценарий проверки + генерация тестовых данных |

## Output Topics

### calls.completed.agg
- **Key:** agentId (STRING)
- **Value:** call_count (BIGINT), avg_nps_score (DOUBLE), last_call_at (LONG)
- **Window:** TUMBLING 5min, GRACE 1min

### calls.fraud-alerts.agg
- **Key:** phone (STRING)
- **Value:** alert_count (BIGINT)
- **Type:** GROUP BY (no window)

## Проверка корректности

```bash
# 1. Server running
curl http://localhost:8088/info
# Ожидаем: "serverStatus": "RUNNING"

# 2. Streams created
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW STREAMS;"}' | grep '"name"'
# Ожидаем: CALLS_COMPLETED_EXT, CALLS_FRAUD_ALERTS_EXT

# 3. Tables created
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW TABLES;"}' | grep '"name"'
# Ожидаем: CALLS_COMPLETED_AGG, FRAUD_ALERTS_FILTERED

# 4. Queries running
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW QUERIES;"}' | grep '"RUNNING"'
# Ожидаем: "RUNNING": 2

# 5. Processing data (offsets)
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW QUERIES EXTENDED;"}' | python3 -m json.tool
# Ожидаем: committed offsets > 0 для обоих queries
```
