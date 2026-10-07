# ksqlDB — Quick Reference

## 🎯 Быстрый старт

### 1. SELECT запросы (ksqlDB CLI)

```bash
# Топ агентов
docker exec ksqldb-server ksql -e \
  "SELECT AGENTID, CALL_COUNT FROM CALLS_COMPLETED_AGG ORDER BY CALL_COUNT DESC LIMIT 10;" \
  http://localhost:8088

# Топ номеров с fraud alerts
docker exec ksqldb-server ksql -e \
  "SELECT PHONE, ALERT_COUNT FROM FRAUD_ALERTS_FILTERED ORDER BY ALERT_COUNT DESC LIMIT 10;" \
  http://localhost:8088
```

### 2. Проверка состояния (REST API)

```bash
# Server info
curl -s http://localhost:8088/info

# Tables
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW TABLES;"}'

# Queries
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW QUERIES;"}'
```

### 3. Чтение из Kafka topics

```bash
# calls.completed.agg
docker exec kafka-1 /usr/bin/kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic calls.completed.agg \
  --from-beginning --max-messages 20 \
  --property print.key=true --property key.separator="|" \
  --timeout-ms 10000

# calls.fraud-alerts.agg
docker exec kafka-1 /usr/bin/kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic calls.fraud-alerts.agg \
  --from-beginning --max-messages 20 \
  --property print.key=true --property key.separator="|" \
  --timeout-ms 10000
```

### 4. Использовать скрипт

```bash
./infrastructure/kafka/ksqldb-data.sh all
```

---

## Output Topics

| Topic | Schema |
|-------|--------|
| `calls.completed.agg` | `AGENTID`, `CALL_COUNT`, `AVG_NPS_SCORE`, `LAST_CALL_AT` |
| `calls.fraud-alerts.agg` | `PHONE`, `ALERT_COUNT` |

---

## Troubleshooting

| Проблема | Решение |
|----------|---------|
| Нет данных | Запустите load-simulator: `curl -X POST http://localhost:8087/api/load/start -d '{"totalCalls":20,"durationMinutes":1}'` |
| Partition mismatch | `docker volume rm final_project_fraud-detector-state && docker compose up -d ksqldb-server ksqldb-init` |
| ksqldb-server не запущен | `docker ps | grep ksqldb-server` |
