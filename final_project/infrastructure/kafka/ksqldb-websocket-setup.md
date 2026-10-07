# ksqlDB REST API — Настройка WebSocket для SELECT/PRINT

## ⚠️ Текущее состояние

**ksqlDB 7.6.1 НЕ поддерживает `SELECT` и `PRINT` через REST API (`/ksql`).**

Эти команды требуют **WebSocket endpoint**, который:
- Не включён по умолчанию в Confluent ksqlDB 7.6.1
- Требует отдельной настройки в docker-compose.yml
- Работает только через WebSocket协议 (`ws://`), не через HTTP

## ✅ Рабочие способы (без WebSocket)

### Способ 1: Чтение из Kafka output topics (РЕКОМЕНДУЕТСЯ)

ksqlDB persistent queries пишут результаты в Kafka-топики. Данные можно читать напрямую:

```bash
# calls.completed.agg
docker exec kafka-1 /usr/bin/kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic calls.completed.agg \
  --from-beginning \
  --max-messages 20 \
  --property print.key=true \
  --property key.separator="|" \
  --timeout-ms 10000

# calls.fraud-alerts.agg
docker exec kafka-1 /usr/bin/kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic calls.fraud-alerts.agg \
  --from-beginning \
  --max-messages 20 \
  --property print.key=true \
  --property key.separator="|" \
  --timeout-ms 10000
```

### Способ 2: Bash-скрипт

```bash
./infrastructure/kafka/ksqldb-data.sh all
```

### Способ 3: REST API для проверки состояния

```bash
# Проверить, что ksqlDB работает
curl -s http://localhost:8088/info

# Показать таблицы
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW TABLES;"}'

# Показать queries
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW QUERIES;"}'
```

---

## 🔧 Как настроить WebSocket endpoint (для будущих версий)

### Вариант 1: Confluent Cloud (рекомендуется для production)

Confluent Cloud предоставляет WebSocket endpoint автоматически. Не требует настройки.

### Вариант 2: Self-hosted ksqlDB с WebSocket

**Шаг 1: Добавить WebSocket listener в docker-compose.yml**

```yaml
services:
  ksqldb-server:
    image: confluentinc/cp-ksqldb-server:7.6.1
    ports:
      - "8088:8088"
      - "9091:9091"  # WebSocket endpoint
    environment:
      KSQL_LISTENERS: http://0.0.0.0:8088,ws://0.0.0.0:9091
      KSQL_HANDLER_WEBSOCKET_ENABLED: "true"
      KSQL_HANDLER_WEBSOCKET_PATH: "/query"
      KSQL_HANDLER_WEBSOCKET_ORIGIN: "*"
      KSQL_HANDLER_CORS_ALLOWED_ORIGINS: "*"
```

**Проблема:** Confluent ksqlDB 7.6.1 **не поддерживает** `ws://` в `KSQL_LISTENERS`.

**Шаг 2: Использовать альтернативный образ**

```yaml
services:
  ksqldb-server:
    # Использовать open-source версию от Confluent
    image: confluentinc/cp-ksqldb-server:7.6.1
    ports:
      - "8088:8088"
    environment:
      KSQL_LISTENERS: http://0.0.0.0:8088
      # WebSocket включён через system property
      KSQL_OPTS: "-Dksql.ws.enabled=true -Dksql.ws.path=/query"
```

**Шаг 3: Перезапустить ksqldb-server**

```bash
docker compose --profile full up -d --force-recreate ksqldb-server
```

**Шаг 4: Проверить WebSocket endpoint**

```bash
# Использовать wscat (npm install -g wscat)
wscat -c ws://localhost:8088/query

# Отправить запрос
{"ksql": "SELECT * FROM CALLS_COMPLETED_AGG LIMIT 10;"}
```

### Вариант 3: Использовать ksqlDB CLI

```bash
# Запустить ksqlDB CLI внутри контейнера
docker exec -it ksqldb-server ksql http://localhost:8088

# Выполнить SELECT
SELECT AGENTID, CALL_COUNT FROM CALLS_COMPLETED_AGG LIMIT 10;

# Выйти
EXIT;
```

### Вариант 4: Использовать ksqlDB REST API с polling

Вместо WebSocket можно использовать REST API с periodic polling:

```bash
# 1. Создать persistent query
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{
    "ksql": "CREATE STREAM CALLS_COMPLETED_STREAM AS SELECT AGENTID, CALL_COUNT FROM CALLS_COMPLETED_AGG LIMIT 10;",
    "streamsProperties": {}
  }'

# 2. Читать из output topic
docker exec kafka-1 /usr/bin/kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic <output-topic> \
  --from-beginning \
  --max-messages 10 \
  --timeout-ms 5000
```

---

## 📊 Сравнение способов

| Способ | SELECT/PRINT | Сложность | Production-ready |
|--------|--------------|-----------|------------------|
| **Чтение из Kafka topics** | ❌ (но данные доступны) | ⭐ Легко | ✅ Да |
| **Bash-скрипт** | ❌ (но данные доступны) | ⭐ Легко | ✅ Да |
| **REST API + polling** | ⚠️ Ограниченно | ⭐⭐ Средне | ✅ Да |
| **WebSocket endpoint** | ✅ Да | ⭐⭐⭐ Сложно | ⚠️ Требует настройки |
| **ksqlDB CLI** | ✅ Да | ⭐ Легко | ❌ Только для dev |

---

## 🎯 Рекомендация

**Для production используйте чтение из Kafka topics.**

Почему:
1. ✅ ksqlDB уже пишет данные в output topics
2. ✅ Нет необходимости настраивать WebSocket
3. ✅ Данные доступны в любом формате (JSON, Avro)
4. ✅ Можно читать через любой язык/инструмент
5. ✅ Не требует дополнительных сервисов

**Для development/ad-hoc запросов используйте ksqlDB CLI.**

```bash
docker exec -it ksqldb-server ksql http://localhost:8088
```

---

## 🔍 Troubleshooting

### Ошибка: "SELECT requires WebSocket"

**Причина:** ksqlDB 7.6.1 не поддерживает SELECT через REST API.

**Решение:** Используйте один из способов выше.

### Ошибка: "Connection refused" на WebSocket

**Причина:** WebSocket endpoint не настроен.

**Решение:** Добавьте `KSQL_HANDLER_WEBSOCKET_ENABLED=true` в docker-compose.yml.

### Нет данных в output topics

**Причина:** Persistent queries не запущены.

**Решение:**
```bash
# Проверить status queries
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW QUERIES;"}'

# Перезапустить ksqldb-init
docker compose up -d ksqldb-init
```
