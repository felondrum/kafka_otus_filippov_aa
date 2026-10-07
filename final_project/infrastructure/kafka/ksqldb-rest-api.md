# ksqlDB REST API — Получение данных агрегации

## ⚠️ Важно

**REST API (`/ksql`) НЕ поддерживает `SELECT` и `PRINT` запросы.**

**НО ksqlDB CLI ПОДДЕРЖИВАЕТ `SELECT` и `PRINT`!**

## ✅ Рабочие способы получить данные

### Способ 1: ksqlDB CLI (РЕКОМЕНДУЕТСЯ для SELECT/PRINT)

```bash
# SELECT запросы
docker exec ksqldb-server ksql -e \
  "SELECT AGENTID, CALL_COUNT FROM CALLS_COMPLETED_AGG ORDER BY CALL_COUNT DESC LIMIT 10;" \
  http://localhost:8088

# SHOW запросы
docker exec ksqldb-server ksql -e "SHOW TABLES;" http://localhost:8088
```

### Способ 2: Чтение напрямую из Kafka output topics (РЕКОМЕНДУЕТСЯ для программной обработки)

ksqlDB persistent queries пишут результаты в Kafka-топики. Данные можно читать напрямую:

**Output topics:**
- `calls.completed.agg` — агрегация вызовов по агентам
- `calls.fraud-alerts.agg` — агрегация fraud alerts по номерам

### Способ 2: Использование bash-скрипта

Скрипт `ksqldb-data.sh` автоматически читает и форматирует данные:

```bash
# Показать все данные
./infrastructure/kafka/ksqldb-data.sh all

# Показать только calls aggregation
./infrastructure/kafka/ksqldb-data.sh calls

# Показать только fraud alerts
./infrastructure/kafka/ksqldb-data.sh fraud
```

### Способ 3: REST API для проверки состояния

Можно проверить, что ksqlDB работает и queries запущены:

```bash
# Информация о сервере
curl -s http://localhost:8088/info | python3 -m json.tool

# Список таблиц
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW TABLES;"}' | python3 -m json.tool

# Список активных queries
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW QUERIES;"}' | python3 -m json.tool
```

---

## 1. Чтение данных из Kafka output topics

### 1.1 calls.completed.agg (агрегация по агентам)

**Schema:**
```
AGENTID VARCHAR
CALL_COUNT BIGINT
AVG_NPS_SCORE DOUBLE
LAST_CALL_AT BIGINT (timestamp)
```

**Command:**
```bash
docker exec kafka-1 /usr/bin/kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic calls.completed.agg \
  --from-beginning \
  --max-messages 20 \
  --property print.key=true \
  --property key.separator="|" \
  --timeout-ms 10000 2>&1
```

**Пример вывода:**
```
agent-1|{"AGENTID":"agent-1","CALL_COUNT":150,"AVG_NPS_SCORE":4.25,"LAST_CALL_AT":1727912400000}
agent-2|{"AGENTID":"agent-2","CALL_COUNT":120,"AVG_NPS_SCORE":3.89,"LAST_CALL_AT":1727912400000}
```

### 1.2 calls.fraud-alerts.agg (агрегация fraud alerts)

**Schema:**
```
PHONE VARCHAR
ALERT_COUNT BIGINT
```

**Command:**
```bash
docker exec kafka-1 /usr/bin/kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic calls.fraud-alerts.agg \
  --from-beginning \
  --max-messages 20 \
  --property print.key=true \
  --property key.separator="|" \
  --timeout-ms 10000 2>&1
```

**Пример вывода:**
```
+79001000005|{"PHONE":"+79001000005","ALERT_COUNT":45}
+79001000004|{"PHONE":"+79001000004","ALERT_COUNT":38}
```

---

## 2. Python: чтение и парсинг данных

### 2.1 Базовый пример

```python
import subprocess
import json

def read_kafka_topic(topic, max_messages=20):
    """Read messages from Kafka topic and parse JSON"""
    cmd = [
        "docker", "exec", "kafka-1",
        "/usr/bin/kafka-console-consumer",
        "--bootstrap-server", "kafka-1:9092",
        "--topic", topic,
        "--from-beginning",
        "--max-messages", str(max_messages),
        "--property", "print.key=true",
        "--property", "key.separator=|",
        "--timeout-ms", "10000"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    records = []
    for line in result.stdout.strip().split('\n'):
        if '|' in line:
            key, value = line.split('|', 1)
            try:
                data = json.loads(value)
                records.append({"key": key, "value": data})
            except json.JSONDecodeError:
                pass
    
    return records

# Получить данные из calls.completed.agg
records = read_kafka_topic("calls.completed.agg", 20)

# Отсортировать по CALL_COUNT
records.sort(key=lambda x: x["value"].get("CALL_COUNT", 0), reverse=True)

# Вывести топ-5 агентов
print("Топ-5 агентов по количеству вызовов:")
for i, record in enumerate(records[:5], 1):
    value = record["value"]
    print(f"  {i}. {record['key']}: {value['CALL_COUNT']} calls, "
          f"avg NPS: {value['AVG_NPS_SCORE']:.2f}")
```

### 2.2 Анализ fraud alerts

```python
# Получить данные из calls.fraud-alerts.agg
records = read_kafka_topic("calls.fraud-alerts.agg", 100)

# Отсортировать по ALERT_COUNT
records.sort(key=lambda x: x["value"].get("ALERT_COUNT", 0), reverse=True)

print("Топ-10 номеров с fraud alerts:")
for i, record in enumerate(records[:10], 1):
    print(f"  {i}. {record['key']}: {record['value']['ALERT_COUNT']} alerts")
```

### 2.3 Статистика по всем агентам

```python
records = read_kafka_topic("calls.completed.agg", 100)

if records:
    total_calls = sum(r["value"]["CALL_COUNT"] for r in records)
    avg_nps = sum(r["value"]["AVG_NPS_SCORE"] for r in records) / len(records)
    max_calls = max(r["value"]["CALL_COUNT"] for r in records)
    min_calls = min(r["value"]["CALL_COUNT"] for r in records)
    
    print(f"Всего вызовов: {total_calls}")
    print(f"Средний NPS: {avg_nps:.2f}")
    print(f"Максимум вызовов: {max_calls}")
    print(f"Минимум вызовов: {min_calls}")
```

---

## 3. Node.js: чтение и парсинг данных

```javascript
const { execSync } = require('child_process');

function readKafkaTopic(topic, maxMessages = 20) {
    const cmd = [
        'docker', 'exec', 'kafka-1',
        '/usr/bin/kafka-console-consumer',
        '--bootstrap-server', 'kafka-1:9092',
        '--topic', topic,
        '--from-beginning',
        '--max-messages', maxMessages.toString(),
        '--property', 'print.key=true',
        '--property', 'key.separator=|',
        '--timeout-ms', '10000'
    ].join(' ');
    
    const output = execSync(cmd, { encoding: 'utf8' });
    
    const records = [];
    output.trim().split('\n').forEach(line => {
        if (line.includes('|')) {
            const [key, value] = line.split('|');
            try {
                records.push({ key, value: JSON.parse(value) });
            } catch (e) {
                // Skip invalid JSON
            }
        }
    });
    
    return records;
}

// Получить данные
const records = readKafkaTopic('calls.completed.agg', 20);

// Отсортировать и вывести топ-5
records.sort((a, b) => b.value.CALL_COUNT - a.value.CALL_COUNT);

console.log('Топ-5 агентов по количеству вызовов:');
records.slice(0, 5).forEach((record, i) => {
    console.log(`  ${i + 1}. ${record.key}: ${record.value.CALL_COUNT} calls, ` +
                `avg NPS: ${record.value.AVG_NPS_SCORE.toFixed(2)}`);
});
```

---

## 4. Bash-скрипт для быстрого получения данных

### Использование

```bash
# Сделать скрипт исполняемым
chmod +x infrastructure/kafka/ksqldb-data.sh

# Показать все данные
./infrastructure/kafka/ksqldb-data.sh all

# Показать только calls aggregation
./infrastructure/kafka/ksqldb-data.sh calls

# Показать только fraud alerts
./infrastructure/kafka/ksqldb-data.sh fraud
```

### Вывод

```
============================================
CALLS_COMPLETED_AGG (Топ агентов по вызовам)
============================================

Agent                     Calls    Avg NPS            Last Call
-----------------------------------------------------------------
agent-1                   150       4.25          1727912400000
agent-2                   120       3.89          1727912400000
agent-3                    95       4.12          1727912400000

============================================
FRAUD_ALERTS_FILTERED (Топ номеров с alerts)
============================================

Phone                         Alerts
--------------------------------------
+79001000005                     45
+79001000004                     38
+79001000001                     22
```

---

## 5. Структура данных

### CALLS_COMPLETED_AGG

| Поле | Тип | Описание |
|------|-----|----------|
| `AGENTID` | VARCHAR | ID агента |
| `CALL_COUNT` | BIGINT | Количество вызовов за окно |
| `AVG_NPS_SCORE` | DOUBLE | Средний NPS за окно |
| `LAST_CALL_AT` | BIGINT | Timestamp последнего вызова |

**Window:** TUMBLING 5 минут, grace period 1 минута  
**Output topic:** `calls.completed.agg` (6 partitions)  
**Key:** `agentId`

### FRAUD_ALERTS_FILTERED

| Поле | Тип | Описание |
|------|-----|----------|
| `PHONE` | VARCHAR | Номер телефона |
| `ALERT_COUNT` | BIGINT | Количество fraud alerts |

**Window:** HOPPING 10 минут, step 1 минута  
**Output topic:** `calls.fraud-alerts.agg` (6 partitions)  
**Key:** `phone`

---

## 6. Troubleshooting

### Нет данных в output topics

**Причина:** Persistent queries не запущены или нет данных в input topics

**Решение:**
```bash
# Проверить status queries
curl -s -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW QUERIES;"}' | grep RUNNING

# Проверить наличие данных в input topic
docker exec kafka-1 /usr/bin/kafka-topics \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --topic calls.completed
```

### Ошибка partition mismatch

**Ошибка в логах ksqldb:**
```
Existing internal topic ... has invalid partitions: expected: 6; actual: 1
```

**Решение:** Пересоздать ksqldb streams и tables:
```bash
# Остановить ksqldb-server
docker compose stop ksqldb-server

# Удалить state directory
docker volume rm final_project_fraud-detector-state

# Запустить заново
docker compose up -d ksqldb-server ksqldb-init
```

### Ошибка: "Resource not found"

**Решение:** Проверьте, что ksqldb-server запущен:
```bash
docker ps | grep ksqldb-server
curl -s http://localhost:8088/info
```

### Ошибка: "Connection refused"

**Решение:** Порт 8088 должен быть маплен в docker-compose.yml:
```yaml
ksqldb-server:
  ports:
    - "8088:8088"
```
