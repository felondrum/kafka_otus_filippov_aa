# Call Platform — Kafka Infrastructure

Полный стек микросервисов на базе Apache Kafka в KRaft mode (без ZooKeeper).

## Архитектура

**10 сервисов:**
- **Kafka Cluster** (3 брокера: kafka-1, kafka-2, kafka-3) — KRaft mode, SASL/PLAIN на EXTERNAL listener (port 9093), PLAINTEXT на INTERNAL (port 9092)
- **Schema Registry** — port 8085 (external), BACKWARD compatibility
- **PostgreSQL 15** — port 5432, tables: `call_metadata`, `call_transcriptions`
- **Kafka Connect** — port 8086 (external, to avoid conflict with transcription-analyzer)
- **Prometheus** — port 9090, scrape targets: Kafka (JMX), PostgreSQL, infrastructure
- **Grafana** — port 3000, dashboards: Kafka Cluster Overview, PostgreSQL Overview
- **Kafdrop** — port 9000, Kafka topic viewer
- **PostgreSQL Exporter** — port 9187

## Быстрый старт

### 1. Развернуть весь стек
```bash
make deploy
```
Или с конкретным profile:
```bash
make PROFILE=core deploy    # Только Kafka + Schema Registry + PostgreSQL
make PROFILE=monitoring deploy  # Только Prometheus + Grafana + Kafdrop
make PROFILE=load-test deploy  # Полный стек для load testing
```

### 2. Проверить здоровье сервисов
```bash
make health
```

### 3. Инициализировать Kafka темы
```bash
make init-topics
```
Создаёт 10 тем:
- **Stream topics** (partitions=6, replication=3):
  - `calls.completed`, `calls.fraud-alerts`, `transcription.raw`, `transcription.summary`, `transcription.enriched`, `calls.dlq`
- **Compacted topics** (cleanup.policy=compact):
  - `calls.metadata`, `customers.profile`
- **ksqlDB output topics**:
  - `calls.completed.agg`, `calls.fraud-alerts.agg`
- **DLQ topic** (partitions=3):
  - `transcription.enriched.dlq`

### 4. Посмотреть логи
```bash
make logs
```

### 5. Остановить стек
```bash
make stop
```

### 6. Полная очистка (удалить volumes)
```bash
make clean
```

## Сервисы и порты

| Сервис | Port | URL |
|--------|------|-----|
| Kafka (broker-1) | 9092 (INTERNAL), 9093 (EXTERNAL/SASL) | `localhost:9092` (internal), `localhost:9093` (external) |
| Kafka (broker-2) | 9094 (INTERNAL), 9095 (EXTERNAL) | `localhost:9094` |
| Kafka (broker-3) | 9096 (INTERNAL), 9097 (EXTERNAL) | `localhost:9096` |
| Schema Registry | 8085 | http://localhost:8085 |
| PostgreSQL | 5432 | `postgres:5432` (internal), `localhost:5432` (external) |
| Kafka Connect | 8086 | http://localhost:8086 |
| Prometheus | 9090 | http://localhost:9090 |
| Grafana | 3000 | http://localhost:3000 (admin/admin-secret) |
| Kafdrop | 9000 | http://localhost:9000 |
| PostgreSQL Exporter | 9187 | http://localhost:9187 |

## SASL аутентификация

EXTERNAL listener использует SASL/PLAIN:
- **Username**: `admin`
- **Password**: `admin-secret`

Для подключения извне используйте port 9093 с SASL credentials.

## Grafana дашборды

- **Kafka Cluster Overview** — метрики брокеров, партиции, репликация, under-replicated partitions
- **PostgreSQL Overview** — подключения, запросы, размеры таблиц

Доступ: http://localhost:3000 (admin/admin-secret)

## Kafka Connect

Kafka Connect запущен в distributed mode на port 8086 (external).

Пат для плагинов: `/etc/kafka-connect/jars`

Темы Connect:
- `connect-configs` (compact)
- `connect-offsets` (compact)
- `connect-status` (compact)

## Мониторинг

Prometheus собирает метрики с:
- Kafka brokers (JMX Exporter)
- PostgreSQL (postgres_exporter)
- Infrastructure services (Grafana, Kafdrop, Schema Registry, Kafka Connect)

Конфигурация: `infrastructure/monitoring/prometheus.yml`

## Development

### Пересоздать темы
```bash
make init-topics
```

### Проверить темы в Kafdrop
Открыть http://localhost:9000

### Посмотреть метрики в Prometheus
- http://localhost:9090
- Query: `up` (статус сервисов), `kafka_server_replicamanager_partitioncount` (количество партиций)

### Подключиться к PostgreSQL
```bash
docker exec -it postgres psql -U postgres -d call_platform
```

## Профили Docker Compose

- **full** — все сервисы (по умолчанию)
- **core** — Kafka + Schema Registry + PostgreSQL + Kafka Connect
- **monitoring** — Prometheus + Grafana + Kafdrop + PostgreSQL Exporter
- **load-test** — полный стек для load testing
