## Why

Дашборды Grafana для Kafka и PostgreSQL показывают "No Data" из-за неполного JMX exporter конфигура, а метрики микросервисов не собираются вовсе — scrape configs закомментированы, зависимости micrometer отсутствуют в 3 из 4 сервисов. Нет дашбордов для сервисов и общего view системы, что делает невозможным мониторинг состояния платформы.

## What Changes

- **Fix JMX Exporter Config** — расширить `jmx-exporter-config.yml` правилами для всех метрик Kafka (broker topic metrics, network metrics, request latency, consumer group metrics)
- **Add Micrometer dependencies** — добавить `micrometer-registry-prometheus` в `call-processor`, `fraud-detector`, `transcription-analyzer` (reporting-nps уже имеет)
- **Enable Prometheus scrape configs** — раскомментировать и добавить все 4 микросервиса в `prometheus.yml`
- **Create per-service dashboards** — 4 новых дашборда Grafana (call-processor, fraud-detector, transcription-analyzer, reporting-nps)
- **Create system overview dashboard** — единый дашборд для мониторинга потока данных через всю систему (topik throughput, DB write rates, service health)
- **Fix existing dashboards** — добавить template variables вместо хардкода в `kafka-connect-jdbc-sink.json` и `postgres-overview.json`

## Capabilities

### New Capabilities

- `monitoring/metrics-collection`: Требования к сбору метрик из всех микросервисов (Spring Boot Actuator + Micrometer Prometheus), Kafka brokers (JMX Exporter), PostgreSQL (postgres_exporter), и Kafka Connect

- `monitoring/grafana-dashboards`: Требования к Grafana дашбордам — per-service (4 шт), system overview (1 шт), и исправление существующих (Kafka, Kafka Connect, PostgreSQL)

## Impact

**Config files:**
- `infrastructure/kafka/jmx-exporter-config.yml` — расширение правил
- `infrastructure/monitoring/prometheus.yml` — раскомментировать + добавить сервисы
- `infrastructure/monitoring/grafana/dashboards/*.json` — 4 новых + 2 исправленных

**Build files:**
- `call-processor/build.gradle.kts` — добавить micrometer-prometheus
- `fraud-detector/build.gradle.kts` — добавить micrometer-prometheus
- `transcription-analyzer/build.gradle.kts` — добавить micrometer-prometheus

**Infrastructure:**
- Docker Compose — no changes (ports уже открыты)
- Prometheus scrape interval — no changes (15s достаточно)
