## Context

Текущее состояние: 3 дашборда Grafana (kafka-overview, kafka-connect-jdbc-sink, postgres-overview), JMX exporter имеет только 5 правил правил, scrape configs для микросервисов закомментированы, только reporting-nps имеет micrometer-prometheus dependency.

## Goals / Non-Goals

**Goals:**
- Исправить "No Data" на Kafka и PostgreSQL дашбордах через расширение JMX exporter правил
- Включить сбор метрик со всех 4 микросервисов
- Создать 4 per-service дашборда и 1 system overview дашборд
- Добавить template variables в существующие дашборды

**Non-Goals:**
- Настройка алертинга (alerting-rules.yml остаётся без изменений)
- Изменение Docker Compose конфигурации
- Добавление новых инструментов мониторинга (только Prometheus + Grafana)
- Настройка Grafana provisioning для dashboard folders

## Decisions

### Decision 1: JMX Exporter Rules — Pattern-based with explicit metrics

**Approach:** Расширить `jmx-exporter-config.yml` паттерн-правилами для основных групп метрик Kafka.

```
CURRENT (5 rules)        →    EXPANDED (15+ rules)
┌─────────────────────┐     ┌──────────────────────────────┐
│ request-handler     │     │ 1. broker topic metrics       │
│ replica-manager     │     │    (messages, bytes in/out)   │
│ broker-log-manager  │     │ 2. network metrics            │
│ catch-all JMX       │     │    (bytesinpersec, bytesout)  │
└─────────────────────┘     │ 3. request latency            │
                            │ 4. consumer group lag         │
                            │ 5. partition leadership       │
                            │ 6. catch-all JMX (keep)       │
                            └──────────────────────────────┘
```

**Rationale:** Паттерн-правила (`kafka.<type>`) автоматически экспортируют все метрики из каждого JMX MBean. Это надёжнее, чем хардкодить каждую метрику, и покрывает новые метрики при обновлении Kafka.

**Alternatives considered:**
- Хардкодить каждую метрику вручную — хрупко, требует обновления при каждом обновлении Kafka
- Использовать сторонний JMX exporter config — нет гарантии совместимости с текущей версией (7.6.1)

### Decision 2: Micrometer dependencies — Minimal addition

**Approach:** Добавить только `micrometer-registry-prometheus` dependency в build.gradle.kts каждого сервиса. Spring Boot Actuator уже есть во всех сервисах, поэтому достаточно добавить prometheus registry.

```
call-processor:        + micrometer-registry-prometheus  (actuator: ✅ already)
fraud-detector:        + micrometer-registry-prometheus  (actuator: ✅ already)
transcription-analyzer:+ micrometer-registry-prometheus  (actuator: ✅ already)
reporting-nps:         (already has it)
```

**Rationale:** Spring Boot Actuator уже добавлен во все 4 сервиса. Добавление только prometheus registry минимально и не меняет API endpoints.

**Alternatives considered:**
- Добавить full micrometer stack — избыточно, actuator уже есть
- Использовать external metrics exporter — не нужно, Spring Boot встроенный достаточно

### Decision 3: Dashboard JSON — Manual creation with Grafana API reference

**Approach:** Создать JSON файлы дашбордов вручную, используя структуру существующих дашбордов как шаблон. Для system overview дашборда использовать комбинированные панели: stat panels для health status, timeseries для throughput, gauge для lag.

**Dashboard structure:**

```
System Overview Dashboard (14 panels):
┌────────────────────────────────────────────────────────────┐
│  [Service Health] [Broker Count] [DB Status] [Connect]     │  ← Row 1: 4 stat panels
├────────────────────────────────────────────────────────────┤
│  Topic Throughput (messages/sec) — grouped by topic        │  ← Row 2: timeseries
├────────────────────────────────────────────────────────────┤
│  DB Write Rates (INSERT/UPDATE/sec) — grouped by table     │  ← Row 3: timeseries
├────────────────────────────────────────────────────────────┤
│  Consumer Group Lag — grouped by group                     │  ← Row 4: timeseries
├────────────────────────────────────────────────────────────┤
│  Network Throughput (Kafka In/Out)                         │  ← Row 5: timeseries
├────────────────────────────────────────────────────────────┤
│  Request Latency (P50/P95) — per service                   │  ← Row 6: timeseries
└────────────────────────────────────────────────────────────┘

Per-Service Dashboard (10 panels each):
┌─────────────────────────────────────────────┐
│  [JVM Memory] [CPU] [Threads] [GC]          │  ← Row 1: JVM stats
├─────────────────────────────────────────────┤
│  Kafka Producer/Consumer metrics            │  ← Row 2: throughput
├─────────────────────────────────────────────┤
│  HTTP Request Rate + Latency                │  ← Row 3: API perf
├─────────────────────────────────────────────┤
│  Custom business metrics (service-specific) │  ← Row 4: domain
└─────────────────────────────────────────────┘
```

**Rationale:** Структура с row-by-row grouping обеспечивает читаемость. Stat panels для quick health check, timeseries для trend analysis.

**Alternatives considered:**
- Использовать Grafana provisioning для import JSON — уже работает через volumes
- Использовать Terraform Grafana provider — overkill для локального dev/qa

### Decision 4: Template Variables — Simple dropdown with query

**Approach:** Добавить template variables через `templating.list` в JSON дашбордов.

```json
"templating": {
  "list": [
    {
      "name": "broker",
      "type": "query",
      "query": "label_values(kafka_server_replicamanager_partitioncount, instance)",
      "datasource": "Prometheus"
    },
    {
      "name": "connector",
      "type": "query",
      "query": "label_values(kafka_connect_connector_connector_status, connector)",
      "datasource": "Prometheus"
    },
    {
      "name": "database",
      "type": "query",
      "query": "label_values(pg_stat_activity_count, datname)",
      "datasource": "Prometheus"
    }
  ]
}
```

**Rationale:** Query-based variables автоматически подхватывают новые значения без ручного обновления.

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| JMX exporter config expansion increases memory usage | Low — JMX exporter уже использует ~50MB, расширение добавит <10MB | Monitor JMX exporter heap usage |
| New scrape targets increase Prometheus storage | Low — 4 сервиса × 15s interval ≈ +200 samples/sec, ~17MB/day | Acceptable for current retention (15d) |
| Dashboard JSON files are large and hard to maintain | Medium — each dashboard ~10-15KB JSON | Use version control, consider Grafana provisioning from repo |
| Micrometer adds ~500 default metrics per service | Low — стандартное поведение Spring Boot, можно фильтровать | Review metrics via `/actuator/prometheus` после деплоя |

## Migration Plan

**Step 1:** Update JMX exporter config + Prometheus scrape configs
**Step 2:** Add micrometer dependencies to 3 build.gradle.kts
**Step 3:** Create 4 per-service dashboards + 1 system overview
**Step 4:** Fix existing dashboards with template variables
**Step 5:** Deploy with `docker compose --profile full up -d`
**Step 6:** Validate all dashboards show data in Grafana

**Rollback:** Откат к предыдущей версии файлов (git revert) — no schema changes, no data migration.

## Open Questions

- Нужно ли добавить custom business metrics (например, количество обработанных звонков, фрод-алертов, NPS-отчётов)? Это можно добавить в следующей итерации через Counter/Timer метрики в коде сервисов.
- Нужны ли dashboard folders для организации дашбордов (Infrastructure, Services, System)? Можно добавить в следующей итерации через Grafana provisioning folders.
