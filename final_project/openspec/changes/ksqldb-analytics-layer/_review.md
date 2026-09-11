# ksqldb-analytics-layer — Change Review

**Дата:** 2026-09-10
**Рецензируемые артефакты:** proposal.md, spec.md, design.md, tasks.md
**Критерии:** логика, консистентность, корректность, связь с другими модулями

---

## Summary

| Категория | Критических | Высоких | Средних | Низких | Итого |
|-----------|:-----------:|:-------:|:-------:|:------:|:-----:|
| **Блокирующие** | 2 | 1 | — | — | 3 |
| **Высокие** | — | 2 | — | — | 2 |
| **Средние** | — | — | 3 | — | 3 |
| **Низкие** | — | — | 2 | 4 | 6 |
| **Итого** | **2** | **3** | **5** | **4** | **14** |

**Вердикт:** **NEEDS FIXES** — 3 блокирующих issue требуют исправления до начала реализации.

---

## Исправления

| № | Issue | Статус | Где исправлено |
|---|-------|--------|----------------|
| R-1 | B1: Schema Registry port 8081→8085 | ✅ FIXED | `ARCHITECTURE/03-container-architecture.md` |
| R-2 | B2: ksqlDB image cli→server | ✅ FIXED | `ARCHITECTURE/03-container-architecture.md` |
| R-3 | B3: SASL auth not configured | ✅ FIXED | `design.md` (Decision 7), `tasks.md` (1.2), container-architecture diagram |
| R-4 | H1: unique_phones meaningless | ✅ FIXED | `spec.md`, `design.md`, `tasks.md` — удалён |
| R-5 | H2: output topics missing in infra | ✅ FIXED | `infrastructure/spec.md` — добавлен scenario |
| R-6 | H3: proposal agentId vs spec phone | ✅ FIXED | `proposal.md` — заменён на "per phone" |
| R-7 | M1: transcription.enriched out of scope | ✅ FIXED | `tasks.md` (1.5) — удалён |
| R-8 | M2: PARTITIONS=2 vs 6 | ✅ FIXED | `design.md` (Migration Plan) — PARTITIONS=6 |
| R-9 | M3: docs target wrong file | ✅ FIXED | `tasks.md` (7.3) → `ARCHITECTURE/04-data-flow.md` |
| R-10 | L1: ksqldb-cli in proposal | ✅ FIXED | `proposal.md` — удалено |
| R-11 | L3: query timeout not configured | ✅ FIXED | `tasks.md` (1.6) — добавлена |
| R-12 | L4: no idempotency scenario | ✅ FIXED | `spec.md` — добавлен scenario |
| R-13 | L5: no late event scenario | ✅ FIXED | `spec.md` — добавлен scenario |
| R-14 | L6: avg_nps_score precision | ✅ FIXED | `spec.md` — добавлена note |
| R-15 | H1 residual: scenario text still references unique_phones | ✅ FIXED | `spec.md` — scenario text cleaned |
| R-16 | L2: CI/CD rationale missing | ✅ FIXED | `design.md` (Non-Goals) — добавлен rationale |

**Итоговый вердикт: APPROVED** — все 16 замечаний исправлены, блокирующих issues нет.

---

## B1 [BLOCKING] Schema Registry port inconsistency — container-architecture vs spec

**Где:** `ARCHITECTURE/03-container-architecture.md` (таблица контейнеров, Schema Registry: 8081) vs `spec.md` / `design.md` (schema.registry.url=http://schema-registry:8085)

**Что:** В `ARCHITECTURE/03-container-architecture.md` Schema Registry указан на порту **8081**. В spec и design ksqldb-analytics-layer — порт **8085**. `infrastructure` change также определяет порт **8085**.

**Почему неправильно:** ksqlDB не сможет подключиться к Schema Registry — Avro events не будут десериализованы, streams не запустятся.

**Рекомендация:** Исправить `ARCHITECTURE/03-container-architecture.md` — Schema Registry порт 8081 → 8085. Это кросс-чейндж inconsistency, уже зафиксированный в failure-log R-5.

---

## B2 [BLOCKING] ksqlDB container image mismatch

**Где:** `ARCHITECTURE/03-container-architecture.md` (таблица контейнеров)

**Что:** В container-architecture ksqlDB использует образ `confluentinc/cp-ksqldb-cli:latest`. В design.md и tasks.md — ksqlDB server в embedded mode.

**Почему неправильно:** `cp-ksqldb-cli` — это CLI-образ, он не предоставляет REST API (port 8088). Для embedded mode с REST API нужен `cp-ksqldb-server:latest`. Tasks 1.1 также ссылаются на `confluentinc/cp-ksqldb-server:latest`, что противоречит architecture diagram.

**Рекомендация:** Исправить `ARCHITECTURE/03-container-architecture.md` — образ ksqlDB → `confluentinc/cp-ksqldb-server:latest`, роль → "SQL-обработка потоков + REST API".

---

## B3 [BLOCKING] SASL authentication not configured for ksqlDB → Kafka

**Где:** `ARCHITECTURE/03-container-architecture.md` (диаграмма связей: ksqlDB ──[HTTP]──▶ Kafka Cluster) vs `ARCHITECTURE/03-container-architecture.md` (секция 3.2: Kafka brokers require SASL/PLAIN)

**Что:** ksqlDB connects to Kafka via plain HTTP in the diagram. Kafka cluster requires SASL/PLAIN authentication (per infrastructure spec).

**Почему неправильно:** ksqlDB не подключится к Kafka без SASL credentials — streams не запустятся, вся функциональность не работает.

**Рекомендация:** Добавить в design.md Decision про SASL configuration для ksqlDB (sasl.jaas.config, security.protocol=SASL_PLAINTEXT). Добавить в tasks 1.2 настройку SASL credentials. Обновить диаграм��у связей в container-architecture: ksqlDB ──[Kafka SASL]──▶ Kafka Cluster.

---

## H1 [HIGH] `unique_phones` metric is meaningless in fraud_alerts_filtered

**Где:** `spec.md` (Requirement: fraud_alerts_filtered — "unique_phones: COUNT(DISTINCT phone)"), `design.md` (Migration Plan Phase 2 SQL — "COUNT(DISTINCT phone) AS unique_phones"), `tasks.md` (3.3 — "unique_phones (COUNT DISTINCT)")

**Что:** Stream группируется по `phone, pattern, severity`. Внутри каждой группы phone = константа. COUNT(DISTINCT phone) = всегда 1.

**Почему неправильно:** Метрика "unique_phones" вводит в заблуждение. В текущей группировке уникальных phone нет — каждый group key уже уникален по phone. Если цель — уникальные phone per pattern+severity (без группировки по phone), то GROUP BY должен быть только по `pattern, severity`.

**Рекомендация:** Выбрать одну из стратегий:
- **Option A (remove):** Удалить `unique_phones` из spec, output schema, tasks. Оставить только `alert_count`.

---

## H2 [HIGH] Output topics not defined in infrastructure spec

**Где:** `openspec/changes/infrastructure/specs/infrastructure/spec.md` (Requirement: Kafka Topics — 8 topics listed) vs `design.md` (Decision 6 — "ksqlDB auto-creates output topics")

**Что:** Infrastructure spec определяет 8 топиков. ksqlDB output topics (`calls.completed.agg`, `calls.fraud-alerts.agg`) не указаны.

**Почему неправильно:** ksqlDB auto-creates topics с default partitioning (1 partition по умолчанию), что противоречит infrastructure standard (6 partitions). Auto-created topics также не получат replication factor 3, что критично для production.

**Рекомендация:** Добавить 2 output topics в infrastructure spec:
- `calls.completed.agg` — stream topic, Avro, key=agentId, partitions=6, replication=3
- `calls.fraud-alerts.agg` — stream topic, Avro, key=phone:pattern:severity, partitions=6, replication=3

---

## H3 [HIGH] Proposal mentions agentId for fraud alerts aggregation (outdated)

**Где:** `proposal.md` ("Count of alerts per agentId, pattern, severity") vs `spec.md` (correctly uses phone)

**Что:** Proposal говорит "per agentId, pattern, severity" для fraud alerts. Spec правильно использует phone (после R-6 в failure log).

**Почему неправильно:** Proposal — entrypoint для review, должен быть консистентен со spec. Inconsistency запутает reviewer и future maintainers.

**Рекомендация:** Обновить proposal.md — заменить "per agentId" на "per phone" в секции "What Changes".

---

## M1 [MEDIUM] Task 1.5 references `transcription.enriched` — out of scope

**Где:** `tasks.md` (1.5 — "verify ksqlDB connects to Kafka cluster and can list available topics (calls.completed, calls.fraud-alerts, transcription.enriched)")

**Что:** Task 1.5 включает `transcription.enriched` в список тем для верификации.

**Почему неправильно:** ksqldb-analytics-layer change обрабатывает только `calls.completed` и `calls.fraud-alerts`. `transcription.enriched` относится к transcription-analyzer change. Включение out-of-scope topic создаёт confusion.

**Рекомендация:** Удалить `transcription.enriched` из tasks 1.5.

---

## M2 [MEDIUM] Output topic partitioning in Migration Plan (2 vs 6)

**Где:** `design.md` (Migration Plan Phase 2 — "PARTITIONS=2") vs `infrastructure` spec (all topics = 6 partitions)

**Что:** Migration Plan SQL создает output topics с PARTITIONS=2. Infrastructure spec определяет 6 partitions для всех stream topics.

**Почему неправильно:** Inconsistent partitioning — 2 partition vs 6 partition. ksqlDB CREATE STREAM WITH (PARTITIONS=2) создаст topic с 2 partition, что ниже standard. Это ограничит parallelism consumers.

**Рекомендация:** Изменить PARTITIONS=2 → PARTITIONS=6 в Migration Plan SQL, или определить output topics в infrastructure change с partitions=6 и убрать PARTITIONS из CREATE STREAM.

---

## M3 [MEDIUM] Documentation task 7.3 targets wrong file

**Где:** `tasks.md` (7.3 — "Document ksqlDB REST API usage... in ARCHITECTURE/08-monitoring.md")

**Что:** REST API documentation размещается в monitoring architecture doc.

**Почему неправильно:** REST API — это data flow / integration documentation, не monitoring. Monitoring doc должен содержать метрики, алерты, dashboards.

**Рекомендация:** Переместить task 7.3 в ARCHITECTURE/04-data-flow.md (вместе с 7.1 и 7.4).

---

## L1 [LOW] ksqldb-cli service listed in proposal but not in design

**Где:** `proposal.md` ("New docker-compose service: ksqldb-cli (for bootstrap, ephemeral)") vs `design.md` (Decision 1 — embedded mode, "no separate CLI container needed in production")

**Что:** Proposal упоминает отдельный ksqldb-cli service. Design решает использовать embedded mode.

**Почему неправильно:** Minor inconsistency — proposal устарел относительно design. Не блокирует реализацию (design overrides).

**Рекомендация:** Удалить упоминание ksqldb-cli из proposal.md Impact секции.

---

## L2 [LOW] Non-Goals: "manual testing only" for CI/CD

**Где:** `design.md` (Non-Goals — "Add ksqlDB to CI/CD pipeline (manual testing only)")

**Что:** ksqlDB excluded from CI/CD.

**Почему неправильно:** Non-goal — это осознанный выбор, но стоит задокументировать rationale (scope constraint vs deliberate decision). Если ksqlDB деплоится в production, отсутствие CI/CD для stream validation — operational risk.

**Рекомендация:** Добавить rationale в Non-Goals: "CI/CD for ksqlDB deferred to future change — current scope is manual validation only."

---

## L3 [LOW] Query timeout mechanism not specified

**Где:** `spec.md` (Requirement: Ad-hoc Query via REST API — "Timeout long-running queries after 30 seconds")

**Что:** Spec требует 30s timeout, но не указывает как он конфигурируется.

**Почему неправильно:** ksqlDB uses `request.timeout.ms` и `max.block.ms` для query timeout. Без явной конфигурации implementer не знает где и как установить 30s.

**Рекомендация:** Добавить в tasks 1.2 или 1.3 задачу на конфигурацию query timeout: `ksql.query.timeout.ms=30000` (или через REST API header).

---

## L4 [LOW] No scenario for ksqlDB topic auto-creation behavior

**Где:** `spec.md` (Requirement: ksqlDB Server Deployment)

**Что:** Spec не определяет behavior когда output topics уже существуют (re-deploy, rollback+redeploy).

**Почему неправильно:** ksqlDB CREATE STREAM fail если topic уже существует. При rollback+redeploy streams fail.

**Рекомендация:** Добавить scenario "Stream creation idempotency" — WHEN output topic already exists, THEN ksqlDB drops and recreates, OR returns success if schema matches.

---

## L5 [LOW] Missing scenario for late-arriving events in calls_completed_agg

**Где:** `spec.md` (Requirement: calls_completed_agg Stream)

**Что:** Spec упоминает "1-minute grace period" но не определяет что происходит с events, пришедшими после grace period.

**Почему неправильно:** Implementer не знает — events дропаются или включаются в next window.

**Рекомендация:** Добавить scenario "Late event after grace period" — WHEN event arrives >1 minute after window close, THEN event is dropped (not included in aggregation).

---

## L6 [LOW] `avg_nps_score` precision not specified

**Где:** `spec.md` (Requirement: calls_completed_agg — "avg_nps_score: AVG of npsScore field")

**Что:** AVG вычисляется, но precision не определена (DOUBLE в output schema).

**Почему неправильно:** ksqlDB AVG возвращает DOUBLE по умолчанию. Если reporting-nps или downstream consumers ожидают конкретную точность (например ROUND(avg, 2)), это неясно.

**Рекомендация:** Добавить note в output schema: "avg_nps_score: DOUBLE, precision as computed by ksqlDB (default DOUBLE)".

---

## Cross-Module Consistency Map

```
ksqldb-analytics-layer ──consumes──▶ calls.completed       ← call-processor (agentId ✓)
       │                                  │
       │                                  ├─ key=callId (Avro)
       │                                  └─ fields: agentId, npsScore, completedAt ✓
       │
       ├─consumes──▶ calls.fraud-alerts    ← fraud-detector (phone ✓)
       │                                  │
       │                                  ├─ key=phone (Avro)
       │                                  └─ fields: phone, pattern, severity, count ✓
       │
       ├─produces──▶ calls.completed.agg   ← infrastructure ✓(R-5, partitions=6)
       │                                  │
       │                                  ├─ key=agentId
       │                                  └─ fields: agentId, call_count, avg_nps_score, last_call_at ✓
       │
       └─produces──▶ calls.fraud-alerts.agg ← infrastructure ✓(R-5, partitions=6)
                                        │
                                        ├─ key=phone:pattern:severity
                                        └─ fields: phone, pattern, severity, alert_count ✓

       ┌─connects──▶ Kafka (SASL/PLAIN)   ← R-3: Decision 7 + task 1.2 ✓
       │
       └─connects──▶ Schema Registry:8085 ← R-1: port aligned ✓
```

---

## Verification Checklist

| Check | Status | Notes |
|-------|--------|-------|
| Field names match source schemas | ✅ PASS | agentId (call-processor ✓), phone/pattern/severity (fraud-detector ✓) |
| Schema Registry port consistency | ✅ PASS | R-1: container-architecture = 8085 |
| ksqlDB container image | ✅ PASS | R-2: cp-ksqldb-server:latest |
| SASL auth for ksqlDB→Kafka | ✅ PASS | R-3: Decision 7 + task 1.2 |
| Output topics in infrastructure spec | ✅ PASS | R-5: 2 new topics added |
| unique_phones metric meaningfulness | ✅ PASS | R-4 + R-15: removed, alert_count only |
| Proposal vs spec consistency | ✅ PASS | R-6: "per phone" aligned |
| Task scope (no transcription.enriched) | ✅ PASS | R-7: removed from task 1.5 |
| Partitioning consistency (6 partitions) | ✅ PASS | R-8: PARTITIONS=6 |
| Documentation targets correct files | ✅ PASS | R-9: data-flow.md |
| Idempotent stream creation | ✅ PASS | R-12: scenario added |
| Late event handling | ✅ PASS | R-13: scenario added |
| Query timeout configuration | ✅ PASS | R-11: task 1.6 |
| CI/CD rationale | ✅ PASS | R-16: rationale added to Non-Goals |

---

## Recommended Action

**Все issues исправлены. Change APPROVED для реализации.**

1. ✅ B1, B2, B3 — port, image, SASL auth (R-1, R-2, R-3)
2. ✅ H1 — unique_phones removed (R-4, R-15)
3. ✅ H2 — output topics added to infrastructure spec (R-5)
4. ✅ H3 — proposal aligned with spec (R-6)
5. ✅ M1-M3 — task cleanup (R-7, R-8, R-9)
6. ✅ L1, L3-L6 — minor improvements (R-10, R-11, R-12, R-13, R-14)
7. ✅ L2 — CI/CD rationale added (R-16)

**Итог:** 16 замечаний исправлены, 0 блокирующих issues. Change готов к реализации.

**После исправлений:** re-run `openspec validate` для ksqldb-analytics-layer change.
