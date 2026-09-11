# Failure Log — Журнал ошибок и замечаний

Этот файл используется для отслеживания ошибок, багов и замечаний, обнаруженных в процессе реализации changes.

## Как заполнять

Каждая запись описывает одну ошибку/замечание:

| Колонка | Описание |
|---------|----------|
| **№** | Порядковый номер замечания (начиная с 1) |
| **Где** | Файл, компонент или change, где обнаружена ошибка |
| **Что было** | Описание ошибки — что произошло не так |
| **Почему неправильно** | Причина ошибки — почему это ошибка |
| **Как исправили** | Решение — что было сделано для исправления |

## Шаблон таблицы

| № | Где | Что было | Почему неправильно | Как исправили |
|---|-----|----------|-------------------|---------------|
| R-1 | `ARCHITECTURE/03-container-architecture.md` (Schema Registry port) | Schema Registry указан на порту 8081, но spec/design ksqldb-analytics-layer и infrastructure spec определяют порт 8085 | Port mismatch: ksqlDB не подключится к SR с неверным портом, Avro events не десериализуются | Исправлен порт 8081 → 8085 в таблице контейнеров container-architecture.md |
| R-2 | `ARCHITECTURE/03-container-architecture.md` (ksqlDB container) | ksqlDB использует образ `cp-ksqldb-cli:latest` — CLI-образ без REST API | CLI-образ не предоставляет порт 8088, streams не запустятся. Design/tasks определяют server mode | Исправлен образ на `cp-ksqldb-server:latest`, роль → "SQL-обработка потоков + REST API" |
| R-3 | `ARCHITECTURE/03-container-architecture.md` (связи), `design.md`, `tasks.md` (1.2, 1.6) | ksqlDB → Kafka connection без SASL auth. Kafka cluster требует SASL/PLAIN (infrastructure spec) | ksqlDB не подключится к Kafka — streams не запустятся, вся функциональность не работает | Добавлен Decision 7 (SASL Authentication) в design.md с конфигурацией. Обновлён task 1.2 (SASL properties). Обновлён task 1.6 (query timeout). Обновлена диаграмма связей: ksqlDB ──[Kafka SASL]──▶ Kafka Cluster |
| R-4 | `spec.md`, `design.md` (Migration Plan SQL), `tasks.md` (3.3, 5.4) | `unique_phones = COUNT(DISTINCT phone)` в fraud_alerts_filtered — всегда = 1 при GROUP BY phone | Metric meaningless: каждая группа уже уникальна по phone. Вводит в заблуждение downstream consumers | Удалён `unique_phones` из spec scenario, output topic schema, Migration Plan SQL, tasks 3.3 и 5.4. Оставлен только `alert_count` |
| R-5 | `openspec/changes/infrastructure/specs/infrastructure/spec.md`, `design.md` (Decision 6) | Output topics (`calls.completed.agg`, `calls.fraud-alerts.agg`) не определены в infrastructure spec | ksqlDB auto-creates topics с default 1 partition, replication factor не настроен — risk data loss при restart. Не соответствует infrastructure standard (6 partitions, replication=3) | Добавлен новый scenario "ksqlDB output topics are created" и scenario "All topics have consistent partitioning" в infrastructure spec. Обновлён Decision 6: output topics pre-created by infrastructure change |
| R-6 | `proposal.md` (fraud_alerts_filtered) | Proposal говорит "Count of alerts per agentId, pattern, severity" — agentId не существует в fraud-alerts | Proposal не ��онсистентен со spec (phone-based aggregation). Запутает reviewer и future maintainers | Заменён "per agentId" на "per phone" в секции What Changes |
| R-7 | `tasks.md` (1.5) | Task 1.5 включает `transcription.enriched` в список тем для верификации ksqlDB | Out-of-scope topic: относится к transcription-analyzer change, не ksqldb-analytics-layer | Удалён `transcription.enriched` из tasks 1.5 |
| R-8 | `design.md` (Migration Plan SQL), `tasks.md` (2.1, 3.1) | Migration Plan создаёт output topics с PARTITIONS=2, infrastructure spec определяет 6 partitions | Inconsistent partitioning: 2 partition vs 6 partition. ksqlDB auto-created topics с 2 partitions ограничат parallelism consumers | Изменён PARTITIONS=2 → PARTITIONS=6 в SQL для обоих CREATE STREAM statements |
| R-9 | `tasks.md` (7.3) | Task 7.3 документирует REST API в `ARCHITECTURE/08-monitoring.md` | REST API — data flow / integration documentation, не monitoring. Monitoring doc должен содержать метрики и алерты | Перемещён target в `ARCHITECTURE/04-data-flow.md` |
| R-10 | `proposal.md` (Infrastructure) | Proposal упоминает отдельный service `ksqldb-cli` (for bootstrap, ephemeral) | Design.md Decision 1 определяет embedded mode: "no separate CLI container needed". Proposal устарел | Удалено упоминание ksqldb-cli из Impact секции |
| R-11 | `tasks.md` (1.2, 1.6) | Query timeout (30s) определён в spec, но не маппится на ksqlDB конфигурацию | Implementer не знает где и как установить `ksql.query.timeout.ms=30000` | Добавлена task 1.6: "Configure query timeout (ksql.query.timeout.ms=30000)" |
| R-12 | `spec.md` (Scenario: Stream survives restart) | Отсутствует scenario для idempotent stream creation (rollback + redeploy) | ksqlDB CREATE STREAM fail если topic уже существует. При rollback+redeploy streams fail | Добавлен scenario "Stream creation idempotency" с описанием DROP+recreate behavior |
| R-13 | `spec.md` (Scenario: Stream survives restart) | Отсутствует scenario для late-arriving events после grace period | Implementer не знает — events дропаются или включаются в next window | Добавлен scenario "Late event after grace period" — event >1 minute after window close is dropped |
| R-14 | `spec.md` (Output topic schema: avg_nps_score) | `avg_nps_score: DOUBLE` без указания precision | ksqlDB AVG возвращает DOUBLE по умолчанию; downstream consumers (reporting-nps) не знают точность | Добавлена note: "precision as computed by ksqlDB, default DOUBLE" в schema description |
| R-15 | `spec.md` (Scenario: Stream aggregates HIGH severity alerts) | Scenario text содержит `unique_phones=<computed>` — metric удалён в R-4, но scenario не обновлён | Inconsistency: scenario ссылается на удалённый field. Вводит implementer в заблуждение | Удалён `unique_phones=<computed>` из scenario text в spec.md |
| R-16 | `design.md` (Non-Goals) | Non-Goals: "Add ksqlDB to CI/CD pipeline (manual testing only)" без rationale | Operational risk: отсутствие CI/CD для production-компонента не объяснено | Добавлен rationale: "CI/CD for ksqlDB deferred to future change — current scope is manual validation only" |
| R-17 | `infrastructure/specs/infrastructure/spec.md` (All topics partitioning) vs `infrastructure/tasks.md` (6.x) | Spec говорит "10 topics have 6 partitions", tasks создают только 8 топиков (2 ksqlDB output topics отсутствуют) | ksqlDB auto-creates topics с default 1 partition, replication=1 — risk data loss | Зафиксировано в cross-review, требуется добавить tasks 7.x в infrastructure/tasks.md |
| R-18 | `kafka-connect-jdbc-sink/tasks.md` (1.4) vs `infrastructure/tasks.md` (3.x) | kafka-connect-jdbc-sink ожидает kafka-connect-user от infrastructure change, но infrastructure не создаёт user | Dependency gap между changes: kafka-connect требует user, infrastructure не знает о kafka-connect | Зафиксировано в cross-review, требуется добавить task 3.5 в infrastructure/tasks.md |
| R-19 | `kafka-connect-jdbc-sink/spec.md` + `reporting-nps/spec.md` | Оба компонента пишут в call_transcriptions из transcription.enriched — dual-write duplication | Нет согласованности: какой path primary, conflict resolution при race condition | Зафиксировано в cross-review, требуется architectural decision (Option A/B/C) |

## История изменений

| Дата | Изменения                                              | Автор |
|------|--------------------------------------------------------|-------|
| 2026-09-09 | Создан шаблон failure log                              |       | |
| 2026-09-10 | Создан Failure log - 1, архивация предыдущих сообщений | Owner |
| 2026-09-10 | Ревью ksqldb-analytics-layer: 14 замечаний (B1 Schema Registry port, B2 ksqlDB image, B3 SASL auth; H1 unique_phones meaningless, H2 output topics missing in infra, H3 proposal agentId inconsistency; M1 transcription.enriched out of scope, M2 partitioning 2→6, M3 docs target; L1 ksqldb-cli removal, L3 query timeout, L4 idempotency scenario, L5 late event scenario, L6 avg_nps_score precision). Исправлены все 14 issues в spec/design/tasks/architecture/infrastructure. Оценка: APPROVED после исправлений. | AI agent |
| 2026-09-10 | Post-review cleanup: R-15 (unique_phones scenario text), R-16 (CI/CD rationale). Итог: 16 замечаний, все исправлены. Статус: APPROVED. | AI agent |
| 2026-09-11 | Cross-review round 2 (7 changes): R-17 (10 topics spec vs 8 tasks), R-18 (kafka-connect-user missing), R-19 (dual-write duplication). Итог: 3 critical issues открыты. Статус: NEEDS RESOLUTION. | AI agent |

## Notes

- Заполняйте таблицу по мере обнаружения ошибок
- Каждый change может иметь свои ошибки — указывайте конкретный change в колонке "Где"
- Если ошибка обнаружена на этапе планирования (до реализации) — всё равно фиксируйте
- После завершения change подведите итог: сколько ошибок, основные категории
