# Review: call-processor

**Дата:** 2026-09-10  
**Статус артефактов:** proposal ✅ | specs ✅ | design ✅ | tasks ✅  
**Тип ревью:** логичность и согласованность (без изменений кода)

---

## Методология

Ревью проведено по 4 уровням:
1. **Внутренняя согласованность** — proposal → specs → design → tasks
2. **Кросс-чейндж согласованность** — call-processor ↔ fraud-detector, transcription-analyzer, reporting-nps
3. **Согласованность с ARCHITECTURE** — 14 документов в папке ARCHITECTURE/
4. **Полнота и корректность формулировок**

---

## 🔴 КРИТИЧЕСКИЕ (Blocking)

### C-1: Конфликт портов — call-processor vs Schema Registry

**Статус:** ✅ ИСПРАВЛЕНО

| Где | Что было | Почему неправильно |
| proposal.md, ARCHITECTURE/01-overview.md | call-processor использует порт 8081; Schema Registry также использует порт 8081 | Два сервиса на одном порту в docker-compose не могут стартовать одновременно. Это blocking issue. |

**Рекомендация:** Schema Registry должен использовать другой порт (например, 8085). call-processor на 8081 — корректно.
**Действие** Проверить согласованность портов - исправить в случае конфликта.
---

### C-2: Topic `calls.dlq` отсутствует в ARCHITECTURE

**Статус:** ❌ ОШИБОЧНОЕ ОБНАРУЖЕНИЕ

| Где | Что было | Почему неправильно |
|-----|----------|---------------------|
| ARCHITECTURE/01-overview.md | Перечислены 8 топиков, но `calls.dlq` отсутствует | call-processor spec, design, proposal и tasks все ссылаются на `calls.dlq` как на ключевой компонент. Архитектурный документ неполный. |

**Рекомендация:** Добавить `calls.dlq` в список 8 топиков в ARCHITECTURE/01-overview.md. Итого 9 топиков.
**Действие** Добавить `calls.dlq`в ARCHITECTURE/01-overview.md
---

## 🟡 СРЕДНИЕ (Should Fix)

### M-1: `calls.metadata` — compaction rationale не объяснён

| Где | Что было | Почему неправильно |
|-----|----------|---------------------|
| ARCHITECTURE/01-overview.md, design.md | `calls.metadata` обозначен как Compacted Table | Compaction имеет смысл для topic, где ключ перезаписывается. Но ключ — `callId` (уникальный для каждого звонка). Compaction полезна только если metadata обновляется (PENDING → TRANSCRIBING → COMPLETED). Ни spec, ни design не объясняют, почему compaction выбрана и кто обновляет metadata. transcription-analyzer design упоминает обновление, но call-processor artifacts — нет. |

**Рекомендация:** Добавить в spec scenario: "Metadata status updated by downstream services" и объяснить rationale compaction в design.
**Действие** Добавить
---

### M-2: `npsScore` — Russia-specific phone format vs npsScore range

| Где | Что было | Почему неправильно |
|-----|----------|---------------------|
| spec.md, tasks.md | Phone: E.164 format `+7XXXXXXXXXX` (только Россия); npsScore: 0-10 | Phone validation hardcoded под +7 (Россия), что согласуется с "банковский колл-центр". Но npsScore range 0-10 — это не универсальный NPS (обычно 0-9 или -5..+5). Нужно подтвердить, что 0-10 — осознанный выбор. |

**Рекомендация:** Добавить комментарий в spec, что 0-10 — осознанный выбор для MVP.
**Действие** Добавить
---

### M-3: `Open Questions: Нет` — не соответствует действительности

| Где | Что было | Почему неправильно |
|-----|----------|---------------------|
| design.md (все 4 change-а) | "Open Questions: Нет — все технические решения определены" | Порт Schema Registry конфликтует с call-processor (C-1). calls.dlq отсутствует в ARCHITECTURE (C-2). Compaction rationale для calls.metadata не объяснён (M-1). Это реальные открытые вопросы. |

**Рекомендация:** Заменить "Нет" на актуальные open questions.
**Действие** Добавить
---

### M-4: Missing task для producer throughput properties

| Где | Что было | Почему неправильно |
|-----|----------|---------------------|
| tasks.md | Нет задач для настройки batch.size, linger.ms, compression.type | design.md секция Risks/Trade-offs упоминает эти свойства для high throughput (>1000 events/sec), но tasks не включают их настройку. |

**Рекомендация:** Добавить task 4.6: "Configure producer throughput properties (batch.size=16384, linger.ms=5, compression.type=lz4)".
**Действие** Добавить
---

## 🟢 НИЗКИЕ (Nice to Have)

### L-1: Correlation ID не выделен в design Decisions

| Где | Что было | Почему неправильно |
|-----|----------|---------------------|
| design.md | Correlation ID упомянут в Goals, но нет отдельного Decision | Correlation ID — важная cross-cutting concern для трейсинга. В других change-ах (transcription-analyzer) это учтено через metadata status transitions. |

**Рекомендация:** Добавить Decision: "Correlation ID generation approach" с обоснованием UUID v4 vs v7.
**Действие** Добавить

**Статус:** ✅ ИСПРАВЛЕНО: Добавлен Decision 7 с обоснованием UUID v4 vs v7.
---

### L-2: Avro schema — один или два?

| Где | Что было | Почему неправильно |
|-----|----------|---------------------|
| design.md, tasks.md | "Avro schemas for CallEvent (calls.completed, calls.metadata)" — неясно, один schema для обоих topics или разные | Скорее всего один schema (CallEvent), но spec не уточняет. Если schema одна — нужно сказать явно. |

**Рекомендация:** Уточнить в spec: "Same Avro schema for calls.completed and calls.metadata topics".
**Действие** Уточнить
---

### L-3: Task 1.5 и 13.1 — дублирование Docker

| Где | Что было | Почему неправильно |
|-----|----------|---------------------|
| tasks.md | Task 1.5: "Create Dockerfile... verify Docker build succeeds"; Task 13.1: "Create Dockerfile with multi-stage build... verify image size < 300MB" | Dockerfile создаётся дважды. Task 1.5 должен быть "scaffold Dockerfile", а 13.1 — "finalize and optimize". |

**Рекомендация:** Переформулировать 1.5 как "Create initial Dockerfile (multi-stage build)" и 13.1 как "Optimize Docker image (verify size < 300MB, multi-stage build finalized)".
**Действие** Переформулировать

**Статус:** ✅ ИСПРАВЛЕНО: 1.5 → "Create initial Dockerfile", 13.1 → "Optimize Docker image".
---

### L-4: Task 2.4 — Avro schema verification

| Где | Что было | Почему неправильно |
|-----|----------|---------------------|
| tasks.md | "Create Avro schemas... and verify Schema Registry compatibility" | Schema Registry ещё не запущен на этапе создания schema-файлов. Verification — это задача 5.x. |

**Рекомендация:** Убрать "verify Schema Registry compatibility" из 2.4, оставить только "Create Avro schemas".
**Действие** выполнить

**Статус:** ✅ ИСПРАВЛЕНО: Убрано "verify Schema Registry compatibility" из task 2.4.
---

## ✅ ПОЛОЖИТЕЛЬНЫЕ НАХОДКИ (Consistent Areas)

| Область | Статус | Комментарий |
|---------|--------|-------------|
| REST API spec (202/400) | ✅ Consistent | proposal → spec → tasks полностью согласованы |
| Kafka producer idempotent mode | ✅ Consistent | spec (exactly-once) → design (enable.idempotence=true, acks=all) → tasks (4.1) |
| Retry + DLQ flow | ✅ Consistent | spec (3 retries, 1s/2s/4s) → design (5 decisions) → tasks (7.x, 8.x) |
| Topic configuration | ✅ Consistent | design (rf=3, 6 partitions) → spec (same) → tasks (6.3) |
| Testing strategy | ✅ Consistent | design (Embedded Kafka + Testcontainers) → tasks (11.x, 12.x) |
| Port assignments (services) | ✅ Consistent | call-processor:8081, fraud-detector:8082, transcription-analyzer:8083, reporting-nps:8084 |
| Cross-change data flow | ✅ Consistent | call-processor → fraud-detector (calls.completed), transcription-analyzer (calls.completed), reporting-nps (calls.metadata) |
| Java 21 + Spring Boot 3 + Gradle Kotlin DSL | ✅ Consistent | Все 4 service changes используют одинаковый стек |
| Health check pattern | ✅ Consistent | Все services имеют GET /api/health с Kafka connectivity check |

---

## ✅ ПРИМЕНЁННЫЕ ИСПРАВЛЕНИЯ

| № | Что исправлено | Файлы |
|---|----------------|-------|
| C-1 | Schema Registry порт 8081→8085 | `infrastructure/proposal.md`, `infrastructure/specs/infrastructure/spec.md` |
| F-1 | Infrastructure spec — Schema Registry port 8081→8085 | `infrastructure/specs/infrastructure/spec.md` |
| F-2 | Infrastructure spec — DLQ format Avro→JSON | `infrastructure/specs/infrastructure/spec.md` |
| M-1 | Compaction rationale для calls.metadata | `call-processor/spec.md` (новый scenario), `call-processor/design.md` (Decision 6) |
| M-2 | npsScore range 0-10 — комментарий о Russian call-center convention | `call-processor/spec.md` |
| M-3 | Open Questions — заменён "Нет" на актуальные открытые вопросы | `call-processor/design.md` |
| M-4 | Добавлен task 4.6 для producer throughput properties | `call-processor/tasks.md` |
| L-1 | Correlation ID — добавлен Decision 7 (UUID v4 vs v7) | `call-processor/design.md` |
| L-2 | Avro schema clarification — единая схема для обоих topics | `call-processor/spec.md` |
| L-3 | Task 1.5 и 13.1 — переформулировано для устранения дублирования | `call-processor/tasks.md` |
| L-4 | Task 2.4 — убрано "verify Schema Registry compatibility" | `call-processor/tasks.md` |

## 🔴 ФИНАЛИЗИРОВАННОЕ РЕВЬЮ (2026-09-10)

### F-1: Infrastructure spec — Schema Registry port inconsistency

| Где | Что было | Почему неправильно | Статус |
|-----|----------|-------------------|--------|
| `infrastructure/specs/infrastructure/spec.md` | Schema Registry starts on port 8081 | Proposal говорит 8085 (исправление C-1). Spec и proposal внутри одного change должны совпадать. | ✅ ИСПРАВЛЕНО: port 8081→8085 |

### F-2: Infrastructure spec — DLQ format inconsistency

| Где | Что было | Почему неправильно | Статус |
|-----|----------|-------------------|--------|
| `infrastructure/specs/infrastructure/spec.md` | `calls.dlq` created as stream topic with Avro format | call-processor spec, design и ARCHITECTURE/04-data-flow.md все говорят JSON. DLQ использует JSON для максимальной совместимости битых сообщений. | ✅ ИСПРАВЛЕНО: DLQ → JSON |

## 📊 Сводная таблица (после исправлений)

| Критичность | Кол-во | Описание |
|-------------|--------|----------|
| 🔴 Critical | 2 (исправлены) | F-1 Schema Registry port, F-2 DLQ format |
| 🟡 Medium | 4 (исправлены) | M-1 compaction rationale, M-2 npsScore, M-3 Open Questions, M-4 producer tasks |
| 🟢 Low | 4 (исправлены) | L-1 Correlation ID decision, L-2 Avro schema, L-3 Docker dedup, L-4 Avro verification |
| **Итого** | **10** | |

---

## 🎯 Итоговое заключение

**call-processor change в целом согласован и логичен.** Архитектурный подход (REST → validate → Kafka producer → DLQ/retry) последовательно реализован во всех артефактах. Основные потоки данных согласованы с зависимыми changes (fraud-detector, transcription-analyzer, reporting-nps).

**Все критические, medium и low issues исправлены.**
- F-1: Schema Registry port синхронизирован (8085) в infrastructure spec
- F-2: DLQ format синхронизирован (JSON) в infrastructure spec
- M-1: Compaction rationale добавлен в spec и design
- M-2: npsScore 0-10 с комментарием о Russian call-center convention
- M-3: Open Questions заменены на актуальные
- M-4: Добавлен task 4.6 для producer throughput
- L-1: Добавлен Decision 7 для Correlation ID (UUID v4 vs v7)
- L-2: Avro schema clarification в spec
- L-3: Tasks 1.5 и 13.1 переформулированы
- L-4: Убрано Schema Registry verification из task 2.4

**Change готов к apply.**
