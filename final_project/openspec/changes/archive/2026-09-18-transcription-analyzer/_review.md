# Review: transcription-analyzer change

**Дата:** 2026-09-10
**Change:** `transcription-analyzer`
**Рецензируемые артефакты:** proposal.md, design.md, spec.md, tasks.md
**Контекст для сравнения:** infrastructure, call-processor, fraud-detector, reporting-nps, ARCHITECTURE/04-data-flow.md, ARCHITECTURE/05-component-diagrams.md
**Финальное ревью:** 2026-09-10 — все 14 замечаний исправлены, +1 обнаружен при финальной проверке ARCHITECTURE

---

## H-1 (Critical): KTable Join — Key Mismatch Не Описан

**Статус:** ✅ FIXED (исправлено)

**Было:** transcription.summary key=callId, customers.profile key=phone — KTable join невозможен без repartition

**Стало:** Заменён KTable join на broadcast enrichment. customers.profile загружается в in-memory map при старте, каждый event обогащается broadcast lookup.

**Файлы изменены:**
- spec.md: requirement "Customer Profile Broadcast Enrichment" (замена "KTable Join with Customer Profile")
- design.md: Decision 4 "Broadcast Enrichment vs KTable Join"
- tasks.md: section 5 → section 6 "Customer Profile Broadcast Enrichment"

**Архитектура:** ARCHITECTURE/04-data-flow.md section 4.3.6 обновлён: "broadcast customers.profile (in-memory map, key=phone)"

---

## H-2 (Critical): Metadata Lifecycle — PENDING Status Дублируется

**Статус:** ✅ FIXED (исправлено)

**Было:** transcription-analyzer устанавливал PENDING статус, дублируя call-processor

**Стало:** PENDING удалён из transcription-analyzer. Lifecycle начинается с TRANSCRIBING. call-processor spec подтверждает: metadata events используют CallEvent+status schema.

**Файлы изменены:**
- spec.md: удалён scenario "Initial status PENDING", metadata lifecycle начинается с TRANSCRIBING
- design.md: Goals обновлены, lifecycle: TRANSCRIBING → SUMMARIZING → COMPLETED
- tasks.md: удалён task 7.2 (PENDING), lifecycle: TRANSCRIBING → SUMMARIZING → COMPLETED
- call-processor spec.md: добавлена нотация что downstream сервисы используют CallEvent+status schema

---

## H-3 (High): Dual-Write Consistency — Kafka First, Then DB

**Статус:** ✅ FIXED (исправлено)

**Было:** "if PostgreSQL write fails, the Kafka produce is retried" — логически неверно, Kafka produce уже отправлен

**Стало:** Scenario "PostgreSQL failure with Kafka success" — Kafka event уже у consumers, PostgreSQL retry/DLQ для догоняющей синхронизации

**Файлы изменены:**
- spec.md: requirement "Dual-Write Strategy" обновлён, новый scenario

---

## M-1 (High): Self-Referential Topology Не Описан

**Статус:** ✅ FIXED (исправлено)

**Было:** Design не описывал self-referential topology (сервис produces и consumes собственные topics)

**Стало:** Decision 6 "Self-Referential Topology" с consumer group isolation strategy

**Файлы изменены:**
- design.md: Decision 6 с объяснением паттерна и mitigation

---

## M-2 (High): Summary Generation Input Source Не Определён

**Статус:** ✅ FIXED (исправлено)

**Было:** Spec не указывал input source для summary generation

**Стало:** Новый requirement "Summary Generator Reads from transcription.raw" с scenario и default values для no keyword match

**Файлы изменены:**
- spec.md: новый requirement + scenario "Default values when no keywords match"

---

## M-3 (High): Priority Calculation Matrix Не Определён

**Статус:** ✅ FIXED (исправлено)

**Было:** Определена только 1 комбинация (premium+high=critical), "etc." в tasks

**Стало:** Полная priority matrix (3×3) в spec и tasks

**Файлы изменены:**
- spec.md: полный priority matrix table
- tasks.md: section 6.4 с полной матрицей

---

## M-4 (Medium): Metadata Event Schema Inconsistency с call-processor

**Статус:** ✅ FIXED (исправлено)

**Было:** transcription-analyzer tasks определяли separate MetadataEvent Avro schema, call-processor spec — CallEvent+status

**Стало:** transcription-analyzer tasks: удалён MetadataEvent schema task. call-processor spec обновлён — все downstream сервисы используют CallEvent+status

**Файлы изменены:**
- tasks.md: удалён task 2.4 (MetadataEvent Avro schema)
- call-processor spec.md: добавлена нотация что downstream сервисы используют CallEvent+status

---

## M-5 (Medium): EnrichedTranscription Schema — Priority Поле Не В Spec

**Статус:** ✅ FIXED (исправлено)

**Было:** Priority в tasks.md, но не в spec scenario

**Стало:** Priority включён в spec через scenario "Priority calculated from segment and risk_level" и EnrichedTranscription Avro schema (task 2.3)

**Файлы изменены:**
- spec.md: priority в EnrichedTranscription scenario и priority matrix

---

## M-6 (Medium): Keyword Extraction — Input Text Не Определён

**Статус:** ✅ FIXED (исправлено)

**Было:** Нет default behavior, нет confidence formula, нет case-sensitivity

**Стало:** Три новых scenario: default values, confidence score formula, case-insensitive matching

**Файлы изменены:**
- spec.md: scenario "Default values when no keywords match", scenario "Confidence score based on keyword frequency", "Keyword matching is case-insensitive" в requirement header

---

## L-1 (Low): Design "Non-Goals" Дублирует Context

**Статус:** ✅ FIXED (исправлено)

**Было:** 6 пунктов Non-Goals, 3 дублируют Context/Goals

**Стало:** Сокращён до 3 ключевых пунктов (LLM, audio, real-time streaming)

**Файлы изменены:**
- design.md: Non-Goals секция

---

## L-2 (Low): Architecture Docs Still Say "LLM Simulation"

**Статус:** ✅ FIXED (исправлено)

**Было:** ARCHITECTURE/04-data-flow.md section 4.3.5: "LLM simulation"

**Стало:** "Synthetic summary generation (keyword-based, no LLM)"

**Файлы изменены:**
- ARCHITECTURE/04-data-flow.md: section 4.3.5

---

## L-3 (Low): Tasks Section 8 Duplicates Section 4

**Статус:** ✅ FIXED (исправлено)

**Было:** Section 8 (Keyword Extraction Configuration) дублировал section 4

**Стало:** Section 8 merged into section 4: configurable dictionary, case-insensitive search, frequency scoring

**Файлы изменены:**
- tasks.md: section 4 обновлён, section 8 удалён

---

## L-4 (Low): Missing Task for Schema Registry Integration

**Статус:** ✅ FIXED (исправлено)

**Было:** Нет задач для SR client config (blocking gap для Avro serialization)

**Стало:** Добавлена секция 3 "Schema Registry Integration" (6 задач)

**Файлы изменены:**
- tasks.md: новая секция 3

---

## L-5 (Low): "Open Questions: Нет" В Design

**Статус:** ✅ FIXED (исправлено)

**Было:** "Open Questions: Нет" — false confidence

**Стало:** 4 open questions: O-1 customers.profile data source, O-2 broadcast refresh strategy, O-3 keyword dictionary size, O-4 consumer group naming

**Файлы изменены:**
- design.md: секция Open Questions

---

## L-6 (Low): Proposal Impact Section Missing reporting-nps Dependency Detail

**Статус:** ✅ FIXED (исправлено)

**Было:** "reporting-nps зависит от transcription.enriched и PostgreSQL данных"

**Стало:** "reporting-nps зависит от transcription.enriched (consumes для call_transcriptions upsert) и PostgreSQL данных (читает call_transcriptions для v_full_call_info view)"

**Файлы изменены:**
- proposal.md: Impact секция

---

## N-1 (New, Low): ARCHITECTURE/04-data-flow.md section 4.3.6 — KTable Still Referenced

**Статус:** ✅ FIXED (исправлено при финальном ревью)

**Где:** ARCHITECTURE/04-data-flow.md section 4.3.6

**Проблема:** После исправления L-2 (section 4.3.5), section 4.3.6 всё ещё говорил "join transcription.summary × customers.profile (KTable) по key: phone" — противоречие с broadcast decision

**Исправление:** Обновлено на "broadcast customers.profile (in-memory map, key=phone), lookup phone in customers.profile map"

---

## Итоговое состояние

| Severity | Было | Исправлено | Осталось |
|----------|------|------------|----------|
| Critical | 2 | 2 | 0 |
| High | 3 | 3 | 0 |
| Medium | 4 | 4 | 0 |
| Low | 6 | 7* | 0 |
| **Total** | **15** | **16** | **0** |

*\*Low: L-2 исправлен дважды (section 4.3.5 и section 4.3.6), посчитано как 2 исправления*

### Все артефакты согласованы:

```
spec.md          ✅ broadcast enrichment, priority matrix, default values, confidence formula
design.md        ✅ Decision 4 (broadcast), Decision 6 (self-referential), 4 open questions
tasks.md         ✅ SR integration секция, merged keyword section, broadcast enrichment section
proposal.md      ✅ reporting-nps dependency detail
call-processor   ✅ metadata schema alignment note
ARCHITECTURE     ✅ LLM→keyword-based, KTable→broadcast
failure-log.md   ✅ записи 2-15 + история изменений
```

### Архитектура потока данных (обновлённая):

```
calls.completed (call-processor)
    │
    ▼
transcription-analyzer
    │
    ├──[Producer]──▶ transcription.raw (key=callId)
    │       │
    │       ▼
    │   SummaryConsumer (group: summary-processor)
    │       │
    │       ├── keyword matching (case-insensitive)
    │       ├── default values (no match)
    │       ├── confidence formula
    │       │
    │       ▼
    │   transcription.summary (key=callId)
    │       │
    │       ▼
    │   Broadcast Enrichment (in-memory map)
    │       ├── customers.profile loaded on startup (key=phone)
    │       ├── lookup phone → segment + riskLevel
    │       └── priority matrix (3×3)
    │       │
    │       ▼
    │   transcription.enriched (key=callId) ──▶ Kafka
    │       │                                       │
    │       │                              reporting-nps (Consumer)
    │       │                                       │
    │       ▼                                       ▼
    │   DualWriter                              PostgreSQL
    │       ├── Kafka produce (transcription.enriched)
    │       └── JDBC write (call_transcriptions)
    │           ├── retry 3x, exponential backoff
    │           └── DLQ on failure
    │
    └──[MetadataManager]──▶ calls.metadata (key=callId, compacted)
            └── TRANSCRIBING → SUMMARIZING → COMPLETED
                (PENDING set by call-processor)
```

### Open Questions → Решены:

| # | Вопрос | Решение | Артефакт |
|---|--------|---------|----------|
| O-1 | customers.profile data source | **Bootstrap CSV script** (kafka-console-producer) | design.md Decision 7, tasks 12.x |
| O-2 | Broadcast refresh strategy | **Event-driven** (Kafka consumer customers.profile) | design.md Decision 8, tasks 6.1-6.2 |
| O-3 | Keyword dictionary size | **application.yml** (@Value list) | design.md Decision 9, tasks 1.4, 5.4 |
| O-4 | Consumer group naming | **summary-processor**, **enrichment-processor** | design.md Decision 10, tasks 1.4 |

### Verdict: ✅ APPROVED FOR IMPLEMENTATION

Все blocking и critical issues исправлены. Все open questions решены. Артефакты согласованы между собой и с кросс-чейндж зависимостями.
