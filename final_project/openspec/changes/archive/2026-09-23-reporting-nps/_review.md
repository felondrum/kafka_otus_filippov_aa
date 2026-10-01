# Review: reporting-nps Change

**Дата первого ревью:** 2026-09-10
**Дата финального ревью:** 2026-09-10
**Статус change:** planning complete (all artifacts done)
**Рецензиру��мые артефакты:** proposal.md, design.md, specs/reporting-nps/spec.md, tasks.md

---

## Summary

`reporting-nps` — финальный компонент платформы, CQRS read side с REST API. Change полностью спланирован.

**Первичное ревью (2026-09-10):** 14 замечаний (4H + 5M + 5L)
**Финальное ревью (2026-09-10):** 14/14 исправлено, все open questions закрыты. Change готов к implementation.

---

## Critical Issues (H) — All Resolved

### H-1: Enrichment fields (segment, risk_level, priority) lost in call_transcriptions

**Статус:** ✅ ИСПРАВЛЕНО

**Где:** `specs/reporting-nps/spec.md` (CQRS Read Side — Transcriptions), `tasks.md` (section 2.2, 10.2)

**Что было:** `transcription.enriched` events от transcription-analyzer содержат поля `segment`, `risk_level`, `priority` (из broadcast enrichment с customers.profile). Однако `call_transcriptions` таблица в reporting-nps хранит только: `call_id`, `transcription_text`, `sentiment`, `urgency`, `problem`, `solution`, `confidence`. Enrichment-поля не сохраняются в БД.

```
transcription-analyzer produces:
  transcription.enriched → { callId, transcription_text, sentiment, urgency,
                              problem, solution, confidence,
                              segment, risk_level, priority }  ← segment/risk/priority

reporting-nps stores in call_transcriptions:
  call_id, transcription_text, sentiment, urgency, problem, solution, confidence
  ← segment/risk/priority LOST
```

**Почему неправильно:** transcription-analyzer специально обогащает события данными профиля клиента (design.md Decision 2, spec "Customer Profile Broadcast Enrichment"). Потеря этих полей означает:
- `v_full_call_info` view не содержит segment/risk_level/priority
- Agent reports не могут включать fraud alerts по segment
- Инвестиции в broadcast enrichment transcription-analyzer частично бесполезны для reporting-nps

**Как исправлено:**
- spec.md: scenario "Enriched event creates transcription record" обновлён — добавлены segment, risk_level, priority в список полей call_transcriptions
- spec.md: scenario "Enriched event updates existing transcription" обновлён — явное упоминание обновления segment, risk_level, priority
- tasks.md 2.2: CallTranscription entity — полный список полей включая segment, risk_level, priority
- tasks.md 10.2: v_full_call_info view — включает segment, risk_level, priority
- tasks.md 15.5: full pipeline test — проверяет segment, risk_level, priority в transcription data

---

### H-2: No cross-topic correlation mechanism for fraud alerts → agent/call reports

**Статус:** ✅ ИСПРАВЛЕНО

**Где:** `specs/reporting-nps/spec.md` (CQRS Read Side — Fraud Correlation), `tasks.md` (section 5)

**Что было:** `calls.fraud-alerts` topic key = `phone`, но `calls.metadata` key = `callId`. Spec требует "fraud alerts associated with agent's calls" в agent reports. Fraud alert schema от fraud-detector содержит `callId` в body, но topic key — `phone`.

```
calls.metadata:    key=callId    → { callId, phone, agentId, ... }
calls.fraud-alerts: key=phone    → { callId, phone, pattern, count, severity }

Как correlate fraud alert → agent?
  fraud.alert.callId → call_metadata WHERE callId = ? → agentId
  ← нужен JOIN по callId, но callId не является key в fraud-alerts topic
```

**Почему неправильно:** Spec scenario "Agent report endpoint" требует "fraud alerts associated with agent's calls". Для этого нужно:
1. Найти все callId агента в call_metadata
2. Найти fraud alerts с matching callId в calls.fraud-alerts

Но calls.fraud-alerts key = phone, не callId. Consumer получает events grouped by phone, не по callId. Для корреляции нужно either:
- Хранить mapping phone→callId в local state (не масштабируется, т.к. один phone может иметь много calls)
- Использовать callId из body fraud alert и делать lookup в call_metadata по callId (требует дополнительного DB query per event)

Spec не описывает этот механизм корреляции.

**Как исправлено:**
- spec.md: добавлен новый requirement "CQRS Read Side — Fraud Correlation" с 2 scenarios:
  - "Fraud alert correlated with call metadata" — extract callId, lookup call_metadata, associate with agentId
  - "Fraud alert for unknown callId" — buffer + retry 3x with 5s intervals, store with agentId=null
- tasks.md 5.5: "Implement fraud alert correlation: extract callId from fraud alert body, lookup call_metadata to get agentId"
- tasks.md 5.6: "Handle fraud alert for unknown callId: buffer and retry lookup up to 3 times with 5-second intervals; store with agentId=null if still not found"
- tasks.md 5.7: unit test для fraud alert consumption, aggregation, и correlation

---

### H-3: FK integrity gap — transcription event arrives before metadata

**Статус:** ✅ ИСПРАВЛЕНО

**Где:** `specs/reporting-nps/spec.md` (CQRS Read Side — Transcriptions), `tasks.md` (section 4)

**Что было:** Spec требует "call_id must reference an existing record in call_metadata table". Но Kafka events могут приходить out-of-order (no ordering guarantee across topics). Если transcription.enriched event arrives до calls.metadata event, FK constraint fails.

```
Timeline:
  T1: call-processor produces calls.metadata (status=PENDING)
  T2: transcription-analyzer produces transcription.enriched

Если T2 < T1 (out-of-order):
  reporting-nps processes transcription.enriched first
  → FK constraint: call_id not in call_metadata → INSERT FAILS
```

**Почему неправильно:** Spec scenario "Foreign key integrity maintained" описывает желаемое поведение, но не определяет что делать при его нарушении. Без обработки orphan transcription events, данные теряются.

**Как исправлено:**
- spec.md: добавлен новый scenario "Orphan transcription event handling":
  - buffer orphan event, retry after configurable delay (default 5 seconds)
  - max 3 retries, on final failure → internal DLQ for manual review
- tasks.md 4.5: "Handle FK integrity: if call_id not found in call_metadata, buffer the orphan event and retry after 5 seconds (max 3 retries); on final failure, send to internal DLQ"
- tasks.md 13.2: unit test для EnrichedTranscriptionConsumer включает orphan event handling

---

### H-4: Missing Schema Registry consumer configuration

**Статус:** ✅ ИСПРАВЛЕНО

**Где:** `tasks.md` (section 16)

**Что было:** Все producing services (call-processor, fraud-detector, transcription-analyzer) имеют Schema Registry integration tasks. reporting-nps consumer читает Avro events из 3 topics, но ни одна task не определяет Schema Registry consumer configuration.

```
call-processor tasks:     ✓ Section 3 "Schema Registry Integration" (6 tasks)
fraud-detector tasks:     ✓ Section 3 "Schema Registry Integration" (6 tasks)
transcription-analyzer:   ✓ Section 3 "Schema Registry Integration" (6 tasks)
reporting-nps tasks:      ✗ NO Schema Registry consumer config
```

**Почему неправильно:** Без Schema Registry client config, Avro deserialization не будет работать. Consumer не знает где искать схемы, как валидировать совместимость, как сериализовать/десериализовать events. Это **blocking gap** — сервис не запустится.

**Как исправлено:**
- tasks.md: добавлена секция 16 "Schema Registry Consumer Integration" (6 задач):
  - 16.1: Configure Schema Registry client (url from application.yml, basic auth if configured)
  - 16.2: Configure AvroDeserializer for Kafka Consumer with Schema Registry URL and schema compatibility check
  - 16.3: Verify Avro schema compatibility for calls.metadata consumer (CallEvent + status schema)
  - 16.4: Verify Avro schema compatibility for transcription.enriched consumer (EnrichedTranscription schema)
  - 16.5: Verify Avro schema compatibility for calls.fraud-alerts consumer (FraudAlert schema)
  - 16.6: Integration test: verify consumer reads from all 3 topics using Schema Registry deserialization

---

## Medium Issues (M) — All Resolved

### M-1: Fraud statistics aggregation scope — spec vs tasks mismatch

**Статус:** ✅ ИСПРАВЛЕНО

**Где:** `specs/reporting-nps/spec.md` (CQRS Read Side — Fraud Statistics), `tasks.md` (section 5)

**Что было:** Spec scenario "Fraud statistics queryable via API" требует: "total alerts, alerts by pattern, alerts by severity, top offending phone numbers". Tasks section 5 определяют только: "count by phone, pattern, severity" — но нет отдельной задачи для severity-based aggregation.

```
Spec requires:
  ✓ total alerts
  ✓ alerts by pattern
  ✗ alerts by severity     ← no task for this
  ✓ top offending phone numbers
```

**Как исправлено:**
- tasks.md 5.2: обновлён — "Implement fraud statistics aggregation: count by phone, pattern, severity (HIGH/MEDIUM/LOW). Store aggregated stats in fraud_stats table (pre-aggregated model)."

---

### M-2: Cache invalidation mechanism undefined

**Статус:** ✅ ИСПРАВЛЕНО

**Где:** `specs/reporting-nps/spec.md` (Caching), `tasks.md` (section 11)

**Что было:** Spec требует "relevant cache entries are invalidated" when Kafka event changes call state. Но spec не определяет какие cache entries invalidate для каких event types. Tasks 11.3 говорят "invalidate relevant entries" но без mapping.

```
Event type              → Cache entries to invalidate
calls.metadata          → metadataCache(callId), reportCache (daily/agent)
transcription.enriched  → metadataCache(callId), reportCache, sentimentCache
calls.fraud-alerts      → reportCache (daily/agent), fraud stats cache

Spec не определяет этот mapping!
```

**Как исправлено:**
- spec.md: scenario "Cache invalidation on new events" обновлён — explicit mapping:
  - calls.metadata event → invalidate metadataCache(callId), reportCache (daily and agent reports for affected agent)
  - transcription.enriched event → invalidate metadataCache(callId), reportCache (daily and agent reports), sentimentCache
  - calls.fraud-alerts event → invalidate reportCache (daily and agent reports for affected agent)
- tasks.md 11.3: реализация cache invalidation mapping с тем же mapping
- tasks.md 13.5: unit test включает "invalidation per event type mapping"

---

### M-3: Test coverage command `./gradlew testCoverage` doesn't exist

**Статус:** ✅ ИСПРАВЛЕНО

**Где:** `tasks.md` (task 13.6)

**Что было:** Task 13.6: "Achieve >70% code coverage and verify with ./gradlew testCoverage"

**Как исправлено:**
- tasks.md 13.6: заменено на `./gradlew jacocoTestReport`

---

### M-4: FraudStats entity purpose unclear — per-event vs aggregated

**Статус:** ✅ ИСПРАВЛЕНО

**Где:** `tasks.md` (task 2.3)

**Что было:** Task 2.3 определяет "FraudStats entity for aggregated fraud statistics". Но spec описывает fraud alerts как stream (одно событие = один alert), а не как pre-aggregated data. Spec говорит "increments the fraud counter" — но где хранится counter? В FraudStats entity? В memory?

```
Spec: "increments the fraud counter for the corresponding phone number and pattern"
Tasks: "Create FraudStats entity for aggregated fraud statistics"
       "Implement fraud statistics aggregation: count by phone, pattern, severity"
       "Store aggregated fraud stats in PostgreSQL"

Вопрос: FraudStats — это per-phone/pattern aggregate table?
  fraud_stats: { phone, pattern, severity, total_count, last_alert_at }
```

**Как исправлено:**
- tasks.md 2.3: FraudStats JPA entity — полная schema определена:
  - phone (string), callId (string), pattern (string: FREQUENT_CALLS/NPS_ESCALATION/ANOMALOUS_DURATION), severity (string: HIGH/MEDIUM/LOW), count (integer), lastAlertAt (timestamp)
  - Явно указано: "This is a pre-aggregated table updated on each fraud alert event"

---

### M-5: Design "Open Questions: Нет" — false confidence

**Статус:** ✅ ИСПРАВЛЕНО

**Где:** `design.md` (Open Questions секция)

**Что было:** Design говорит "Нет — все технические решения определены в архитектурной документации". Но есть нерешённые вопросы:
- H-3: FK integrity — как handle out-of-order events?
- H-2: fraud correlation — как correlate phone-keyed fraud alerts with callId-keyed metadata?
- M-1: severity aggregation — include or exclude?
- M-4: FraudStats storage model — pre-aggregated or on-demand?
- M-2: cache invalidation strategy — which entries per event type?

**Как исправлено:**
- design.md: добавлена секция Open Questions с 6 вопросами (O-1..O-6):
  - O-1: FK integrity → Решено: buffer + retry (spec.md scenario "Orphan transcription event handling")
  - O-2: Fraud correlation → Решено: callId lookup (spec.md requirement "CQRS Read Side — Fraud Correlation")
  - O-3: FraudStats storage model → Решено: pre-aggregated (tasks.md 2.3 schema)
  - O-4: Cache invalidation → Решено: explicit mapping (spec.md + tasks.md 11.3)
  - O-5: v_full_call_info usage → Решено: использовать в API (spec.md + tasks.md 10.2)
  - O-6: Pagination → Предложено: default=50, max=200 (spec.md + tasks.md 8.5)

---

## Low Issues (L) — All Resolved

### L-1: Docker architecture specific — linux/arm64/v8

**Статус:** ✅ ИСПРАВЛЕНО

**Где:** `tasks.md` (task 1.5)

**Что было:** Task 1.5: "Create Dockerfile ... (multi-stage build, linux/arm64/v8)"

**Как исправлено:**
- tasks.md 1.5: "Create Dockerfile for reporting-nps (multi-stage build, support --platform flag for cross-platform builds)"

---

### L-2: Consumer group strategy — "3 ConsumerTemplate instances" ambiguous

**Статус:** ✅ ИСПРАВЛЕНО

**Где:** `design.md` (Decision 4)

**Что было:** Decision 4: "Один consumer group 'reporting-nps' с 3 ConsumerTemplate instances."

**Как исправлено:**
- design.md Decision 4: "Один consumer group 'reporting-nps' с 3 @KafkaListener beans (по одному на topic)."
- Обоснование обновлено: "3 @KafkaListener beans (по одному на topic) для логической изоляции"
- Каждый @KafkaListener имеет @KafkaListener(topicName = "...", groupId = "reporting-nps")

---

### L-3: No error handling strategy for consumer processing failures

**Статус:** ✅ ИСПРАВЛЕНО

**Где:** `specs/reporting-nps/spec.md`, `tasks.md` (section 17)

**Что было:** Spec определяет offset commit после successful processing. Но не определяет что делать при:
- Avro deserialization error (malformed event)
- PostgreSQL write failure
- Processing timeout

**Как исправлено:**
- spec.md: добавлен новый requirement "Error Handling" с 3 scenarios:
  - "Malformed Avro event" → log raw payload, send to internal DLQ, continue processing
  - "PostgreSQL write failure" → retry 3x with exponential backoff (1s, 2s, 4s), DLQ on final failure
  - "Processing timeout" → log warning, commit offset, retry on next cycle
- tasks.md секция 17 "Error Handling" (6 задач):
  - 17.1: malformed event handler
  - 17.2: PostgreSQL retry logic (3 attempts, exponential backoff)
  - 17.3: processing timeout handler (configurable, default 30s)
  - 17.4-17.6: unit tests для каждого scenario

---

### L-4: v_full_call_info view defined but not used by any API endpoint

**Статус:** ✅ ИСПРАВЛЕНО

**Где:** `specs/reporting-nps/spec.md` (Database Schema — Full Call View), `tasks.md` (section 10)

**Что было:** View `v_full_call_info` определяется и тестируется (tasks 10.1-10.5), но ни один REST API endpoint не использует её. Все API endpoints query call_metadata и call_transcriptions separately.

**Как исправлено:**
- spec.md: requirement "Database Schema — Full Call View" обновлён — "The view is used by daily report and agent report endpoints for efficient aggregation across metadata and transcription data."
- tasks.md 10.2: "View returns all metadata fields combined with transcription fields including segment, risk_level, priority. Used by daily report and agent report endpoints for efficient aggregation."

---

### L-5: No pagination for list endpoints

**Статус:** ✅ ИСПРАВЛЕНО

**Где:** `specs/reporting-nps/spec.md` (REST API — Metadata Queries), `tasks.md` (section 8, 13)

**Что было:** `/api/metadata/status/{status}` returns "a list of calls matching the specified status". No pagination defined.

**Как исправлено:**
- spec.md: scenario "Calls by status (paginated)" — "GET /api/metadata/status/{status} is called with optional pagination parameters (page, size)" → "returns a paginated list of calls matching the specified status with total count and page metadata"
- tasks.md 8.5: "Implement call lookup by status with pagination support (page, size query params, default page size=50, max page size=200)"
- tasks.md 13.7: "Write unit tests for pagination (verify page/size params, verify default page size=50, verify max page size=200)"

---

## Cross-Change Consistency

### ✓ Topic names match
| reporting-nps consumes | Produced by | Topic name | Match |
|------------------------|-------------|------------|-------|
| calls.metadata | call-processor + transcription-analyzer | calls.metadata | ✓ |
| transcription.enriched | transcription-analyzer | transcription.enriched | ✓ |
| calls.fraud-alerts | fraud-detector | calls.fraud-alerts | ✓ |

### ✓ Format consistency
All topics use Avro format — consistent across all changes.

### ✓ Enrichment fields (H-1 resolved)
transcription-analyzer enriches events with segment/risk_level/priority, reporting-nps теперь сохраняет их в call_transcriptions.

### ✓ Fraud alert correlation (H-2 resolved)
calls.fraud-alerts key=phone, reporting-nps коррелирует через callId lookup в call_metadata (spec requirement "CQRS Read Side — Fraud Correlation").

### ✓ Port assignment
reporting-nps uses port 8084 — no conflict with call-processor (8081), fraud-detector (8082), transcription-analyzer (8083).

---

## Финальная оценка (2026-09-10)

| Категория | Кол-во | Статус |
|-----------|--------|--------|
| Critical (H) | 4 | ✅ Все исправлены |
| Medium (M) | 5 | ✅ Все исправлены |
| Low (L) | 5 | ✅ Все исправлены |
| **Итого** | **14** | **14/14 исправлено** |

---

## Итоговый статус

**Change `reporting-nps` готов к implementation.**

Все 14 замечаний из первичного ревью исправлены:
- ✅ 4/4 critical issues resolved
- ✅ 5/5 medium issues resolved
- ✅ 5/5 low issues resolved
- ✅ 6/6 open questions resolved (O-1..O-6)
- ✅ Cross-change consistency verified (topics, formats, enrichment, correlation)
- ✅ Failure log updated (R-1..R-14)

### Артефакты, изменённые при финальном ревью
| Файл | Изменения |
|------|-----------|
| `specs/reporting-nps/spec.md` | H-1 (enrichment fields), H-2 (fraud correlation requirement), H-3 (orphan event handling), M-1 (severity in scenario), M-2 (cache invalidation mapping), L-3 (Error Handling requirement), L-4 (view usage), L-5 (pagination) |
| `design.md` | M-5 (Open Questions O-1..O-6), L-2 (ConsumerTemplate → @KafkaListener) |
| `tasks.md` | H-1 (2.2, 10.2, 15.5), H-2 (5.5-5.7), H-3 (4.5), H-4 (секция 16, 6 задач), M-1 (5.2), M-2 (11.3), M-3 (13.6), M-4 (2.3), L-1 (1.5), L-3 (секция 17, 6 задач), L-4 (10.2), L-5 (8.5, 13.7) |
| `_review.md` | Все статусы обновлены на ✅, финальная оценка |
| `failure-log.md` | R-1..R-14 записей, запись в историю изменений |
