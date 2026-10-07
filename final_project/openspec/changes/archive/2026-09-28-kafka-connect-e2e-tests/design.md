## Context

Kafka Connect JDBC Sink connector is deployed (kafka-connect-jdbc-sink change) but has no e2e tests. Current `e2e-bootstrap.sh` only tests HTTP API and direct PostgreSQL INSERT. The connector reads JSON events from `transcription.enriched` topic (key=callId), applies ExtractField + ReplaceString transformations, and writes to PostgreSQL `call_transcriptions` table with upsert semantics. DualWriter in transcription-analyzer remains as fallback.

## Goals / Non-Goals

**Goals:**
- Isolated e2e test that doesn't depend on transcription-analyzer or full pipeline
- Direct JSON event production via `kafka-console-producer`
- Cover all 7 phases: health, data flow, transformations, upsert, retry/DLQ, offset recovery, DualWriter fallback
- CI/CD-ready: exit code 0/1, structured output, timeout per phase

**Non-Goals:**
- Testing transcription-analyzer (covered by other changes)
- Testing call-processor or fraud-detector (covered by e2e-bootstrap.sh)
- Performance/load testing (separate change)
- Chaos engineering (separate change)

## Decisions

### Decision 1: Direct JSON Producer vs Full Pipeline

**Choice:** Direct JSON event production via `kafka-console-producer`

```
APPROACH A: Direct Producer (CHOSEN)
├── test script → kafka-console-producer → transcription.enriched → Kafka Connect → PostgreSQL
├── + изолированный, быстрый (~15 min)
├── + фокус на Kafka Connect
└── - не тестирует transcription-analyzer

APPROACH B: Full Pipeline
├── caller → call-processor → transcription-analyzer → transcription.enriched → Kafka Connect → PostgreSQL
├── + полный end-to-end
└── - 10+ минут, много точек отказа, сложно отлаживать
```

**Rationale:** Kafka Connect test should be isolated to validate connector behavior. Full pipeline testing is covered by e2e-bootstrap.sh for HTTP API. Direct producer allows focused testing of JSON serialization, transformations, and JDBC writes.

### Decision 2: JSON Serialization Method

**Choice:** `kafka-console-producer` with JSON input + Schema Registry

```
JSON CONVERSION OPTIONS:
├── kafka-console-producer (Confluent CLI)
│   ├── + встроен в Confluent Platform
│   ├── + автоматически конвертирует JSON через Schema Registry
│   └── - требует зарегистрированную схему
├── Python json library
│   ├── + полный контроль
│   └── - требует установку зависимостей
└── Java test utility
    ├── + точная модель
    └── - требует компиляцию
```

**Rationale:** `kafka-console-producer` is already available in Confluent Platform Docker image. Schema `EnrichedTranscription.json` is already registered. JSON input format is simple and matches our test data structure.

### Decision 3: Test Structure — Single Script vs Multiple Scripts

**Choice:** Single `kafka-connect-e2e-test.sh` script with 7 phases

```
SINGLE SCRIPT (CHOSEN):
├── infrastructure/kafka-connect-e2e-test.sh
│   ├── Phase 1: Connector Health (5 tests)
│   ├── Phase 2: Data Flow (4 tests)
│   ├── Phase 3: Transformations (3 tests)
│   ├── Phase 4: Upsert (2 tests)
│   ├── Phase 5: Retry & DLQ (3 tests)
│   ├── Phase 6: Offset Recovery (2 tests)
│   └── Phase 7: DualWriter Fallback (2 tests)
└── Total: 22 tests, ~45 minutes

MULTIPLE SCRIPTS:
├── kafka-connect-health-test.sh
├── kafka-connect-dataflow-test.sh
├── kafka-connect-transformations-test.sh
└── ... (7 separate scripts)
└── - сложнее orchestrate, больше overhead
```

**Rationale:** Single script simplifies CI/CD integration (one Makefile target, one exit code). Phases are sequential and dependent (need connector healthy before data flow tests). Shared setup/teardown logic is easier in one file.

### Decision 4: Waiting Strategy for Kafka Connect Processing

**Choice:** Polling with timeout (not fixed sleep)

```
WAITING STRATEGIES:
├── Fixed sleep (simple, unreliable)
│   ├── sleep 30
│   └── - может быть недостаточно или слишком долго
├── Polling with timeout (CHOSEN)
│   ├── while [ $WAITED -lt $MAX_WAIT ]; do
│   │   ├── count rows in PostgreSQL
│   │   ├── if count == expected: break
│   │   └── sleep 5; WAITED=$((WAITED + 5))
│   └── + адаптивный, надёжный
└── Kafka consumer offset check
    ├── + точный
    └── - сложно из bash
```

**Rationale:** Kafka Connect batch processing (batch.size=100, flush.max.records=1000) means processing time varies. Polling PostgreSQL for row count is reliable and simple. 5-second polling interval with 60-second timeout balances speed and reliability.

### Decision 5: DLQ Testing Approach

**Choice:** Simulate failure by revoking INSERT permission, then restore

```
DLQ TEST APPROACH:
├── Option A: Stop PostgreSQL (too aggressive, affects all tests)
├── Option B: Revoke INSERT permission from kafka-connect-user (CHOSEN)
│   ├── + точечный, не ломает другие операции
│   ├── + быстро восстановить: GRANT INSERT
│   └── - требует superuser (postgres) для revoke/grant
└── Option C: Produce to non-existent topic (doesn't test JDBC write path)
```

**Rationale:** Revoking INSERT permission simulates a real JDBC write failure without stopping PostgreSQL. The test script runs as root in Docker, so superuser access is available. Permission is restored after test completes.

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| `kafka-avro-console-producer` requires schema to be registered | Tests fail if schema missing | Schema is registered by schema-init service (depends_on in docker-compose) |
| Kafka Connect processing delay causes test flakiness | Intermittent failures | Polling with 60s timeout, not fixed sleep |
| DLQ test (permission revoke) affects other connectors | False positives | Run DLQ test in isolation, restore permissions immediately |
| Long test duration (~45 min) slows CI/CD | Developer friction | Phase skipping via env var (e.g., `KAFKA_CONNECT_E2E_SKIP_DLQ=1`) |
| DualWriter test requires stopping Kafka Connect container | Affects other tests | Run DualWriter test last, restart Kafka Connect after |

## Migration Plan

1. **Phase 1:** Add `kafka-connect-e2e-test.sh` to repo (no infrastructure changes)
2. **Phase 2:** Add `make kafka-connect-e2e` target to Makefile
3. **Phase 3:** Run tests manually to validate
4. **Phase 4:** Add to CI/CD pipeline (optional, after manual validation)

**Rollback:** Delete `kafka-connect-e2e-test.sh` and revert Makefile change. No infrastructure impact.

## Open Questions

| ID | Question | Impact | Status |
|----|----------|--------|--------|
| O-1 | Should DLQ test be optional (skip in CI)? | CI/CD pipeline configuration | Deferred: default enabled, can add `KAFKA_CONNECT_E2E_SKIP_DLQ=1` |
| O-2 | What is the exact column mapping for `segment`, `riskLevel`, `priority` fields? | Test data validation | Resolved: These fields exist in `EnrichedTranscription.json` but not in `call_transcriptions` table — they are ignored by JDBC Sink (auto.evolve=true handles schema mismatch) |
