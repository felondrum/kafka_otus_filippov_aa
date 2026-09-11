## Context

The current architecture uses a manual JDBC writer (DualWriter) in transcription-analyzer for PostgreSQL persistence. This couples data pipeline logic to the application service and lacks built-in offset management, transformations, and reliability guarantees. The design.md Decision 2 already mentions "Ручной JDBC writer + Kafka Connect JDBC Sink (дублирование для надёжности)" but only the manual JDBC path has implementation tasks.

The existing dual-write flow:
```
transcription.enriched (Kafka)
    │
    ├──▶ transcription-analyzer DualWriter (JDBC) ──▶ PostgreSQL
    │       [9 tasks in transcription-analyzer change]
    │
    └──▶ reporting-nps consumer ──▶ PostgreSQL (CQRS read)
            [part of reporting-nps change]
```

This change adds Kafka Connect JDBC Sink as a **third path** (primary), while DualWriter remains as fallback. No existing specs or tasks are modified.

## Goals / Non-Goals

**Goals:**
- Deploy Kafka Connect with JDBC Sink connector for `transcription.enriched` → PostgreSQL
- Implement field transformations: ExtractField, ReplaceString
- Achieve automatic offset management and built-in retry via Kafka Connect
- Retain DualWriter in transcription-analyzer as fallback/retry path
- Zero breaking changes to existing services

**Non-Goals:**
- Replace DualWriter in transcription-analyzer (removal is out of scope)
- Modify reporting-nps CQRS read side (unchanged)
- Add new Kafka topics (uses existing `transcription.enriched`)
- Implement Kafka Connect Source connectors (JDBC Sink only)
- Schema evolution / Avro schema migration

## Decisions

### Decision 1: Kafka Connect JDBC Sink Connector (Confluent) vs Apache Kafka Connect

**Choice:** Confluent JDBC Sink Connector (`io.confluent.connect.jdbc.JdbcSinkConnector`)

**Rationale:**
- Confluent connector has built-in Avro schema support (no manual deserialization needed)
- Native Schema Registry integration (required for `transcription.enriched` Avro topic)
- Built-in upsert/merge insert modes (required for `call_transcriptions` table)
- Enterprise-grade retry and DLQ support
- Project already uses Confluent Platform (Kafka, Schema Registry via Confluent images)

**Alternatives considered:**
- Apache Kafka Connect JDBC Connector (`JdbcSinkConnector`) — free but lacks Avro support, requires manual schema handling
- Debezium CDC — overkill (we produce events, don't capture DB changes)

### Decision 2: Upsert Mode vs Insert-Only

**Choice:** `insert.mode=upsert` with `pk.mode=RecordKey` and `pk.fields=call_id`

**Rationale:**
- `call_transcriptions` table may have existing records from DualWriter or reporting-nps
- Upsert ensures idempotent writes — if Kafka Connect reprocesses an event, it updates instead of duplicating
- `pk.mode=RecordKey` uses the Kafka record key (callId) as primary key — this is the correct mode when the Kafka key matches the desired PK field
- Matches the existing dual-write semantics (Kafka event is produced BEFORE PostgreSQL write)

**Alternatives considered:**
- `insert.mode=insert` — would create duplicates if events are retried
- `insert.mode=update` — would fail if record doesn't exist yet
- `pk.mode=record_value` — requires entire value to be PK, incompatible with `pk.fields` parameter

### Decision 3: Transformations Pipeline

**Choice:** 2 transformations in sequence: `extract → replace`

```
Avro event (transcription.enriched value)
    │
    ▼
ExtractField (extract `value` field from wrapper)
    │
    ▼
ReplaceString (escape SQL injection chars: ', ", \ in text fields)
    │
    ▼
PostgreSQL call_transcriptions row
```

**Rationale:**
- `ExtractField` — `transcription.enriched` Avro value contains nested fields; we need to flatten to table columns
- `ReplaceString` — security: prevent SQL injection in text fields (transcription_text, problem, solution)
- **Avro-to-SQL type conversion is automatic** — Confluent JDBC Connector handles this via Schema Registry. No explicit `ConvertFieldSchema` SMT is needed (and `ConvertFieldSchema` does not exist as a Kafka Connect SMT).

**Alternatives considered:**
- Single SMT (Single Message Transform) — not possible, each transformation has different purpose
- No ReplaceString — risk of SQL injection in untrusted text fields
- Explicit ConvertFieldSchema — not needed; Confluent Connector handles type mapping automatically via Schema Registry

### Decision 4: Parallelism (tasks.max=6)

**Choice:** `tasks.max=6` for the connector

**Rationale:**
- `transcription.enriched` topic has 6 partitions (defined in infrastructure change, tasks 6.2)
- `tasks.max=6` allows one task per partition for maximum parallelism
- PostgreSQL connection pool should be sized accordingly (min 6 connections, max 10 shared with DualWriter + reporting-nps)

**Alternatives considered:**
- `tasks.max=1` — simpler but bottleneck on single partition
- `tasks.max=2` — would underutilize 6 partitions (3x under-provisioned)

### Decision 5: DualWriter Retention (Not Removal)

**Choice:** Keep DualWriter in transcription-analyzer as fallback; do NOT remove

**Rationale:**
- DualWriter provides redundancy if Kafka Connect is unavailable
- transcription-analyzer change spec already defines DualWriter behavior (dual-write scenario)
- Removing DualWriter would require modifying transcription-analyzer spec/design/tasks — out of scope for this change
- 3-path architecture (Kafka Connect + DualWriter + reporting-nps) maximizes reliability

**Alternatives considered:**
- Remove DualWriter, use Kafka Connect only — cleaner architecture but less redundancy, requires modifying another change
- Remove reporting-nps JDBC write, use Kafka Connect only — would require modifying reporting-nps change

### Decision 6: Offset Management via Kafka Connect Internal Topic

**Choice:** Use Kafka Connect's built-in offset storage (`_connect_offsets` topic)

**Rationale:**
- Kafka Connect manages offsets automatically — no custom code needed
- Offsets are committed periodically (default: 5 minutes) and on connector shutdown
- Survives connector restarts — no duplicate writes
- Aligns with Kafka Connect best practices

**Alternatives considered:**
- Custom offset tracking in application code — reinventing the wheel, error-prone
- PostgreSQL-based offset storage — adds complexity, no benefit over Kafka Connect native

## Schema Evolution Strategy

In case of schema changes for the `transcription.enriched` topic, the following strategy will be employed to prevent connector failure and ensure data consistency:

1. **Compatibility Policy:** The Schema Registry is configured with `BACKWARD` compatibility. Any schema change that is not backward compatible will be rejected during producer schema registration, preventing invalid events from reaching Kafka.
2. **Breaking Changes:** If a breaking schema change is unavoidable:
    - **Step 1:** Create a new PostgreSQL table with the updated schema.
    - **Step 2:** Deploy a new Kafka Connect JDBC Sink connector instance configured to write to the new table.
    - **Step 3:** Use a new Kafka topic or a new version of the existing topic if the data format allows.
    - **Step 4:** Update downstream analytics (reporting-nps, ksqlDB) to query the new table/topic.
3. **Testing:** All schema changes must be tested in a staging environment using `Testcontainers` (simulating Kafka and PostgreSQL) to ensure the Kafka Connect JDBC Sink connector handles the evolution correctly before production deployment.
4. **Connector Redeployment:** If the schema change requires a reconfiguration of SMTs (e.g., field renaming, new mandatory fields), the connector will be redeployed via the REST API with the updated configuration. The old connector instance will be stopped, and offsets will be migrated if necessary, or the connector will be allowed to catch up from the current topic position if the table structure allows seamless transition.

## Risks / Trade-offs
...

## Migration Plan

### Phase 1: Deploy Kafka Connect (no data flow yet)
1. Add `kafka-connect` service to docker-compose (extends `infrastructure` change)
2. Create `kafka-connect-user` DB user with minimal privileges (defined in `infrastructure` change)
3. Verify connector REST API is available (port 8083)
4. Deploy connector in "dry-run" mode (no writes, only log verification)

### Phase 2: Enable Kafka Connect JDBC Sink (dual-write active)
1. Deploy JDBC Sink connector via REST API (`/connectors/kafka-connect-jdbc-sink`)
2. Verify events flow: `transcription.enriched` → Kafka Connect → PostgreSQL
3. Monitor both Kafka Connect writes AND DualWriter writes
4. Verify no duplicate data (upsert mode working correctly)

### Phase 3: Stabilization
1. Run for 1 week with both paths active
2. Monitor DLQ topic for failed events
3. Verify offset management (no duplicates on connector restart)
4. Collect metrics: write latency, throughput, error rate

### Rollback Strategy
- If Kafka Connect causes issues: delete connector via REST API (`DELETE /connectors/kafka-connect-jdbc-sink`)
- DualWriter continues to work — no data loss
- Kafka Connect container can be stopped without affecting transcription-analyzer or reporting-nps

## Open Questions

| ID | Question | Impact | Status |
|----|----------|--------|--------|
| O-1 | What is the exact schema of `transcription.enriched` Avro value? (nested fields, field names) | Determines ExtractField configuration | Resolved: transcription-analyzer spec defines enriched event schema |
| O-2 | How should Kafka Connect handle `call_transcriptions` table creation? | Table must exist before connector starts | Resolved: `auto.create=false`; table created by infrastructure change init-db.sql (task 3.2) |
| O-3 | What PostgreSQL connection pool size for Kafka Connect? | Affects max parallelism | Decision: min=6 (matches tasks.max=6), max=10 (shared with DualWriter + reporting-nps) |
