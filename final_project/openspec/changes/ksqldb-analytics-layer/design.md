## Context

The system currently lacks a real-time SQL-based analytics layer. draft.md mentions ksqlDB as "аналитический слой" and ARCHITECTURE/03-container-architecture.md defines a ksqlDB container (port 8088), but no streams, queries, or implementation tasks exist. All analytics currently go through reporting-nps, creating unnecessary load and limiting ad-hoc query capabilities.

Current analytics flow:
```
calls.completed (Kafka)
    │
    └──▶ reporting-nps consumer ──▶ PostgreSQL ──▶ API queries
            [CQRS read side, reporting-nps change]

calls.fraud-alerts (Kafka)
    │
    └──▶ reporting-nps consumer ──▶ PostgreSQL ──▶ API queries
            [CQRS read side, reporting-nps change]
```

This change adds ksqlDB as an **additional analytics path** that runs in parallel to reporting-nps. No existing specs or tasks are modified.

## Goals / Non-Goals

**Goals:**
- Deploy ksqlDB server with REST API (port 8088)
- Create 2 persistent streams: `calls_completed_agg` and `fraud_alerts_filtered`
- Enable ad-hoc SQL queries via REST API without querying reporting-nps
- Zero breaking changes to existing services

**Non-Goals:**
- Replace reporting-nps as the source of truth for analytics
- Implement complex windowed joins (only simple aggregations)
- Add ksqlDB to CI/CD pipeline (manual testing only — CI/CD for ksqlDB deferred to future change; current scope is manual validation only)
- Implement ksqlDB security/authentication (internal network only)
- Materialized views (streams only)

## Decisions

### Decision 1: ksqlDB Embedded Mode vs Standalone Server

**Choice:** ksqlDB embedded mode (single container with server + CLI)

**Rationale:**
- Simpler deployment: 1 container instead of 2 (server + CLI)
- CLI commands can be sent via REST API (`/ksql` endpoint)
- No separate CLI container needed in production
- Aligns with Confluent recommended deployment for non-production environments

**Alternatives considered:**
- Standalone ksqlDB server + separate CLI container — more complex, CLI is ephemeral (not needed in production)
- ksqlDB cluster mode — overkill for 2 streams, requires multiple nodes

### Decision 2: Tumbling Window (calls_completed_agg) vs Hopping Window

**Choice:** Tumbling 5-minute window with 1-minute grace period for `calls_completed_agg`

**Rationale:**
- Call completion metrics don't need overlapping windows — tumbling is simpler and more predictable
- 5-minute window balances freshness vs. aggregation quality
- 1-minute grace period handles late-arriving events (clock skew, network delay)

**Alternatives considered:**
- Hopping window — overlapping windows add complexity, not needed for call count/NPS aggregation
- Session window — requires user-defined session boundaries, too complex for this use case

### Decision 3: Hopping Window (fraud_alerts_filtered) vs Tumbling Window

**Choice:** Hopping 10-minute window with 1-minute step for `fraud_alerts_filtered`

**Rationale:**
- Fraud alerts need more granular, real-time visibility — hopping window provides updates every 1 minute
- 10-minute window is wide enough to capture alert patterns
- 1-minute step ensures near-real-time dashboard updates

**Alternatives considered:**
- Tumbling window — 10-minute gaps between updates too slow for fraud monitoring
- Sliding window — Kafka Streams uses "hopping" terminology, not "sliding"

### Decision 4: LOW Severity Filtering

**Choice:** DROP events with `severity='LOW'` in ksqlDB stream

**Rationale:**
- LOW severity alerts are informational, not actionable in real-time
- Reduces output topic volume by ~60% (based on typical fraud alert distribution)
- LOW alerts still available in `calls.fraud-alerts` Kafka topic for batch analytics

**Alternatives considered:**
- Include LOW severity — increases data volume, dashboard noise
- Separate stream for LOW severity — overcomplicates architecture

### Decision 5: Persistent Queries vs Ad-hoc Only

**Choice:** 2 persistent streams (auto-start on boot) + ad-hoc queries via REST API

**Rationale:**
- Persistent streams ensure pre-aggregated data is always available
- Ad-hoc queries via REST API enable flexible exploration without code changes
- No additional persistent queries beyond the 2 defined streams (per user scope)

**Alternatives considered:**
- Ad-hoc only (no persistent streams) — no pre-aggregated data, all queries hit Kafka directly
- More persistent queries (materialized views) — scope creep, not requested

### Decision 6: Output Topics for Streams

**Choice:** Output topics pre-created by infrastructure change (extends `infrastructure` change) with explicit schemas; ksqlDB CREATE STREAM references pre-existing topics

**Rationale:**
- Output topics follow infrastructure standard: 6 partitions, replication factor 3
- Explicit schemas defined in spec for documentation and testing
- reporting-nps can consume output topics if needed in the future (forward-compatible)
- Pre-creation avoids ksqlDB auto-created topics with default 1 partition and no replication

**Alternatives considered:**
- ksqlDB auto-creates output topics — risk of default 1 partition, no replication factor
- No output topics (in-memory only) — data lost on ksqlDB restart

### Decision 7: SASL Authentication for ksqlDB → Kafka

**Choice:** ksqlDB connects to Kafka with SASL/PLAIN authentication using credentials configured via ksqlDB properties.

**Rationale:**
- Kafka cluster requires SASL/PLAIN authentication (per infrastructure spec)
- ksqlDB reads credentials from `bootstrap.servers` SASL config properties
- Credentials configured via docker-compose environment variables or ksqlDB properties file
- Schema Registry connection uses HTTP (no SASL needed, internal Docker network)

**Configuration:**
```
ksql.security.protocol=SASL_PLAINTEXT
ksql.sasl.mechanism=PLAIN
ksql.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="ksqldb" password="${KSQL_SASL_PASSWORD}";
schema.registry.url=http://schema-registry:8085
```

**Alternatives considered:**
- SASL_SSL — unnecessary, Docker network is isolated
- No authentication — not possible, Kafka enforces SASL/PLAIN

### Decision 8: fraud_alerts_filtered Aggregation Key

**Choice:** Group by `phone`, `pattern`, `severity` (not `agentId`)

**Rationale:**
- `calls.fraud-alerts` topic has key=phone and value fields {phone, pattern, severity, count} (defined in fraud-detector change)
- There is no `agentId` field in fraud-alerts events
- Aggregation by phone provides per-phone fraud visibility
- If agent-level fraud correlation is needed in the future, it requires adding `agentId` to fraud-detector schema or joining with `calls.metadata`

**Alternatives considered:**
- Group by `agentId` — field does not exist in fraud-alerts schema (blocking issue)
- Add `agentId` to fraud-detector schema — requires modifying fraud-detector change, out of scope

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| **Schema mismatch** — ksqlDB reads Avro events, schema change breaks stream | High (stream stops processing) | Schema Registry BACKWARD compatibility; monitor stream lag |
| **Late-arriving events** — events arrive after window expires | Low (data slightly stale) | 1-minute grace period handles minor delays; accept minor data loss for major delays |
| **ksqlDB restart** — persistent queries resume from last offset | Low (brief gap in aggregation) | ksqlDB stores query state in internal Kafka topics; restart < 30 seconds |
| **REST API load** — ad-hoc queries may hit ksqlDB hard | Medium (query timeout, CPU spike) | 30-second query timeout; rate limiting (future enhancement) |
| **No authentication** — ksqlDB REST API is open on port 8088 | Medium (unauthorized queries) | Internal network only (docker network); no external exposure |
| **Data duplication** — ksqlDB output topics may have duplicates | Low (aggregation uses COUNT, idempotent) | ksqlDB at-least-once semantics; accept minor count inflation |

## Migration Plan

### Phase 1: Deploy ksqlDB (no streams)
1. Add `ksqldb-server` service to docker-compose (extends `infrastructure` change)
2. Verify REST API is available (port 8088)
3. Verify connection to Kafka and Schema Registry
4. Run test query: `SELECT * FROM calls.completed LIMIT 5` — verify events are readable

### Phase 2: Create Persistent Streams
1. Create `calls_completed_agg` stream via REST API:
   ```sql
   CREATE STREAM calls_completed_agg WITH (KAFKA_TOPIC='calls.completed.agg', PARTITIONS=6) AS
   SELECT agentId,
          COUNT(*) AS call_count,
          AVG(npsScore) AS avg_nps_score,
          MAX(completedAt) AS last_call_at
   FROM calls.completed
   WINDOW TUMBLING (SIZE 5 MINUTES, GRACE PERIOD 1 MINUTE)
   GROUP BY agentId
   EMIT CHANGES;
   ```
2. Create `fraud_alerts_filtered` stream via REST API:
   ```sql
   CREATE STREAM fraud_alerts_filtered WITH (KAFKA_TOPIC='calls.fraud-alerts.agg', PARTITIONS=6) AS
   SELECT phone, pattern, severity,
          COUNT(*) AS alert_count
   FROM calls.fraud-alerts
   WHERE severity IN ('HIGH', 'MEDIUM')
   WINDOW HOPPING (SIZE 10 MINUTES, ADVANCE BY 1 MINUTE)
   GROUP BY phone, pattern, severity
   EMIT CHANGES;
   ```
   Note: `phone` is used instead of `agentId` because `calls.fraud-alerts` has key=phone and no agentId field (fraud-detector change schema).
3. Verify output topics are created and receiving data

### Phase 3: Validate Ad-hoc Queries
1. Test ad-hoc query: `SELECT * FROM calls_completed_agg WHERE agentId = 'agent-001'`
2. Test ad-hoc query: `SELECT * FROM fraud_alerts_filtered WHERE severity = 'HIGH'`
3. Verify timeout behavior (query > 30 seconds returns 408)

### Phase 4: Stabilization
1. Run for 1 week with streams active
2. Monitor stream lag, query latency, error rate
3. Verify persistent queries survive ksqlDB restart

### Rollback Strategy
- Drop streams via REST API: `DROP STREAM calls_completed_agg; DROP STREAM fraud_alerts_filtered;`
- Stop ksqlDB container — no data loss (Kafka topics retain all events)
- reporting-nps continues to work unchanged

## Open Questions

| ID | Question | Impact | Status |
|----|----------|--------|--------|
| O-1 | What is the exact field name for agentId in `calls.completed` Avro schema? | Stream GROUP BY clause | Resolved: call-processor spec defines `agentId` field in CallEvent |
| O-2 | What is the exact field name for severity in `calls.fraud-alerts` Avro schema? | Stream WHERE clause | Resolved: fraud-detector spec defines `severity` field (HIGH/MEDIUM/LOW) |
| O-3 | Should ksqlDB output topics be exposed to reporting-nps in the future? | Architecture decision | Deferred: output topics exist internally; reporting-nps can consume if needed |
| O-4 | Why fraud_alerts_filtered aggregates by phone, not agentId? | Design decision documentation | Resolved: Decision 7 — fraud-alerts schema has no agentId field |
