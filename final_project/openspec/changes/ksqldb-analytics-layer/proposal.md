## Why

The system currently lacks a real-time SQL-based analytics layer. draft.md mentions ksqlDB as "аналитический слой" and ARCHITECTURE/03-container-architecture.md defines a ksqlDB container (port 8088), but no streams, queries, or implementation tasks exist. This forces all analytics to go through reporting-nps, creating unnecessary load and limiting ad-hoc query capabilities.

This change introduces ksqlDB as a lightweight real-time analytics layer with two persistent streams: call metrics aggregation by agentId and fraud alerts filtering.

## What Changes

- **ksqlDB Server** — add ksqlDB container (port 8088) with REST API for ad-hoc queries
- **Persistent Stream: calls_completed_agg** — aggregate `calls.completed` events by `agentId` in real-time:
  - Count of completed calls per agent
  - Average NPS score per agent
  - Last call timestamp per agent
  - Window: tumbling 5-minute windows with 1-minute grace period
- **Persistent Stream: fraud_alerts_filtered** — filter and aggregate `calls.fraud-alerts` by severity and pattern:
  - Count of alerts per phone, pattern, severity
  - Filter: only HIGH and MEDIUM severity alerts (ignore LOW for real-time dashboard)
  - Window: hopping 10-minute window with 1-minute step
- **REST API** — ksqlDB REST endpoint (port 8088) for external analytics tools
- **No ad-hoc persistent queries beyond the 2 streams** — ad-hoc queries via REST API only, no auto-started additional queries

## Capabilities

### New Capabilities

- `ksqldb-analytics`: ksqlDB real-time analytics layer with persistent streams for `calls.completed` aggregation by agentId and `calls.fraud-alerts` filtering by severity/pattern, exposed via REST API (port 8088)

### Modified Capabilities

<!-- No existing spec requirements are changed. This is a new analytics layer that complements reporting-nps, not replaces it. -->

## Impact

**Affected services:**
- `reporting-nps` — no spec changes. ksqlDB provides pre-aggregated data that CAN be consumed by reporting-nps API, but reporting-nps CQRS read side remains the source of truth
- `call-processor` — no changes (produces `calls.completed` topic, consumed by ksqlDB)
- `fraud-detector` — no changes (produces `calls.fraud-alerts` topic, consumed by ksqlDB)

**Infrastructure:**
- New docker-compose service: `ksqldb-server` (port 8088, embedded mode with REST API)
- Schema Registry dependency: ksqlDB reads Avro schemas from Schema Registry (port 8085)

**Dependencies:**
- Depends on `infrastructure` change (Kafka, Schema Registry must be running)
- Depends on `call-processor` change (`calls.completed` topic must exist)
- Depends on `fraud-detector` change (`calls.fraud-alerts` topic must exist)

**Breaking changes:** None. ksqlDB is an additional read path, not a replacement for reporting-nps.
