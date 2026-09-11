## Why

The current architecture uses a manual JDBC writer in transcription-analyzer for PostgreSQL persistence, which couples data pipeline logic to the application service and lacks built-in offset management, transformations, and reliability guarantees. Additionally, the system lacks a real-time SQL-based analytics layer — ksqlDB is mentioned in draft.md and ARCHITECTURE but never implemented, leaving ad-hoc analytics impossible without querying reporting-nps directly.

This change introduces Kafka Connect JDBC Sink as the primary data pipeline for PostgreSQL persistence (with existing DualWriter retained as fallback) and adds ksqlDB as a lightweight real-time analytics layer for call metrics and fraud monitoring.

## What Changes

- **Kafka Connect JDBC Sink** — add Kafka Connect container with JDBC Sink connector for `transcription.enriched` → PostgreSQL `call_transcriptions`
  - Add transformations: `ExtractField` (call_id, transcription_text, sentiment, urgency, problem, solution, confidence), `ReplaceString` (sanitize text fields)
  - Avro-to-SQL type conversion is automatic via Confluent JDBC Connector + Schema Registry (no explicit ConvertFieldSchema SMT needed)
  - Automatic offset management and built-in retry with dead-letter queue
  - DualWriter in transcription-analyzer remains as fallback/retry path
  - reporting-nps CQRS read side remains unchanged
- **ksqlDB Analytics Layer** — add ksqlDB container with 2 persistent streams:
  - `calls_completed_agg` — real-time aggregation of `calls.completed` events by `agentId`
  - `fraud_alerts_filtered` — real-time filtering and aggregation of `calls.fraud-alerts` by severity and pattern
- **Infrastructure extension** — add Kafka Connect and ksqlDB services to docker-compose (extends `infrastructure` change)

## Capabilities

### New Capabilities

- `kafka-connect-jdbc-sink`: Kafka Connect JDBC Sink connector for automatic PostgreSQL synchronization from `transcription.enriched` topic with field transformations, offset management, and retry logic
- `ksqldb-analytics`: ksqlDB real-time analytics layer with persistent streams for call metrics aggregation by agentId and fraud alerts filtering by severity/pattern

### Modified Capabilities

<!-- No existing spec requirements are changed. DualWriter in transcription-analyzer remains; Kafka Connect is an ADDITIONAL primary path, not a replacement. -->

## Impact

**Affected services:**
- `transcription-analyzer` — no spec changes (DualWriter remains), but data flow diagram updated
- `reporting-nps` — no spec changes (CQRS read side remains)

**Infrastructure:**
- New docker-compose services: `kafka-connect` (port 8083), `ksqldb-server` (port 8088)
- New database user: `kafka-connect-user` with INSERT privileges (defined in `infrastructure` change security)
- New topics: none (uses existing `transcription.enriched`)

**Dependencies:**
- Depends on `infrastructure` change (Kafka, PostgreSQL, Schema Registry must be running)
- Depends on `transcription-analyzer` change (produces `transcription.enriched` topic)
- Depends on `call-processor` change (produces `calls.completed` topic for ksqlDB)
- Depends on `fraud-detector` change (produces `calls.fraud-alerts` topic for ksqlDB)

**Breaking changes:** None. This is a pure addition — all existing paths (DualWriter, reporting-nps CQRS) remain unchanged.
