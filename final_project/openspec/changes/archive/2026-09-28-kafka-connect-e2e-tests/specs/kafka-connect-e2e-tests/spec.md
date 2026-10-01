## Purpose

Provides comprehensive end-to-end testing for Kafka Connect JDBC Sink connector — validates connector health, data flow (Avro → PostgreSQL), field transformations, upsert semantics, retry/DLQ behavior, offset recovery, and DualWriter fallback.

## ADDED Requirements

### Requirement: Connector Health & Configuration Verification

The e2e test suite SHALL verify that the Kafka Connect JDBC Sink connector is deployed, healthy, and correctly configured.

The test suite SHALL:
- Send GET request to Kafka Connect REST API (`http://localhost:8086/connectors/kafka-connect-jdbc-sink`) and verify HTTP 200
- Verify connector status is `RUNNING` (not `PAUSED` or `FAILED`)
- Verify connector configuration includes: `connector.class=io.confluent.connect.jdbc.JdbcSinkConnector`, `tasks.max=6`, `topics=transcription.enriched`, `insert.mode=upsert`, `pk.mode=RecordKey`, `pk.fields=call_id`
- Verify 6 connector tasks are created (matching `tasks.max=6`)
- Verify Schema Registry is accessible and Avro schemas are resolvable

#### Scenario: Connector is deployed and accessible
- **WHEN** GET request is sent to `/connectors/kafka-connect-jdbc-sink`
- **THEN** HTTP 200 is returned with connector metadata

#### Scenario: Connector is running
- **WHEN** GET request is sent to `/connectors/kafka-connect-jdbc-sink/status`
- **THEN** `connector.status` is `RUNNING`

#### Scenario: Connector configuration is correct
- **WHEN** GET request is sent to `/connectors/kafka-connect-jdbc-sink/config`
- **THEN** configuration includes `insert.mode=upsert`, `pk.mode=RecordKey`, `tasks.max=6`

#### Scenario: All 6 tasks are created
- **WHEN** GET request is sent to `/connectors/kafka-connect-jdbc-sink/status`
- **THEN** `tasks` array contains exactly 6 task entries

#### Scenario: Schema Registry is accessible
- **WHEN** GET request is sent to Schema Registry (`http://localhost:8085/subjects`)
- **THEN** HTTP 200 is returned with list of registered subjects

### Requirement: Data Flow — Avro to PostgreSQL

The e2e test suite SHALL verify that Avro-encoded events from `transcription.enriched` topic are correctly written to PostgreSQL `call_transcriptions` table via Kafka Connect.

The test suite SHALL:
- Produce Avro-encoded events using `kafka-avro-console-producer` with Schema Registry
- Use `EnrichedTranscription.avsc` schema for serialization
- Wait for Kafka Connect to consume and write (up to 60 seconds)
- Verify row count in `call_transcriptions` matches expected
- Verify field mapping: `callId` → `call_id`, `transcriptionText` → `transcription_text`, `sentiment` → `sentiment`, `urgency` → `urgency`, `problem` → `problem`, `solution` → `solution`, `confidence` → `confidence`

#### Scenario: Single Avro event produces one row
- **WHEN** 1 Avro event is produced to `transcription.enriched` topic
- **THEN** exactly 1 row appears in `call_transcriptions` table within 60 seconds

#### Scenario: Field values are correctly mapped
- **WHEN** Avro event with `callId="test-123"`, `sentiment="POSITIVE"`, `confidence=0.95` is produced
- **THEN** PostgreSQL row has `call_id="test-123"`, `sentiment="POSITIVE"`, `confidence=0.95`

#### Scenario: Multiple events produce multiple rows
- **WHEN** 10 Avro events with unique `callId` values are produced
- **THEN** exactly 10 rows exist in `call_transcriptions` with correct field values

#### Scenario: Event with all nullable fields is handled
- **WHEN** Avro event with null `problem`, `solution`, `urgency` is produced
- **THEN** PostgreSQL row has NULL values for those columns (no error)

### Requirement: Field Transformations (ExtractField + ReplaceString)

The e2e test suite SHALL verify that Kafka Connect transformations correctly flatten and sanitize Avro events before PostgreSQL writes.

The test suite SHALL:
- Verify `ExtractField` transformation extracts `value` field from nested Avro structure
- Verify `ReplaceString` transformation escapes SQL injection characters (`'`, `"`, `\`) in text fields
- Produce events with malicious text containing `'; DROP TABLE call_transcriptions; --`
- Verify text is stored safely in PostgreSQL (no table dropped, text escaped)

#### Scenario: ExtractField flattens nested structure
- **WHEN** Avro event with nested `value` field is produced
- **THEN** `ExtractField` transformation extracts fields to flat columns

#### Scenario: ReplaceString escapes single quotes
- **WHEN** event with `transcription_text` containing `'` is produced
- **THEN** PostgreSQL stores escaped `'` (no SQL injection)

#### Scenario: ReplaceString escapes double quotes
- **WHEN** event with `transcription_text` containing `"` is produced
- **THEN** PostgreSQL stores escaped `"` (no SQL injection)

#### Scenario: ReplaceString escapes backslashes
- **WHEN** event with `transcription_text` containing `\` is produced
- **THEN** PostgreSQL stores escaped `\` (no SQL injection)

#### Scenario: SQL injection attempt is safely stored
- **WHEN** event with `transcription_text="'; DROP TABLE call_transcriptions; --"` is produced
- **THEN** PostgreSQL stores escaped text, table remains intact

### Requirement: Upsert Semantics

The e2e test suite SHALL verify that Kafka Connect correctly handles upsert operations — updating existing rows instead of creating duplicates.

The test suite SHALL:
- Produce event with `callId="upsert-test-1"`
- Wait for write to PostgreSQL
- Produce event with same `callId="upsert-test-1"` but different `sentiment`
- Wait for write to PostgreSQL
- Verify row count is still 1 (no duplicate)
- Verify `sentiment` field is updated to new value

#### Scenario: Duplicate callId updates existing row
- **WHEN** event with existing `callId` is produced with different `sentiment`
- **THEN** existing row is updated (not duplicated) in `call_transcriptions`

#### Scenario: Row count remains stable after upsert
- **WHEN** 3 unique events + 1 duplicate event are produced
- **THEN** exactly 3 rows exist in `call_transcriptions` (no duplicates)

### Requirement: Retry and Dead Letter Queue (DLQ)

The e2e test suite SHALL verify that Kafka Connect retry logic and DLQ work correctly when writes fail.

The test suite SHALL:
- Simulate database write failure (e.g., revoke INSERT permission temporarily)
- Verify connector retries with exponential backoff (1s, 2s, 4s)
- After `max.retries=3`, verify event is sent to DLQ topic `transcription.enriched.dlq`
- Verify DLQ topic contains the failed event with error details
- Verify `errors.deadletterqueue.topic.name=transcription.enriched.dlq` in connector config

#### Scenario: Connector retries failed write
- **WHEN** database write fails (permission denied)
- **THEN** connector retries with backoff (1s, 2s, 4s) up to 3 times

#### Scenario: Failed event is sent to DLQ after max retries
- **WHEN** max retries (3) exceeded
- **THEN** event is written to `transcription.enriched.dlq` topic

#### Scenario: DLQ topic contains error details
- **WHEN** event is in DLQ topic
- **THEN** DLQ event includes error message and original event payload

#### Scenario: DLQ topic is configured in connector
- **WHEN** connector config is retrieved
- **THEN** `errors.deadletterqueue.topic.name=transcription.enriched.dlq` is set

### Requirement: Offset Recovery

The e2e test suite SHALL verify that Kafka Connect correctly manages offsets and resumes from last committed position after restart.

The test suite SHALL:
- Produce 5 events and wait for all to be written
- Stop Kafka Connect container
- Verify offsets are committed to `_connect_offsets` topic
- Restart Kafka Connect container
- Verify connector resumes from last offset (no duplicate writes)
- Produce 3 more events after restart
- Verify total row count = 5 + 3 = 8 (no duplicates from old events)

#### Scenario: Offsets are committed periodically
- **WHEN** connector is running
- **THEN** offsets are committed to `_connect_offsets` topic (every 5 minutes default)

#### Scenario: Connector resumes after restart
- **WHEN** Kafka Connect container is restarted
- **THEN** connector resumes from last committed offset without reprocessing old events

#### Scenario: No duplicate writes after restart
- **WHEN** 5 events produced → connector stopped → 3 more events produced after restart
- **THEN** exactly 8 rows in `call_transcriptions` (5 + 3, no duplicates)

### Requirement: DualWriter Fallback

The e2e test suite SHALL verify that DualWriter in transcription-analyzer continues to write to PostgreSQL when Kafka Connect is unavailable.

The test suite SHALL:
- Stop Kafka Connect container
- Produce event to `transcription.enriched` topic
- Wait for DualWriter to process (up to 60 seconds)
- Verify event is written to `call_transcriptions` via DualWriter (not Kafka Connect)
- Restart Kafka Connect container
- Verify Kafka Connect resumes from offset (no duplicate writes)

#### Scenario: DualWriter works when Kafka Connect is down
- **WHEN** Kafka Connect container is stopped
- **THEN** DualWriter in transcription-analyzer still writes to PostgreSQL

#### Scenario: Kafka Connect resumes without duplicates after DualWriter
- **WHEN** Kafka Connect container is restarted after DualWriter was active
- **THEN** Kafka Connect resumes from offset, no duplicate writes to PostgreSQL
