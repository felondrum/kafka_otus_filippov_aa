## Purpose

Определяет требования к полному end-to-end pipeline в Docker: от POST /api/calls до GET /api/reports/daily, включая Kafka Connect JDBC Sink, reporting-nps consumers, и автоматическую инициализацию.

## ADDED Requirements

### Requirement: Full Docker Pipeline

The system SHALL support a complete end-to-end pipeline running in Docker:

```
POST /api/calls → calls.metadata → reporting-nps → PG call_metadata
POST /api/calls → calls.completed → ksqlDB
WebRTC → transcription.raw → transcription-analyzer → transcription.enriched
transcription.enriched → Kafka Connect JDBC Sink → PG call_transcriptions
transcription.analyzer → calls.fraud-alerts → reporting-nps → PG fraud_stats
GET /api/reports/daily → PG aggregation → JSON response
```

#### Scenario: Full pipeline from call to report
- **WHEN** a call is created via POST /api/calls
- **THEN** call_metadata is populated in PostgreSQL by reporting-nps consumer
- **AND** calls.completed event is produced to Kafka

#### Scenario: Enriched transcription flows through Kafka Connect
- **WHEN** transcription-analyzer produces transcription.enriched event
- **THEN** Kafka Connect JDBC Sink writes to call_transcriptions table
- **AND** reporting-nps consumer also writes to call_transcriptions (dual-write)

#### Scenario: Fraud alert flows to fraud_stats
- **WHEN** fraud-detector produces calls.fraud-alerts event
- **THEN** reporting-nps consumer writes aggregated stats to fraud_stats table

#### Scenario: Daily report returns complete data
- **WHEN** GET /api/reports/daily is called
- **THEN** response includes total_calls, average_nps, calls_by_status, calls_by_segment, calls_by_agent
- **AND** data is sourced from PostgreSQL tables populated by Kafka consumers

### Requirement: Database Schema Initialization

The system SHALL create reporting-nps tables and views on PostgreSQL startup:

- **fraud_stats** table: phone, call_id, pattern, severity, count, last_alert_at, agent_id
- **v_full_call_info** view: JOIN of call_metadata + call_transcriptions

#### Scenario: fraud_stats table exists
- **WHEN** PostgreSQL is initialized
- **THEN** fraud_stats table is created with correct schema and indexes

#### Scenario: v_full_call_info view exists
- **WHEN** PostgreSQL is initialized
- **THEN** v_full_call_info view is created joining call_metadata and call_transcriptions

### Requirement: Kafka Connect JDBC Sink Deployment

The system SHALL automatically deploy Kafka Connect JDBC Sink connector on startup:

- **Connector class**: `io.confluent.connect.jdbc.JdbcSinkConnector`
- **Topics**: `transcription.enriched`
- **Table**: `call_transcriptions`
- **PK mode**: `RecordKey` with `pk.fields=call_id`
- **Insert mode**: `upsert`
- **Tasks**: 6 (matches topic partitions)
- **Transformations**: ExtractField + ReplaceString

#### Scenario: JDBC Sink connector starts automatically
- **WHEN** docker compose up is executed
- **THEN** kafka-connect-init service deploys JDBC Sink connector via REST API
- **AND** connector begins reading from transcription.enriched topic

#### Scenario: Connector survives restart
- **WHEN** Kafka Connect container restarts
- **THEN** connector configuration is preserved
- **AND** connector resumes from last committed offset

### Requirement: Bootstrap and Verification

The system SHALL provide a bootstrap script that:

1. Waits for all services to be healthy
2. Sends test events through the full pipeline
3. Verifies REST API responses contain expected data
4. Reports PASS/FAIL for each step

#### Scenario: Bootstrap script validates full pipeline
- **WHEN** make e2e is executed
- **THEN** test call event flows through all consumers
- **AND** GET /api/reports/daily returns non-zero total_calls
- **AND** GET /api/metadata/{callId} returns the test call data
- **AND** GET /api/sentiment/distribution returns sentiment counts
