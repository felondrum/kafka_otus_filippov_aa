## Purpose

Определяет требования к сервису отчётности и аналитики: Kafka Consumer для чтения событий из топиков платформы, REST API для получения отчётов, метаданных и агрегатов из PostgreSQL (заполняемого Kafka Connect), агрегации и кэширование для быстрых запросов.

## ADDED Requirements

### Requirement: Kafka Consumer Pipeline

The system SHALL consume events from three Kafka topics: calls.metadata, transcription.enriched, and calls.fraud-alerts.

#### Scenario: Consumer reads from calls.metadata
- **WHEN** the reporting-nps service starts
- **THEN** it creates a Kafka consumer group that reads from the calls.metadata topic

#### Scenario: Consumer reads from transcription.enriched
- **WHEN** the reporting-nps service starts
- **THEN** it creates a Kafka consumer group that reads from the transcription.enriched topic

#### Scenario: Consumer reads from calls.fraud-alerts
- **WHEN** the reporting-nps service starts
- **THEN** it creates a Kafka consumer group that reads from the calls.fraud-alerts topic

#### Scenario: Consumer commits offsets after processing
- **WHEN** a batch of events is processed successfully
- **THEN** Kafka consumer commits offsets to allow reprocessing on restart

### Requirement: CQRS Read Side — Read-only Access

The system SHALL read from PostgreSQL to provide analytics based on data populated by the Kafka Connect JDBC Sink.

#### Scenario: Metadata access
- **WHEN** the reporting-nps service queries the database
- **THEN** it reads from call_metadata and call_transcriptions tables populated by Kafka Connect

### Requirement: REST API — Daily Reports

The system SHALL provide daily aggregated reports via REST API.

#### Scenario: Daily report endpoint
- **WHEN** GET /api/reports/daily is called with optional date range
- **THEN** the system returns: total calls, average NPS, calls by status, calls by segment, calls by agent

#### Scenario: Daily report with date range filter
- **WHEN** GET /api/reports/daily?from=2024-01-01&to=2024-01-31 is called
- **THEN** the system returns aggregated data for the specified date range

### Requirement: REST API — Agent Reports

The system SHALL provide agent-specific statistics via REST API.

#### Scenario: Agent report endpoint
- **WHEN** GET /api/reports/agent/{agentId} is called
- **THEN** the system returns: total calls handled, average NPS, calls by status, calls by segment, fraud alerts associated with agent's calls

#### Scenario: Agent not found
- **WHEN** GET /api/reports/agent/{agentId} is called with non-existent agentId
- **THEN** the system returns HTTP 404 with agent not found message

### Requirement: REST API — Metadata Queries

The system SHALL provide call metadata queries via REST API.

#### Scenario: Call metadata by ID
- **WHEN** GET /api/metadata/{callId} is called
- **THEN** the system returns the call metadata record including status, segment, riskLevel, priority

#### Scenario: Call metadata not found
- **WHEN** GET /api/metadata/{callId} is called with non-existent callId
- **THEN** the system returns HTTP 404 with call not found message

#### Scenario: Calls by status (paginated)
- **WHEN** GET /api/metadata/status/{status} is called with optional pagination parameters (page, size)
- **THEN** the system returns a paginated list of calls matching the specified status with total count and page metadata

### Requirement: REST API — Sentiment Distribution

The system SHALL provide sentiment distribution via REST API.

#### Scenario: Sentiment distribution endpoint
- **WHEN** GET /api/sentiment/distribution is called with optional date range
- **THEN** the system returns: count of positive, neutral, negative sentiments with percentages

#### Scenario: Sentiment distribution with date range
- **WHEN** GET /api/sentiment/distribution?from=2024-01-01&to=2024-01-31 is called
- **THEN** the system returns sentiment distribution for the specified date range

### Requirement: Database Schema — Full Call View

The system SHALL provide a database view that joins call_metadata and call_transcriptions. The view is used by daily report and agent report endpoints for efficient aggregation across metadata and transcription data.

#### Scenario: Full call info view exists
- **WHEN** the database is initialized
- **THEN** the view v_full_call_info is created, joining call_metadata and call_transcriptions on call_id, including segment, risk_level, priority fields

#### Scenario: Full call info view returns complete data
- **WHEN** SELECT * FROM v_full_call_info is executed
- **THEN** it returns all metadata fields combined with transcription fields (including segment, risk_level, priority) for each call

### Requirement: Caching

The system SHALL cache frequent query results to reduce database load.

#### Scenario: Metadata query cached
- **WHEN** GET /api/metadata/{callId} is called
- **THEN** the result is cached for 5 minutes to reduce database queries

#### Scenario: Daily report cached
- **WHEN** GET /api/reports/daily is called
- **THEN** the result is cached for 1 minute to reduce database aggregation

#### Scenario: Cache invalidation on new events
- **WHEN** a Kafka event is processed that changes call state
- **THEN** the relevant cache entries are invalidated per the following mapping:
  - calls.metadata event → invalidate metadataCache(callId), reportCache (daily and agent reports for affected agent)
  - transcription.enriched event → invalidate metadataCache(callId), reportCache (daily and agent reports), sentimentCache
  - calls.fraud-alerts event → invalidate reportCache (daily and agent reports for affected agent)

### Requirement: Error Handling

The system SHALL handle processing errors gracefully without stopping the consumer pipeline.

#### Scenario: Malformed Avro event
- **WHEN** a Kafka event cannot be deserialized (malformed Avro, schema mismatch)
- **THEN** the system logs the error with the raw payload, sends the event to an internal DLQ, and continues processing subsequent events

#### Scenario: PostgreSQL write failure
- **WHEN** a PostgreSQL write fails during event processing
- **THEN** the system retries the write up to 3 times with exponential backoff (1s, 2s, 4s); if all retries fail, the event is sent to an internal DLQ and the consumer continues processing

#### Scenario: Processing timeout
- **WHEN** event processing exceeds the configured timeout (default 30 seconds)
- **THEN** the system logs a warning, commits the current offset, and retries the event on next consumption cycle

### Requirement: Data Consistency Monitoring

The system SHALL monitor for data inconsistencies caused by asynchronous processing timing between Kafka Connect and the reporting-nps consumer.

#### Scenario: Consumer lag monitoring
- **WHEN** reporting-nps service is running
- **THEN** it exposes `consumer_lag` metric for the subscribed topics, alerting if the lag exceeds 10 seconds to detect data staleness.

**Note on Temporal Consistency:** Due to the asynchronous nature of Kafka Connect (database writer) and reporting-nps (consumer), a transient period of data inconsistency (when transcription data is not yet in PG, but metadata event is processed) is expected. This is handled by upsert logic in the database and monitored via consumer lag.
