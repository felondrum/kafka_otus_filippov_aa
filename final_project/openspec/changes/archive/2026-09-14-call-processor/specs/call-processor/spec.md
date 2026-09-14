## Purpose

Определяет требования к REST API для приёма событий о завершённых звонках, Kafka Producer для надёжной доставки событий, Topic Manager для автоматической инициализации топиков и механизмам обработки ошибок (DLQ, retry).

## ADDED Requirements

### Requirement: REST API for Call Events

The system SHALL expose a REST API endpoint to accept events about completed calls.

#### Scenario: Successful call event submission
- **WHEN** a client sends POST /api/calls with valid JSON body (callId, phone, duration, agentId, npsScore)
- **THEN** the service returns HTTP 202 Accepted with a correlation ID

#### Scenario: Call event submission with missing required fields
- **WHEN** a client sends POST /api/calls with missing required fields (e.g., no agentId)
- **THEN** the service returns HTTP 400 Bad Request with a descriptive error message

#### Scenario: Call event submission with invalid phone format
- **WHEN** a client sends POST /api/calls with phone in invalid format (e.g., "abc")
- **THEN** the service returns HTTP 400 Bad Request with a phone format validation error

#### Scenario: Call event submission with invalid duration
- **WHEN** a client sends POST /api/calls with duration <= 0
- **THEN** the service returns HTTP 400 Bad Request with a duration validation error

### Requirement: Input Validation

The system MUST validate all incoming call event data before processing.

#### Scenario: Phone number format validation
- **WHEN** a client sends a call event with phone number
- **THEN** the system rejects phone numbers that do not match the E.164 format (+7XXXXXXXXXX)

#### Scenario: Agent ID validation
- **WHEN** a client sends a call event with agentId
- **THEN** the system rejects null or empty agentId values

#### Scenario: NPS score validation
- **WHEN** a client sends a call event with npsScore
- **THEN** the system rejects npsScore values outside the range 0-10 (0 = full dissatisfaction, 10 = full satisfaction; Russian call-center convention)

### Requirement: Kafka Producer with Idempotent Delivery

The system SHALL produce call events to Kafka with exactly-once semantics.

#### Scenario: Call event produced to calls.completed topic
- **WHEN** a call event passes validation
- **THEN** the event is produced to the `calls.completed` topic with callId as the key and Avro format

#### Scenario: Metadata event produced to calls.metadata topic
- **WHEN** a call event passes validation
- **THEN** the event is also produced to the `calls.metadata` topic with callId as the key, Avro format, and initial status PENDING

#### Scenario: Producer uses idempotent mode
- **WHEN** the producer sends events to Kafka
- **THEN** it uses enable.idempotence=true and acks=all to ensure exactly-once delivery

### Requirement: Schema Registry Integration

The system SHALL use Schema Registry for Avro schema validation and serialization.

#### Scenario: Event serialization uses registered Avro schema
- **WHEN** the producer sends a call event
- **THEN** it serializes the event using the latest compatible Avro schema from Schema Registry

#### Scenario: Schema validation on send
- **WHEN** the producer attempts to send an event that doesn't match the schema
- **THEN** the service rejects the event with an error and logs the failure

### Requirement: Topic Manager

The system SHALL create Kafka topics on startup if they do not exist.

#### Scenario: Topics created on first startup
- **WHEN** the service starts and topics do not exist
- **THEN** it creates the `calls.completed`, `calls.metadata`, and `calls.dlq` topics with replication factor 3 and 6 partitions

#### Scenario: Metadata topic uses compaction for lifecycle tracking
- **WHEN** downstream services (transcription-analyzer, fraud-detector) update call metadata
- **THEN** the `calls.metadata` topic uses compaction (cleanup.policy=compact) to retain only the latest status per callId

#### Scenario: Same Avro schema for calls.completed and calls.metadata
- **WHEN** the producer sends events to both `calls.completed` and `calls.metadata` topics
- **THEN** it uses the same CallEvent Avro schema (registered in Schema Registry); `calls.metadata` additionally includes a `status` field (PENDING/TRANSCRIBING/SUMMARIZING/COMPLETED) compatible with the base schema
- **THEN** downstream services (transcription-analyzer, fraud-detector) use the same CallEvent+status schema for `calls.metadata` events — no separate MetadataEvent schema required

#### Scenario: Topics not recreated if they exist
- **WHEN** the service starts and topics already exist
- **THEN** it skips topic creation without errors

### Requirement: Dead Letter Queue (DLQ)

The system SHALL send failed events to a Dead Letter Queue topic.

#### Scenario: Event sent to DLQ after max retries
- **WHEN** an event fails to produce after 3 retry attempts
- **THEN** the event is sent to the `calls.dlq` topic with callId as key and JSON format

#### Scenario: DLQ event includes error details
- **WHEN** an event is sent to DLQ
- **THEN** the DLQ message includes the original payload, error message, and timestamp

### Requirement: Retry with Exponential Backoff

The system SHALL retry failed Kafka produce operations with exponential backoff.

#### Scenario: First retry after 1 second
- **WHEN** a Kafka produce operation fails
- **THEN** the system retries after 1 second

#### Scenario: Second retry after 2 seconds
- **WHEN** the first retry fails
- **THEN** the system retries after 2 seconds

#### Scenario: Third retry after 4 seconds
- **WHEN** the second retry fails
- **THEN** the system retries after 4 seconds

#### Scenario: Event sent to DLQ after all retries exhausted
- **WHEN** all 3 retry attempts fail
- **THEN** the event is sent to the DLQ topic

### Requirement: Health Check

The system SHALL expose a health check endpoint for monitoring.

#### Scenario: Health check returns 200 when healthy
- **WHEN** the service is running and Kafka is reachable
- **THEN** GET /api/health returns HTTP 200 with status OK

#### Scenario: Health check returns 503 when Kafka is down
- **WHEN** the service is running but Kafka is unreachable
- **THEN** GET /api/health returns HTTP 503 with Kafka connection error details

### Requirement: Correlation ID

The system SHALL generate and return a correlation ID for each processed call event.

#### Scenario: Correlation ID generated for each request
- **WHEN** a call event is accepted
- **THEN** the response includes a unique correlation ID (UUID)

#### Scenario: Correlation ID logged with event
- **WHEN** the event is produced to Kafka
- **THEN** the correlation ID is included in the log entry for traceability
