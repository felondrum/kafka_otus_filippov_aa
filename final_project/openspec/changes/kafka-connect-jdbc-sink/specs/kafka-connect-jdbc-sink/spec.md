## Purpose

Provides automatic, reliable synchronization of Kafka topic data to PostgreSQL via Kafka Connect JDBC Sink connector, with field transformations, offset management, and built-in retry — serving as the primary writer for transcription data to PostgreSQL.

## ADDED Requirements

### Requirement: Kafka Connect JDBC Sink Connector

The system SHALL deploy a Kafka Connect JDBC Sink connector that automatically writes events from the `transcription.enriched` Kafka topic to the PostgreSQL `call_transcriptions` table.

The connector SHALL:
- Read Avro-encoded events from `transcription.enriched` topic (key=callId)
- Transform fields using Kafka Connect transformations: `ExtractField`, `ReplaceString`
- Write to PostgreSQL `call_transcriptions` table with columns: `call_id`, `transcription_text`, `sentiment`, `urgency`, `problem`, `solution`, `confidence`
- Manage offsets automatically via Kafka Connect offset storage
- Retry failed writes with exponential backoff (max 3 retries)
- Send failed events to a dead-letter queue (DLQ) topic after max retries exhausted

#### Scenario: Connector starts and reads from topic
- **WHEN** Kafka Connect JDBC Sink connector is deployed and configured
- **THEN** connector reads events from `transcription.enriched` topic and writes to PostgreSQL `call_transcriptions` table

... (rest of the file remains unchanged, just remove the DualWriter fallback scenario)

### Requirement: Database User and Permissions

The system SHALL create a dedicated `kafka-connect-user` database user with minimal required privileges.

The `kafka-connect-user` SHALL:
- Have INSERT privilege on `call_transcriptions` table only
- Have SELECT privilege on `call_transcriptions` for verification queries
- NOT have privileges on any other tables (call_metadata, customers, etc.)

#### Scenario: Minimal privileges enforced
- **WHEN** kafka-connect-user connects to PostgreSQL
- **THEN** user can INSERT and SELECT only on `call_transcriptions` table

### Requirement: Connector Configuration via REST API

The system SHALL configure the JDBC Sink connector via Kafka Connect REST API (port 8083).

The connector configuration SHALL include:
- `connector.class`: `io.confluent.connect.jdbc.JdbcSinkConnector`
- `tasks.max`: `6` (matches 6 partitions of `transcription.enriched` topic, defined in infrastructure change)
- `connection.url`: JDBC URL to PostgreSQL (local development, SSL optional)
- `topics`: `transcription.enriched`
- `table.name.format`: `call_transcriptions`
- `pk.mode`: `RecordKey` (primary key from Kafka record key = callId)
- `pk.fields`: `call_id`
- `auto.create`: `false` (table must exist, created by infrastructure change init-db.sql)
- `insert.mode`: `upsert` (update existing records, insert new ones)
- `transforms`: `extract,replace`
- `transforms.extract.type`: `org.apache.kafka.connect.transforms.ExtractField$Value`
- `transforms.extract.field`: `value`
- `transforms.replace.type`: `org.apache.kafka.connect.transforms.ReplaceString$Value`

**Notes:**
- `pk.mode=RecordKey` — primary key is extracted from Kafka record key (callId), not from record value. This is the correct mode when the Kafka key matches the desired PK field.
- Confluent JDBC Connector automatically handles Avro-to-SQL type conversion via Schema Registry — no explicit `ConvertFieldSchema` SMT needed.
- `ReplaceString` is applied after `ExtractField`, sanitizing only the extracted text fields.

#### Scenario: Connector deployed via REST API
- **WHEN** POST request is sent to Kafka Connect REST API `/connectors/kafka-connect-jdbc-sink`
- **THEN** connector starts and begins consuming from `transcription.enriched`

#### Scenario: Connector persists configuration
- **WHEN** Kafka Connect container restarts
- **THEN** connector configuration is preserved and connector resumes from last committed offset
