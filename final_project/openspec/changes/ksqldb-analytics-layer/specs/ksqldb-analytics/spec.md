## Purpose

Provides real-time SQL-based analytics over Kafka streams using ksqlDB, with persistent streams for call metrics aggregation by agentId and fraud alerts filtering — enabling ad-hoc analytics without querying reporting-nps directly.

## ADDED Requirements

### Requirement: ksqlDB Server Deployment

The system SHALL deploy a ksqlDB server with REST API for stream processing and ad-hoc queries.

The ksqlDB server SHALL:
- Run on port 8088 (HTTP REST API)
- Connect to Kafka cluster for consuming/producing events
- Connect to Schema Registry (port 8085) for Avro schema resolution
- Auto-start 2 persistent streams on bootstrap
- Run in embedded mode (no separate ksqlDB CLI container in production)

#### Scenario: ksqlDB server starts and connects
- **WHEN** ksqlDB server container starts
- **THEN** server connects to Kafka cluster and Schema Registry, REST API is available on port 8088

#### Scenario: Persistent streams auto-start
- **WHEN** ksqlDB server boots
- **THEN** streams `calls_completed_agg` and `fraud_alerts_filtered` are created and begin processing immediately

### Requirement: calls_completed_agg Stream

The system SHALL create a persistent ksqlDB stream that aggregates `calls.completed` events by `agentId` in real-time.

The stream SHALL:
- Consume from `calls.completed` Kafka topic (Avro-encoded, key=callId)
- Group by `agentId` field from event body (defined in call-processor spec, CallEvent schema)
- Compute per-agent metrics using tumbling 5-minute windows with 1-minute grace period:
  - `call_count`: COUNT of completed calls
  - `avg_nps_score`: AVG of `npsScore` field
  - `last_call_at`: MAX of `completedAt` timestamp
- Emit results to `calls.completed.agg` output topic (key=agentId, Avro)
- Run persistently (survives ksqlDB restarts via ksqlDB persistent queries)

#### Scenario: Stream aggregates calls by agent
- **WHEN** 3 calls complete for agentId="agent-001" within 5-minute window
- **THEN** `calls.completed.agg` topic receives updated record: agentId="agent-001", call_count=3, avg_nps_score=<computed>, last_call_at=<timestamp>

#### Scenario: Window expiration
- **WHEN** 5-minute tumbling window expires + 1-minute grace period
- **THEN** final aggregation is emitted to `calls.completed.agg` and window state is cleared

#### Scenario: Stream survives restart
- **WHEN** ksqlDB server restarts
- **THEN** `calls_completed_agg` persistent query resumes from last committed offset in `calls.completed` topic

#### Scenario: Late event after grace period
- **WHEN** event arrives >1 minute after tumbling window close
- **THEN** event is dropped (not included in aggregation, not emitted to output topic)

#### Scenario: Stream creation idempotency
- **WHEN** output topic already exists (rollback + redeploy)
- **THEN** ksqlDB DROPs and recreates the stream, OR returns success if schema matches (depends on ksqlDB version and configuration)

### Requirement: fraud_alerts_filtered Stream

The system SHALL create a persistent ksqlDB stream that filters and aggregates `calls.fraud-alerts` events by severity and pattern.

The stream SHALL:
- Consume from `calls.fraud-alerts` Kafka topic (Avro-encoded, key=phone)
- Filter: only process events where `severity` = 'HIGH' or `severity` = 'MEDIUM' (DROP events with severity='LOW')
- Group by `phone`, `pattern`, `severity` from event body (note: `calls.fraud-alerts` has key=phone, no agentId field — aggregation is by phone, not agentId)
- Compute per-group metrics using hopping 10-minute window with 1-minute step:
  - `alert_count`: COUNT of fraud alerts
- Emit results to `calls.fraud-alerts.agg` output topic (key=phone:pattern:severity, Avro)
- Run persistently (survives ksqlDB restarts via ksqlDB persistent queries)

**Output topic schema for `calls.fraud-alerts.agg`:**
| Field | Type | Description |
|-------|------|-------------|
| `phone` | STRING | Phone number (from fraud-alerts key/value) |
| `pattern` | STRING | Fraud pattern (FREQUENT_CALLS, NPS_ESCALATION, ANOMALOUS_DURATION) |
| `severity` | STRING | Alert severity (HIGH, MEDIUM) |
| `alert_count` | BIGINT | Count of alerts in window |

**Key schema for `calls.fraud-alerts.agg`:**
| Field | Type | Description |
|-------|------|-------------|
| `phone` | STRING | Phone number |
| `pattern` | STRING | Fraud pattern |
| `severity` | STRING | Alert severity |

**Rationale for phone-based aggregation:** `calls.fraud-alerts` topic has key=phone and value fields {phone, pattern, severity, count}. There is no `agentId` field in fraud-alerts events. Aggregation by phone provides per-phone fraud visibility.

#### Scenario: Stream filters LOW severity
- **WHEN** fraud alert with severity='LOW' arrives
- **THEN** event is dropped — NOT included in aggregation, NOT emitted to output topic

#### Scenario: Stream aggregates HIGH severity alerts
- **WHEN** 5 fraud alerts with severity='HIGH' arrive for phone="+79001234567", pattern="FREQUENT_CALLS" within 10-minute window
- **THEN** `calls.fraud-alerts.agg` topic receives updated record: phone="+79001234567", pattern="FREQUENT_CALLS", severity="HIGH", alert_count=5

#### Scenario: Stream aggregates MEDIUM severity alerts
- **WHEN** fraud alert with severity='MEDIUM' arrives
- **THEN** event is processed and included in aggregation

#### Scenario: Hopping window step
- **WHEN** 1-minute hopping window step elapses
- **THEN** partial aggregation is emitted to `calls.fraud-alerts.agg` (window does not wait for full 10 minutes)

### Requirement: Output Topic Schemas

The system SHALL define explicit Avro schemas for ksqlDB output topics.

**Output topic schema for `calls.completed.agg`:**
| Field | Type | Description |
|-------|------|-------------|
| `agentId` | STRING | Agent identifier (from calls.completed event) |
| `call_count` | BIGINT | Count of completed calls in window |
| `avg_nps_score` | DOUBLE | Average NPS score in window (precision as computed by ksqlDB, default DOUBLE) |
| `last_call_at` | LONG | Timestamp of last completed call |

#### Scenario: Output topic schema matches stream definition
- **WHEN** ksqlDB creates output topics for persistent streams
- **THEN** `calls.completed.agg` has schema: agentId (STRING), call_count (BIGINT), avg_nps_score (DOUBLE), last_call_at (LONG)
- **THEN** `calls.fraud-alerts.agg` has schema: phone (STRING), pattern (STRING), severity (STRING), alert_count (BIGINT)

### Requirement: Ad-hoc Query via REST API

The system SHALL expose ksqlDB REST API (port 8088) for ad-hoc SQL queries over the streams.

The REST API SHALL:
- Accept POST requests to `/ksql` with SQL query in request body
- Return query results as JSON array
- Support SELECT queries on `calls_completed_agg` and `fraud_alerts_filtered` streams
- NOT support INSERT/UPDATE/DELETE (ksqlDB streams are append-only)
- Timeout long-running queries after 30 seconds

#### Scenario: Ad-hoc query for agent metrics
- **WHEN** POST request with `SELECT * FROM calls_completed_agg WHERE agentId = 'agent-001'` is sent to `/ksql`
- **THEN** response returns JSON array with current aggregation for agent-001

#### Scenario: Ad-hoc query for fraud alerts
- **WHEN** POST request with `SELECT * FROM fraud_alerts_filtered WHERE severity = 'HIGH'` is sent to `/ksql`
- **THEN** response returns JSON array with all HIGH severity fraud aggregations

#### Scenario: Query timeout
- **WHEN** ad-hoc query runs longer than 30 seconds
- **THEN** ksqlDB returns HTTP 408 Request Timeout with error message
