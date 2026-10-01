## Purpose

Определяет требования к Kafka Streams pipeline для детекции мошеннических паттернов в звонках колл-центра: частые звонки с одного номера, эскалация негативного NPS, аномальная длительность.

## ADDED Requirements

### Requirement: Kafka Streams Pipeline

The system SHALL read call events from calls.completed topic and process them using Kafka Streams.

#### Scenario: Pipeline consumes from calls.completed
- **WHEN** the fraud-detector service starts
- **THEN** it creates a Kafka Streams topology that reads from the calls.completed topic

#### Scenario: Pipeline produces to calls.fraud-alerts
- **WHEN** a fraud pattern is detected
- **THEN** the pipeline produces a fraud alert to the calls.fraud-alerts topic with phone as the key and Avro format

### Requirement: Frequent Calls Detection (Hopping Window)

The system SHALL detect when more than 5 calls are received from the same phone number within a 1-minute hopping window (10-second advance).

#### Scenario: Frequent calls alert triggered
- **WHEN** 6 or more calls from the same phone number are received within 1 minute
- **THEN** the system produces a fraud alert with pattern="FREQUENT_CALLS" and count=6

#### Scenario: No alert within threshold
- **WHEN** 5 or fewer calls from the same phone number are received within 1 minute
- **THEN** no fraud alert is produced

#### Scenario: Window expiration
- **WHEN** a hopping window period (1 minute) elapses
- **THEN** the windowed count is finalized and a new window starts for subsequent calls
- **AND** a single call may contribute to multiple overlapping windows due to the 10-second advance interval

### Requirement: Phone Number Normalization

The system SHALL normalize phone numbers to E.164 format (+7XXXXXXXXXX) before any fraud detection processing.

#### Scenario: Phone number normalization
- **WHEN** a call event with a phone number in any valid format is received
- **THEN** the system normalizes the phone number to E.164 format (+7XXXXXXXXXX) before processing for fraud detection

#### Scenario: Normalized phone used for all detection
- **WHEN** fraud detection logic processes events
- **THEN** all grouping, counting, and state store operations use the normalized E.164 phone number as the key

### Requirement: NPS Escalation Detection (Processor API)

The system SHALL detect when a phone number has 3 or more negative NPS scores (NPS < 2) within a 24-hour period.

**Precondition:** Phone number must be normalized to E.164 format (see Phone Number Normalization requirement).

#### Scenario: NPS escalation alert triggered
- **WHEN** 3 calls with NPS < 2 are received from the same phone number within 24 hours
- **THEN** the system produces a fraud alert with pattern="NPS_ESCALATION" and count=3

#### Scenario: NPS escalation threshold not reached
- **WHEN** fewer than 3 calls with NPS < 2 are received from the same phone number within 24 hours
- **THEN** no fraud alert is produced

#### Scenario: NPS escalation window reset
- **WHEN** more than 24 hours pass since the most recent negative NPS for a phone number
- **THEN** the counter resets and subsequent negative NPS starts a new window

### Requirement: State Store (RocksDB)

The system SHALL use RocksDB as the local state store for Kafka Streams to maintain call counts and NPS history.

#### Scenario: State store persists on disk
- **WHEN** the fraud-detector service is running
- **THEN** the RocksDB state store is persisted to /tmp/kafka-streams/fraud-detector

#### Scenario: State store recovers on restart
- **WHEN** the fraud-detector service is restarted
- **THEN** the state store is restored from disk and processing continues without data loss

#### Scenario: State store TTL cleanup
- **WHEN** 24 hours pass since a phone number's last activity
- **THEN** the state store automatically removes the entry for that phone number

### Requirement: Short Call Filter

The system SHALL filter out calls with duration less than 5 seconds before processing.

#### Scenario: Short calls are filtered
- **WHEN** a call event with duration < 5 seconds is received
- **THEN** the event is not processed for fraud detection and no alert is produced

#### Scenario: Normal calls pass through
- **WHEN** a call event with duration >= 5 seconds is received
- **THEN** the event is processed for fraud detection

#### Scenario: Filtered calls produce no alerts
- **WHEN** a call event with duration < 5 seconds is received
- **THEN** no fraud alert of any pattern (FREQUENT_CALLS, NPS_ESCALATION, ANOMALOUS_DURATION) is produced

### Requirement: Fraud Alert Format

The system SHALL produce fraud alerts with a structured Avro schema containing all relevant information.

#### Scenario: Fraud alert contains required fields
- **WHEN** a fraud alert is produced
- **THEN** it includes: callId, phone, pattern (FREQUENT_CALLS, NPS_ESCALATION, ANOMALOUS_DURATION), count, timestamp, severity
- **AND** `count` semantics: for FREQUENT_CALLS = total calls in window; for NPS_ESCALATION = total negative NPS in 24h; for ANOMALOUS_DURATION = 1 (single-call pattern)

#### Scenario: Fraud alert severity levels
- **WHEN** a fraud alert is produced
- **THEN** it includes a severity level: HIGH for NPS_ESCALATION, MEDIUM for FREQUENT_CALLS, LOW for ANOMALOUS_DURATION

### Requirement: Anomalous Duration Detection

The system SHALL detect calls with abnormally long duration (> 300 seconds) as a potential fraud pattern.

#### Scenario: Anomalous duration alert triggered
- **WHEN** a call with duration > 300 seconds is received
- **THEN** the system produces a fraud alert with pattern="ANOMALOUS_DURATION"

#### Scenario: Normal duration passes through
- **WHEN** a call with duration <= 300 seconds is received
- **THEN** no anomalous duration alert is produced
