## 1. Project Setup

- [x] 1.1 Create Spring Boot 3 project structure (Java 21, Gradle Kotlin DSL) and verify gradlew is present
- [x] 1.2 Add dependencies: spring-boot-starter, spring-kafka (streams), avro, schema-registry-client
- [x] 1.3 Add test dependencies: junit-jupiter, mockito, embedded-kafka, testcontainers-kafka
- [x] 1.4 Create application.yml with Kafka Streams, RocksDB, and Spring Boot configuration
- [x] 1.5 Create Dockerfile for fraud-detector (multi-stage build, linux/arm64/v8) and verify Docker build succeeds

## 2. Domain Models and Avro Schemas

- [x] 2.1 Create FraudAlert Avro schema with fields: callId, phone, pattern, count, timestamp, severity
- [x] 2.2 Define pattern enum (FREQUENT_CALLS, NPS_ESCALATION, ANOMALOUS_DURATION)
- [x] 2.3 Define severity enum (HIGH, MEDIUM, LOW)
- [x] 2.4 Verify Avro schema compiles with avro-gradle-plugin (no runtime errors)
- [x] 2.5 Register FraudAlert schema in Schema Registry with BACKWARD compatibility mode
- [x] 2.6 Verify FraudAlert Avro schema is compatible with reporting-nps consumer expectations (cross-change check)

## 3. Schema Registry Integration

- [x] 3.1 Add dependencies: confluent-kafka-schema-registry or spring-kafka SchemaRegistrySerializer
- [x] 3.2 Configure Schema Registry URL from application.yml (point to infrastructure Schema Registry on 8085)
- [x] 3.3 Register FraudAlert Avro schema in Schema Registry on startup (idempotent: skip if already registered)
- [x] 3.4 Configure Kafka Streams producer to use Schema Registry for Avro serialization
- [x] 3.5 Verify fraud alerts are serialized with correct Avro schema via Schema Registry
- [x] 3.6 Integration test: produce fraud alert and verify Schema Registry has the schema

## 4. Kafka Streams Topology — DSL (Frequent Calls)

- [x] 4.1 Create Kafka Streams builder with calls.completed as input source
- [x] 4.2 Normalize phone numbers to E.164 format before any processing
- [x] 4.3 Filter out calls with duration < 5 seconds
- [x] 4.4 GroupBy phone number and apply hopping window (1 minute size, 10 second advance)
- [x] 4.5 Compute windowed count and detect count > 5
- [x] 4.6 Produce fraud alerts with pattern="FREQUENT_CALLS" to calls.fraud-alerts topic
- [x] 4.7 Verify hopping window logic with Embedded Kafka test (6 calls in 30 sec → alert; verify overlapping windows)

## 5. Kafka Streams Topology — Processor API (NPS Escalation)

- [x] 5.1 Create Processor that tracks NPS scores per phone number (normalized E.164)
- [x] 5.2 Implement counter: count negative NPS (NPS < 2) per phone within 24-hour window
- [x] 5.3 Store per-phone last activity timestamp for TTL tracking
- [x] 5.4 Detect threshold: 3 negative NPS in 24 hours → fraud alert
- [x] 5.5 Produce fraud alerts with pattern="NPS_ESCALATION" to calls.fraud-alerts topic
- [x] 5.6 Implement per-phone TTL cleanup: remove entries where last activity > 24 hours ago
- [x] 5.7 Verify NPS escalation logic with Embedded Kafka test (3x NPS=1 in 1 hour → alert)

## 6. Anomalous Duration Detection

- [x] 6.1 Add filter for calls with duration > 300 seconds
- [x] 6.2 Produce fraud alerts with pattern="ANOMALOUS_DURATION" for long calls
- [x] 6.3 Set severity=LOW for anomalous duration alerts
- [x] 6.4 Verify anomalous duration detection with Embedded Kafka test

## 7. State Store (RocksDB)

- [x] 7.1 Configure RocksDB state store with state.dir=/tmp/kafka-streams/fraud-detector
- [x] 7.2 Enable RocksDB compression for disk efficiency
- [x] 7.3 Configure cache.max.bytes.buffering=10MB for performance
- [x] 7.4 Implement per-phone TTL with timestamp tracking: store (count, lastActivityTimestamp) as value
- [x] 7.5 Verify state store persists to disk and recovers on restart
- [x] 7.6 Verify per-phone TTL cleanup removes stale entries after 24 hours of inactivity
- [x] 7.7 Verify periodic purge task cleans up expired entries (Kafka Streams cleanup.interval.ms)

## 8. Exactly-Once Semantics

- [x] 8.1 Configure Kafka Streams with processing.guarantee=exactly_once_v2
- [x] 8.2 Verify exactly-once semantics with test: duplicate calls.completed → single fraud alert
- [x] 8.3 Configure transactional.id for idempotent producer

## 9. Unit Tests

- [x] 9.1 Write unit tests for hopping window logic (frequent calls detection) and verify all tests pass
- [x] 9.2 Write unit tests for NPS escalation logic (Processor API) and verify coverage
- [x] 9.3 Write unit tests for short call filter (duration < 5 sec) and verify filtering
- [x] 9.4 Write unit tests for anomalous duration detection and verify alerts
- [x] 9.5 Write unit tests for state store TTL cleanup and verify expiration
- [x] 9.6 Achieve >70% code coverage and verify with ./gradlew testCoverage

## 10. Integration Tests

- [x] 10.1 Create integration test suite with Testcontainers (Kafka container)
- [x] 10.2 Write end-to-end test: 6 calls from same phone in 30 sec → fraud alert in calls.fraud-alerts
- [x] 10.3 Write test for NPS escalation: 3x NPS < 2 in 24 hours → fraud alert
- [x] 10.4 Write test for short call filter: duration < 5 sec → no fraud alert
- [x] 10.5 Write test for state store recovery: restart service → state restored
- [x] 10.6 Write test for exactly-once: duplicate event → single fraud alert
- [x] 10.7 Verify all integration tests pass with real Kafka container

## 11. Health Check and Monitoring

- [x] 11.1 Add spring-boot-starter-actuator dependency
- [x] 11.2 Configure /actuator/health endpoint to return 200 when Kafka Streams topology is running
- [x] 11.3 Add liveness and readiness probes for Docker/Kubernetes
- [x] 11.4 Verify health endpoint returns 503 when Kafka connection is lost

## 12. Docker and Deployment

- [x] 12.1 Create Dockerfile with multi-stage build (Gradle build → runtime JRE) and verify image size < 300MB
- [x] 12.2 Configure docker-compose service for fraud-detector with correct environment variables
- [x] 12.3 Verify service starts and connects to Kafka cluster in Docker network
- [x] 12.4 Verify fraud-detector consumes from calls.completed and produces to calls.fraud-alerts
- [x] 12.5 Verify fraud alerts are visible in Kafdrop after simulation
