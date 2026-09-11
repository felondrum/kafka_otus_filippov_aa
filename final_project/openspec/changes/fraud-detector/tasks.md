## 1. Project Setup

- [ ] 1.1 Create Spring Boot 3 project structure (Java 21, Gradle Kotlin DSL) and verify gradlew is present
- [ ] 1.2 Add dependencies: spring-boot-starter, spring-kafka (streams), avro, schema-registry-client
- [ ] 1.3 Add test dependencies: junit-jupiter, mockito, embedded-kafka, testcontainers-kafka
- [ ] 1.4 Create application.yml with Kafka Streams, RocksDB, and Spring Boot configuration
- [ ] 1.5 Create Dockerfile for fraud-detector (multi-stage build, linux/arm64/v8) and verify Docker build succeeds

## 2. Domain Models and Avro Schemas

- [ ] 2.1 Create FraudAlert Avro schema with fields: callId, phone, pattern, count, timestamp, severity
- [ ] 2.2 Define pattern enum (FREQUENT_CALLS, NPS_ESCALATION, ANOMALOUS_DURATION)
- [ ] 2.3 Define severity enum (HIGH, MEDIUM, LOW)
- [ ] 2.4 Verify Avro schema compiles with avro-gradle-plugin (no runtime errors)
- [ ] 2.5 Register FraudAlert schema in Schema Registry with BACKWARD compatibility mode
- [ ] 2.6 Verify FraudAlert Avro schema is compatible with reporting-nps consumer expectations (cross-change check)

## 3. Schema Registry Integration

- [ ] 3.1 Add dependencies: confluent-kafka-schema-registry or spring-kafka SchemaRegistrySerializer
- [ ] 3.2 Configure Schema Registry URL from application.yml (point to infrastructure Schema Registry on 8085)
- [ ] 3.3 Register FraudAlert Avro schema in Schema Registry on startup (idempotent: skip if already registered)
- [ ] 3.4 Configure Kafka Streams producer to use Schema Registry for Avro serialization
- [ ] 3.5 Verify fraud alerts are serialized with correct Avro schema via Schema Registry
- [ ] 3.6 Integration test: produce fraud alert and verify Schema Registry has the schema

## 4. Kafka Streams Topology — DSL (Frequent Calls)

- [ ] 4.1 Create Kafka Streams builder with calls.completed as input source
- [ ] 4.2 Normalize phone numbers to E.164 format before any processing
- [ ] 4.3 Filter out calls with duration < 5 seconds
- [ ] 4.4 GroupBy phone number and apply hopping window (1 minute size, 10 second advance)
- [ ] 4.5 Compute windowed count and detect count > 5
- [ ] 4.6 Produce fraud alerts with pattern="FREQUENT_CALLS" to calls.fraud-alerts topic
- [ ] 4.7 Verify hopping window logic with Embedded Kafka test (6 calls in 30 sec → alert; verify overlapping windows)

## 5. Kafka Streams Topology — Processor API (NPS Escalation)

- [ ] 5.1 Create Processor that tracks NPS scores per phone number (normalized E.164)
- [ ] 5.2 Implement counter: count negative NPS (NPS < 2) per phone within 24-hour window
- [ ] 5.3 Store per-phone last activity timestamp for TTL tracking
- [ ] 5.4 Detect threshold: 3 negative NPS in 24 hours → fraud alert
- [ ] 5.5 Produce fraud alerts with pattern="NPS_ESCALATION" to calls.fraud-alerts topic
- [ ] 5.6 Implement per-phone TTL cleanup: remove entries where last activity > 24 hours ago
- [ ] 5.7 Verify NPS escalation logic with Embedded Kafka test (3x NPS=1 in 1 hour → alert)

## 6. Anomalous Duration Detection

- [ ] 6.1 Add filter for calls with duration > 300 seconds
- [ ] 6.2 Produce fraud alerts with pattern="ANOMALOUS_DURATION" for long calls
- [ ] 6.3 Set severity=LOW for anomalous duration alerts
- [ ] 6.4 Verify anomalous duration detection with Embedded Kafka test

## 7. State Store (RocksDB)

- [ ] 7.1 Configure RocksDB state store with state.dir=/tmp/kafka-streams/fraud-detector
- [ ] 7.2 Enable RocksDB compression for disk efficiency
- [ ] 7.3 Configure cache.max.bytes.buffering=10MB for performance
- [ ] 7.4 Implement per-phone TTL with timestamp tracking: store (count, lastActivityTimestamp) as value
- [ ] 7.5 Verify state store persists to disk and recovers on restart
- [ ] 7.6 Verify per-phone TTL cleanup removes stale entries after 24 hours of inactivity
- [ ] 7.7 Verify periodic purge task cleans up expired entries (Kafka Streams cleanup.interval.ms)

## 8. Exactly-Once Semantics

- [ ] 8.1 Configure Kafka Streams with processing.guarantee=exactly_once_v2
- [ ] 8.2 Verify exactly-once semantics with test: duplicate calls.completed → single fraud alert
- [ ] 8.3 Configure transactional.id for idempotent producer

## 9. Unit Tests

- [ ] 9.1 Write unit tests for hopping window logic (frequent calls detection) and verify all tests pass
- [ ] 9.2 Write unit tests for NPS escalation logic (Processor API) and verify coverage
- [ ] 9.3 Write unit tests for short call filter (duration < 5 sec) and verify filtering
- [ ] 9.4 Write unit tests for anomalous duration detection and verify alerts
- [ ] 9.5 Write unit tests for state store TTL cleanup and verify expiration
- [ ] 9.6 Achieve >70% code coverage and verify with ./gradlew testCoverage

## 10. Integration Tests

- [ ] 10.1 Create integration test suite with Testcontainers (Kafka container)
- [ ] 10.2 Write end-to-end test: 6 calls from same phone in 30 sec → fraud alert in calls.fraud-alerts
- [ ] 10.3 Write test for NPS escalation: 3x NPS < 2 in 24 hours → fraud alert
- [ ] 10.4 Write test for short call filter: duration < 5 sec → no fraud alert
- [ ] 10.5 Write test for state store recovery: restart service → state restored
- [ ] 10.6 Write test for exactly-once: duplicate event → single fraud alert
- [ ] 10.7 Verify all integration tests pass with real Kafka container

## 11. Health Check and Monitoring

- [ ] 11.1 Add spring-boot-starter-actuator dependency
- [ ] 11.2 Configure /actuator/health endpoint to return 200 when Kafka Streams topology is running
- [ ] 11.3 Add liveness and readiness probes for Docker/Kubernetes
- [ ] 11.4 Verify health endpoint returns 503 when Kafka connection is lost

## 12. Docker and Deployment

- [ ] 12.1 Create Dockerfile with multi-stage build (Gradle build → runtime JRE) and verify image size < 300MB
- [ ] 12.2 Configure docker-compose service for fraud-detector with correct environment variables
- [ ] 12.3 Verify service starts and connects to Kafka cluster in Docker network
- [ ] 12.4 Verify fraud-detector consumes from calls.completed and produces to calls.fraud-alerts
- [ ] 12.5 Verify fraud alerts are visible in Kafdrop after simulation
