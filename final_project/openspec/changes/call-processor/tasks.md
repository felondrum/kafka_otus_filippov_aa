## 1. Project Setup

- [ ] 1.1 Create Spring Boot 3 project structure (Java 21, Gradle Kotlin DSL) and verify gradlew is present
- [ ] 1.2 Add dependencies: spring-boot-starter-web, spring-kafka, spring-boot-starter-validation, avro, schema-registry-client
- [ ] 1.3 Add test dependencies: junit-jupiter, mockito, embedded-kafka, testcontainers-kafka, testcontainers-postgresql
- [ ] 1.4 Create application.yml with Kafka, Schema Registry, and Spring Boot configuration
- [ ] 1.5 Create initial Dockerfile (multi-stage build, linux/arm64/v8) and verify Docker build succeeds

## 2. Domain Models and DTOs

- [ ] 2.1 Create CallEvent DTO with fields: callId, phone, duration, agentId, npsScore and verify Jackson serialization
- [ ] 2.2 Create CallEventRequest DTO with validation annotations (@NotBlank, @Min, @Pattern) and verify validation errors return 400
- [ ] 2.3 Create ErrorResponse DTO for consistent error responses and verify it serializes correctly
- [ ] 2.4 Create Avro schemas for CallEvent (calls.completed, calls.metadata)

## 3. REST API — Call Event Endpoint

- [ ] 3.1 Create CallController with POST /api/calls endpoint and verify it accepts JSON requests
- [ ] 3.2 Implement input validation (phone E.164 format, duration > 0, agentId not null, npsScore 0-10) and verify 400 on invalid input
- [ ] 3.3 Generate correlation ID (UUID) for each request and include it in the 202 response
- [ ] 3.4 Return HTTP 202 Accepted with correlation ID on valid input and verify response format
- [ ] 3.5 Handle validation errors and return HTTP 400 with descriptive error messages

## 4. Kafka Producer Integration

- [ ] 4.1 Configure KafkaProducerTemplate with enable.idempotence=true, acks=all, retries=3 and verify producer properties
- [ ] 4.2 Implement method to produce CallEvent to calls.completed topic with callId as key and Avro serialization
- [ ] 4.3 Implement method to produce CallEvent to calls.metadata topic with initial status PENDING and Avro serialization
- [ ] 4.4 Verify events are correctly serialized to Avro and registered with Schema Registry
- [ ] 4.5 Write unit test with Embedded Kafka that verifies events are produced to correct topics
- [ ] 4.6 Configure producer throughput properties (batch.size=16384, linger.ms=5, compression.type=lz4) and verify >1000 events/sec throughput

## 5. Schema Registry Integration

- [ ] 5.1 Configure SchemaRegistryClient with URL from application.yml and verify connection to Schema Registry service
- [ ] 5.2 Register CallEvent Avro schema with BACKWARD compatibility mode
- [ ] 5.3 Verify schema registration on startup and error handling if Schema Registry is unavailable
- [ ] 5.4 Write integration test with Testcontainers that verifies Avro serialization with real Schema Registry

## 6. Topic Manager

- [ ] 6.1 Create TopicManager component that uses Kafka Admin API to create topics
- [ ] 6.2 Implement startup initialization: create calls.completed, calls.metadata, calls.dlq topics if not exist
- [ ] 6.3 Configure topics with replication factor 3, 6 partitions, and correct properties (compaction for calls.metadata)
- [ ] 6.4 Verify topics are created on first startup and skipped on subsequent startups
- [ ] 6.5 Write unit test that verifies topic creation logic with mocked Admin API

## 7. Dead Letter Queue (DLQ)

- [ ] 7.1 Create DLQ producer that sends failed events to calls.dlq topic with callId as key and JSON format
- [ ] 7.2 Include error details in DLQ message: original payload, error message, timestamp, correlation ID
- [ ] 7.3 Verify DLQ messages are correctly serialized as JSON (not Avro) for maximum compatibility
- [ ] 7.4 Write unit test that verifies DLQ message format and content

## 8. Retry with Exponential Backoff

- [ ] 8.1 Implement retry mechanism with 3 attempts and exponential backoff (1s, 2s, 4s)
- [ ] 8.2 Use Spring Retry (@Retryable) or manual retry with BackOff implementation
- [ ] 8.3 Log retry attempts with correlation ID for traceability
- [ ] 8.4 Verify that after 3 failed attempts, the event is sent to DLQ
- [ ] 8.5 Write unit test that verifies retry behavior with mocked Kafka producer failures

## 9. Health Check

- [ ] 9.1 Create GET /api/health endpoint that checks Kafka connectivity
- [ ] 9.2 Return HTTP 200 with status OK when Kafka is reachable
- [ ] 9.3 Return HTTP 503 with error details when Kafka is unreachable
- [ ] 9.4 Verify health check responds correctly when Kafka is up and down
- [ ] 9.5 Write unit test that verifies health check logic

## 10. Logging and Monitoring

- [ ] 10.1 Configure structured JSON logging (logback-spring.xml) with correlation ID in all log entries
- [ ] 10.2 Add Micrometer metrics: call_events_received, call_events_produced, call_events_failed, call_events_dlq
- [ ] 10.3 Add Micrometer tags: callId, status (success, failed, dlq)
- [ ] 10.4 Verify Prometheus endpoint (/actuator/prometheus) exposes custom metrics
- [ ] 10.5 Write unit test that verifies metrics are recorded correctly

## 11. Unit Tests

- [ ] 11.1 Write unit tests for CallController (valid/invalid requests, response format) and verify all tests pass
- [ ] 11.2 Write unit tests for input validation (phone format, duration, agentId, npsScore) and verify coverage
- [ ] 11.3 Write unit tests for KafkaProducer with Embedded Kafka and verify events are produced correctly
- [ ] 11.4 Write unit tests for TopicManager with mocked Admin API and verify topic creation logic
- [ ] 11.5 Write unit tests for DLQ and retry logic and verify error handling
- [ ] 11.6 Achieve >70% code coverage and verify with ./gradlew testCoverage

## 12. Integration Tests

- [ ] 12.1 Create integration test suite with Testcontainers (Embedded Kafka container)
- [ ] 12.2 Write end-to-end test: POST /api/calls → event appears in calls.completed topic
- [ ] 12.3 Write test for validation errors: POST /api/calls with invalid data → HTTP 400
- [ ] 12.4 Write test for DLQ: simulate Kafka failure → event appears in calls.dlq topic
- [ ] 12.5 Write test for health check: service healthy when Kafka up, unhealthy when Kafka down
- [ ] 12.6 Verify all integration tests pass with real Kafka container

## 13. Docker and Deployment

- [ ] 13.1 Optimize Docker image (verify multi-stage build finalized, image size < 300MB, linux/arm64/v8)
- [ ] 13.2 Configure docker-compose service for call-processor with correct environment variables
- [ ] 13.3 Verify service starts and connects to Kafka cluster in Docker network
- [ ] 13.4 Verify service creates topics on first startup
- [ ] 13.5 Verify service health check works in Docker environment
