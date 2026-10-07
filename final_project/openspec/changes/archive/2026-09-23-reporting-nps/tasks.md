## 1. Project Setup

- [x] 1.1 Create Spring Boot 3 project structure (Java 21, Gradle Kotlin DSL) and verify gradlew is present
- [x] 1.2 Add dependencies: spring-boot-starter-web, spring-kafka, spring-boot-starter-data-jpa, avro, schema-registry-client, postgresql-driver, caffeine (caching)
- [x] 1.3 Add test dependencies: junit-jupiter, mockito, embedded-kafka, testcontainers-kafka, testcontainers-postgresql
- [x] 1.4 Create application.yml with Kafka Consumer, PostgreSQL (JPA), Caffeine cache, and Spring Boot configuration
- [x] 1.5 Create Dockerfile for reporting-nps (multi-stage build, support --platform flag for cross-platform builds) and verify Docker build succeeds

## 2. Domain Models and JPA Entities

- [x] 2.1 Create CallMetadata JPA entity mapped to call_metadata table with all fields
- [x] 2.2 Create CallTranscription JPA entity mapped to call_transcriptions table with all fields: call_id (FK), transcription_text, sentiment, urgency, problem, solution, confidence, segment, risk_level, priority
- [x] 2.3 Create FraudStats JPA entity mapped to fraud_stats table with schema: phone (string), callId (string), pattern (string: FREQUENT_CALLS/NPS_ESCALATION/ANOMALOUS_DURATION), severity (string: HIGH/MEDIUM/LOW), count (integer), lastAlertAt (timestamp). This is a pre-aggregated table updated on each fraud alert event.
- [x] 2.4 Create JPA repositories: CallMetadataRepository, CallTranscriptionRepository, FraudStatsRepository
- [x] 2.5 Verify JPA entities map correctly to PostgreSQL tables

## 3. Kafka Consumer — Metadata

- [x] 3.1 Create KafkaConsumer configuration with consumer group "reporting-nps"
- [x] 3.2 Create MetadataConsumer listener that reads from calls.metadata topic
- [x] 3.3 Implement upsert logic: insert new or update existing call_metadata record
- [x] 3.4 Handle Avro deserialization for calls.metadata events
- [x] 3.5 Commit offsets after successful processing
- [x] 3.6 Write unit test with Embedded Kafka that verifies metadata consumption and upsert

## 4. Kafka Consumer — Enriched Transcription

- [x] 4.1 Create EnrichedTranscriptionConsumer listener that reads from transcription.enriched topic
- [x] 4.2 Implement upsert logic for call_transcriptions table
- [x] 4.3 Implement upsert logic for call_metadata table (segment, riskLevel, priority fields)
- [x] 4.4 Handle Avro deserialization for transcription.enriched events
- [x] 4.5 Handle FK integrity: if call_id not found in call_metadata, buffer the orphan event and retry after 5 seconds (max 3 retries); on final failure, send to internal DLQ
- [x] 4.6 Write unit test with Embedded Kafka that verifies enriched transcription consumption

## 5. Kafka Consumer — Fraud Alerts

- [x] 5.1 Create FraudAlertConsumer listener that reads from calls.fraud-alerts topic
- [x] 5.2 Implement fraud statistics aggregation: count by phone, pattern, severity (HIGH/MEDIUM/LOW). Store aggregated stats in fraud_stats table (pre-aggregated model).
- [x] 5.3 Store aggregated fraud stats in PostgreSQL
- [x] 5.4 Handle Avro deserialization for calls.fraud-alerts events
- [x] 5.5 Implement fraud alert correlation: extract callId from fraud alert body, lookup call_metadata to get agentId, associate fraud alert with agent
- [x] 5.6 Handle fraud alert for unknown callId: buffer and retry lookup up to 3 times with 5-second intervals; store with agentId=null if still not found
- [x] 5.7 Write unit test with Embedded Kafka that verifies fraud alert consumption, aggregation, and correlation

## 6. REST API — Daily Reports

- [x] 6.1 Create ReportController with GET /api/reports/daily endpoint
- [x] 6.2 Implement daily aggregation: total calls, average NPS, calls by status, calls by segment, calls by agent
- [x] 6.3 Support optional date range filter (from, to query parameters)
- [x] 6.4 Return HTTP 200 with aggregated JSON response
- [x] 6.5 Cache daily report results for 1 minute (Caffeine)
- [x] 6.6 Write unit tests for daily report endpoint and verify caching

## 7. REST API — Agent Reports

- [x] 7.1 Create ReportController with GET /api/reports/agent/{agentId} endpoint
- [x] 7.2 Implement agent aggregation: total calls, average NPS, calls by status, calls by segment, fraud alerts
- [x] 7.3 Return HTTP 404 when agent not found
- [x] 7.4 Cache agent report results for 1 minute (Caffeine)
- [x] 7.5 Write unit tests for agent report endpoint and verify 404 handling

## 8. REST API — Metadata Queries

- [x] 8.1 Create MetadataController with GET /api/metadata/{callId} endpoint
- [x] 8.2 Implement call metadata lookup by callId
- [x] 8.3 Return HTTP 404 when call not found
- [x] 8.4 Create MetadataController with GET /api/metadata/status/{status} endpoint
- [x] 8.5 Implement call lookup by status with pagination support (page, size query params, default page size=50, max page size=200)
- [x] 8.6 Cache metadata query results for 5 minutes (Caffeine)
- [x] 8.7 Write unit tests for metadata endpoints and verify 404 handling

## 9. REST API — Sentiment Distribution

- [x] 9.1 Create ReportController with GET /api/sentiment/distribution endpoint
- [x] 9.2 Implement sentiment aggregation: count of positive, neutral, negative with percentages
- [x] 9.3 Support optional date range filter (from, to query parameters)
- [x] 9.4 Return HTTP 200 with sentiment distribution JSON
- [x] 9.5 Cache sentiment distribution results for 1 minute (Caffeine)
- [x] 9.6 Write unit tests for sentiment distribution endpoint and verify percentages

## 10. Database Schema — Full Call View

- [x] 10.1 Create SQL view v_full_call_info joining call_metadata and call_transcriptions on call_id
- [x] 10.2 View returns all metadata fields combined with transcription fields including segment, risk_level, priority. Used by daily report and agent report endpoints for efficient aggregation.
- [x] 10.3 Create JPA @SqlResultSetMapping or native query for v_full_call_info
- [x] 10.4 Verify view works correctly with sample data
- [x] 10.5 Write integration test that verifies view returns complete data

## 11. Caching

- [x] 11.1 Configure Caffeine cache manager with two caches: metadataCache (5 min TTL), reportCache (1 min TTL)
- [x] 11.2 Add @Cacheable annotations to REST controller methods
- [x] 11.3 Implement cache invalidation mapping:
  - calls.metadata event → invalidate metadataCache(callId), reportCache (daily + agent reports for affected agent)
  - transcription.enriched event → invalidate metadataCache(callId), reportCache (daily + agent), sentimentCache
  - calls.fraud-alerts event → invalidate reportCache (daily + agent reports for affected agent)
- [x] 11.4 Add cache hit/miss metrics via Micrometer
- [x] 11.5 Write unit tests that verify caching behavior (cache hit, cache miss, cache invalidation)

## 12. Health Check

- [x] 12.1 Create GET /api/health endpoint that checks PostgreSQL connectivity
- [x] 12.2 Return HTTP 200 with status OK when PostgreSQL is reachable
- [x] 12.3 Return HTTP 503 with error details when PostgreSQL is unreachable
- [x] 12.4 Verify health check responds correctly when PostgreSQL is up and down
- [x] 12.5 Write unit test that verifies health check logic

## 13. Unit Tests

- [x] 13.1 Write unit tests for MetadataConsumer (upsert logic, Avro deserialization) and verify all tests pass
- [x] 13.2 Write unit tests for EnrichedTranscriptionConsumer (upsert logic, FK integrity, orphan event handling) and verify coverage
- [x] 13.3 Write unit tests for FraudAlertConsumer (aggregation logic, fraud correlation, unknown callId handling) and verify correctness
- [x] 13.4 Write unit tests for REST controllers (daily reports, agent reports, metadata, sentiment) and verify endpoints
- [x] 13.5 Write unit tests for caching (cache hit, miss, invalidation per event type mapping) and verify behavior
- [x] 13.7 Write unit tests for pagination (verify page/size params, verify default page size=50, verify max page size=200)
- [x] 13.6 Achieve >70% code coverage and verify with ./gradlew jacocoTestReport

## 14. Integration Tests

- [x] 14.1 Create integration test suite with Testcontainers (Kafka + PostgreSQL)
- [x] 14.2 Write end-to-end test: call event → metadata consumed → GET /api/metadata/{callId} returns data
- [x] 14.3 Write test for enriched transcription: enriched event → call_transcriptions updated → view v_full_call_info returns data
- [x] 14.4 Write test for fraud alerts: fraud alert → fraud stats updated → GET /api/reports/daily includes fraud data
- [x] 14.5 Write test for daily report aggregation: multiple calls → correct averages and counts
- [x] 14.6 Write test for sentiment distribution: calls with different sentiments → correct percentages
- [x] 14.7 Write test for caching: second request returns cached result (verify response time)
- [x] 14.8 Verify all integration tests pass with real Kafka and PostgreSQL containers

## 15. Docker and Deployment

- [x] 15.1 Create Dockerfile with multi-stage build (Gradle build → runtime JRE) and verify image size < 300MB
- [x] 15.2 Configure docker-compose service for reporting-nps with correct environment variables
- [x] 15.3 Verify service starts and connects to Kafka cluster in Docker network
- [x] 15.4 Verify service connects to PostgreSQL in Docker network
- [x] 15.5 Verify full pipeline: call event → all consumers → REST API returns complete data (including segment, risk_level, priority in transcription data)

## 16. Schema Registry Consumer Integration

- [x] 16.1 Configure Schema Registry client (url from application.yml, basic auth if configured)
- [x] 16.2 Configure AvroDeserializer for Kafka Consumer with Schema Registry URL and schema compatibility check
- [x] 16.3 Verify Avro schema compatibility for calls.metadata consumer (CallEvent + status schema)
- [x] 16.4 Verify Avro schema compatibility for transcription.enriched consumer (EnrichedTranscription schema)
- [x] 16.5 Verify Avro schema compatibility for calls.fraud-alerts consumer (FraudAlert schema)
- [x] 16.6 Integration test: verify consumer reads from all 3 topics using Schema Registry deserialization

## 17. Error Handling

- [x] 17.1 Implement malformed event handler: catch AvroDeserializationException, log raw payload, send to internal DLQ, continue processing
- [x] 17.2 Implement PostgreSQL retry logic: 3 attempts with exponential backoff (1s, 2s, 4s) on write failures
- [x] 17.3 Implement processing timeout handler: configurable timeout (default 30s), log warning on timeout, commit offset, retry on next cycle
- [x] 17.4 Write unit test for malformed event handling (verify DLQ send, verify consumer continues)
- [x] 17.5 Write unit test for PostgreSQL retry logic (verify 3 retries with backoff, verify DLQ on final failure)
- [x] 17.6 Write unit test for processing timeout (verify offset commit, verify retry behavior)
