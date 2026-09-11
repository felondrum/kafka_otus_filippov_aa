## 1. Project Setup

- [ ] 1.1 Create Spring Boot 3 project structure (Java 21, Gradle Kotlin DSL) and verify gradlew is present
- [ ] 1.2 Add dependencies: spring-boot-starter-web, spring-kafka, spring-boot-starter-data-jpa, avro, schema-registry-client, postgresql-driver, caffeine (caching)
- [ ] 1.3 Add test dependencies: junit-jupiter, mockito, embedded-kafka, testcontainers-kafka, testcontainers-postgresql
- [ ] 1.4 Create application.yml with Kafka Consumer, PostgreSQL (JPA), Caffeine cache, and Spring Boot configuration
- [ ] 1.5 Create Dockerfile for reporting-nps (multi-stage build, support --platform flag for cross-platform builds) and verify Docker build succeeds

## 2. Domain Models and JPA Entities

- [ ] 2.1 Create CallMetadata JPA entity mapped to call_metadata table with all fields
- [ ] 2.2 Create CallTranscription JPA entity mapped to call_transcriptions table with all fields: call_id (FK), transcription_text, sentiment, urgency, problem, solution, confidence, segment, risk_level, priority
- [ ] 2.3 Create FraudStats JPA entity mapped to fraud_stats table with schema: phone (string), callId (string), pattern (string: FREQUENT_CALLS/NPS_ESCALATION/ANOMALOUS_DURATION), severity (string: HIGH/MEDIUM/LOW), count (integer), lastAlertAt (timestamp). This is a pre-aggregated table updated on each fraud alert event.
- [ ] 2.4 Create JPA repositories: CallMetadataRepository, CallTranscriptionRepository, FraudStatsRepository
- [ ] 2.5 Verify JPA entities map correctly to PostgreSQL tables

## 3. Kafka Consumer — Metadata

- [ ] 3.1 Create KafkaConsumer configuration with consumer group "reporting-nps"
- [ ] 3.2 Create MetadataConsumer listener that reads from calls.metadata topic
- [ ] 3.3 Implement upsert logic: insert new or update existing call_metadata record
- [ ] 3.4 Handle Avro deserialization for calls.metadata events
- [ ] 3.5 Commit offsets after successful processing
- [ ] 3.6 Write unit test with Embedded Kafka that verifies metadata consumption and upsert

## 4. Kafka Consumer — Enriched Transcription

- [ ] 4.1 Create EnrichedTranscriptionConsumer listener that reads from transcription.enriched topic
- [ ] 4.2 Implement upsert logic for call_transcriptions table
- [ ] 4.3 Implement upsert logic for call_metadata table (segment, riskLevel, priority fields)
- [ ] 4.4 Handle Avro deserialization for transcription.enriched events
- [ ] 4.5 Handle FK integrity: if call_id not found in call_metadata, buffer the orphan event and retry after 5 seconds (max 3 retries); on final failure, send to internal DLQ
- [ ] 4.6 Write unit test with Embedded Kafka that verifies enriched transcription consumption

## 5. Kafka Consumer — Fraud Alerts

- [ ] 5.1 Create FraudAlertConsumer listener that reads from calls.fraud-alerts topic
- [ ] 5.2 Implement fraud statistics aggregation: count by phone, pattern, severity (HIGH/MEDIUM/LOW). Store aggregated stats in fraud_stats table (pre-aggregated model).
- [ ] 5.3 Store aggregated fraud stats in PostgreSQL
- [ ] 5.4 Handle Avro deserialization for calls.fraud-alerts events
- [ ] 5.5 Implement fraud alert correlation: extract callId from fraud alert body, lookup call_metadata to get agentId, associate fraud alert with agent
- [ ] 5.6 Handle fraud alert for unknown callId: buffer and retry lookup up to 3 times with 5-second intervals; store with agentId=null if still not found
- [ ] 5.7 Write unit test with Embedded Kafka that verifies fraud alert consumption, aggregation, and correlation

## 6. REST API — Daily Reports

- [ ] 6.1 Create ReportController with GET /api/reports/daily endpoint
- [ ] 6.2 Implement daily aggregation: total calls, average NPS, calls by status, calls by segment, calls by agent
- [ ] 6.3 Support optional date range filter (from, to query parameters)
- [ ] 6.4 Return HTTP 200 with aggregated JSON response
- [ ] 6.5 Cache daily report results for 1 minute (Caffeine)
- [ ] 6.6 Write unit tests for daily report endpoint and verify caching

## 7. REST API — Agent Reports

- [ ] 7.1 Create ReportController with GET /api/reports/agent/{agentId} endpoint
- [ ] 7.2 Implement agent aggregation: total calls, average NPS, calls by status, calls by segment, fraud alerts
- [ ] 7.3 Return HTTP 404 when agent not found
- [ ] 7.4 Cache agent report results for 1 minute (Caffeine)
- [ ] 7.5 Write unit tests for agent report endpoint and verify 404 handling

## 8. REST API — Metadata Queries

- [ ] 8.1 Create MetadataController with GET /api/metadata/{callId} endpoint
- [ ] 8.2 Implement call metadata lookup by callId
- [ ] 8.3 Return HTTP 404 when call not found
- [ ] 8.4 Create MetadataController with GET /api/metadata/status/{status} endpoint
- [ ] 8.5 Implement call lookup by status with pagination support (page, size query params, default page size=50, max page size=200)
- [ ] 8.6 Cache metadata query results for 5 minutes (Caffeine)
- [ ] 8.7 Write unit tests for metadata endpoints and verify 404 handling

## 9. REST API — Sentiment Distribution

- [ ] 9.1 Create ReportController with GET /api/sentiment/distribution endpoint
- [ ] 9.2 Implement sentiment aggregation: count of positive, neutral, negative with percentages
- [ ] 9.3 Support optional date range filter (from, to query parameters)
- [ ] 9.4 Return HTTP 200 with sentiment distribution JSON
- [ ] 9.5 Cache sentiment distribution results for 1 minute (Caffeine)
- [ ] 9.6 Write unit tests for sentiment distribution endpoint and verify percentages

## 10. Database Schema — Full Call View

- [ ] 10.1 Create SQL view v_full_call_info joining call_metadata and call_transcriptions on call_id
- [ ] 10.2 View returns all metadata fields combined with transcription fields including segment, risk_level, priority. Used by daily report and agent report endpoints for efficient aggregation.
- [ ] 10.3 Create JPA @SqlResultSetMapping or native query for v_full_call_info
- [ ] 10.4 Verify view works correctly with sample data
- [ ] 10.5 Write integration test that verifies view returns complete data

## 11. Caching

- [ ] 11.1 Configure Caffeine cache manager with two caches: metadataCache (5 min TTL), reportCache (1 min TTL)
- [ ] 11.2 Add @Cacheable annotations to REST controller methods
- [ ] 11.3 Implement cache invalidation mapping:
  - calls.metadata event → invalidate metadataCache(callId), reportCache (daily + agent reports for affected agent)
  - transcription.enriched event → invalidate metadataCache(callId), reportCache (daily + agent), sentimentCache
  - calls.fraud-alerts event → invalidate reportCache (daily + agent reports for affected agent)
- [ ] 11.4 Add cache hit/miss metrics via Micrometer
- [ ] 11.5 Write unit tests that verify caching behavior (cache hit, cache miss, cache invalidation)

## 12. Health Check

- [ ] 12.1 Create GET /api/health endpoint that checks PostgreSQL connectivity
- [ ] 12.2 Return HTTP 200 with status OK when PostgreSQL is reachable
- [ ] 12.3 Return HTTP 503 with error details when PostgreSQL is unreachable
- [ ] 12.4 Verify health check responds correctly when PostgreSQL is up and down
- [ ] 12.5 Write unit test that verifies health check logic

## 13. Unit Tests

- [ ] 13.1 Write unit tests for MetadataConsumer (upsert logic, Avro deserialization) and verify all tests pass
- [ ] 13.2 Write unit tests for EnrichedTranscriptionConsumer (upsert logic, FK integrity, orphan event handling) and verify coverage
- [ ] 13.3 Write unit tests for FraudAlertConsumer (aggregation logic, fraud correlation, unknown callId handling) and verify correctness
- [ ] 13.4 Write unit tests for REST controllers (daily reports, agent reports, metadata, sentiment) and verify endpoints
- [ ] 13.5 Write unit tests for caching (cache hit, miss, invalidation per event type mapping) and verify behavior
- [ ] 13.7 Write unit tests for pagination (verify page/size params, verify default page size=50, verify max page size=200)
- [ ] 13.6 Achieve >70% code coverage and verify with ./gradlew jacocoTestReport

## 14. Integration Tests

- [ ] 14.1 Create integration test suite with Testcontainers (Kafka + PostgreSQL)
- [ ] 14.2 Write end-to-end test: call event → metadata consumed → GET /api/metadata/{callId} returns data
- [ ] 14.3 Write test for enriched transcription: enriched event → call_transcriptions updated → view v_full_call_info returns data
- [ ] 14.4 Write test for fraud alerts: fraud alert → fraud stats updated → GET /api/reports/daily includes fraud data
- [ ] 14.5 Write test for daily report aggregation: multiple calls → correct averages and counts
- [ ] 14.6 Write test for sentiment distribution: calls with different sentiments → correct percentages
- [ ] 14.7 Write test for caching: second request returns cached result (verify response time)
- [ ] 14.8 Verify all integration tests pass with real Kafka and PostgreSQL containers

## 15. Docker and Deployment

- [ ] 15.1 Create Dockerfile with multi-stage build (Gradle build → runtime JRE) and verify image size < 300MB
- [ ] 15.2 Configure docker-compose service for reporting-nps with correct environment variables
- [ ] 15.3 Verify service starts and connects to Kafka cluster in Docker network
- [ ] 15.4 Verify service connects to PostgreSQL in Docker network
- [ ] 15.5 Verify full pipeline: call event → all consumers → REST API returns complete data (including segment, risk_level, priority in transcription data)

## 16. Schema Registry Consumer Integration

- [ ] 16.1 Configure Schema Registry client (url from application.yml, basic auth if configured)
- [ ] 16.2 Configure AvroDeserializer for Kafka Consumer with Schema Registry URL and schema compatibility check
- [ ] 16.3 Verify Avro schema compatibility for calls.metadata consumer (CallEvent + status schema)
- [ ] 16.4 Verify Avro schema compatibility for transcription.enriched consumer (EnrichedTranscription schema)
- [ ] 16.5 Verify Avro schema compatibility for calls.fraud-alerts consumer (FraudAlert schema)
- [ ] 16.6 Integration test: verify consumer reads from all 3 topics using Schema Registry deserialization

## 17. Error Handling

- [ ] 17.1 Implement malformed event handler: catch AvroDeserializationException, log raw payload, send to internal DLQ, continue processing
- [ ] 17.2 Implement PostgreSQL retry logic: 3 attempts with exponential backoff (1s, 2s, 4s) on write failures
- [ ] 17.3 Implement processing timeout handler: configurable timeout (default 30s), log warning on timeout, commit offset, retry on next cycle
- [ ] 17.4 Write unit test for malformed event handling (verify DLQ send, verify consumer continues)
- [ ] 17.5 Write unit test for PostgreSQL retry logic (verify 3 retries with backoff, verify DLQ on final failure)
- [ ] 17.6 Write unit test for processing timeout (verify offset commit, verify retry behavior)
