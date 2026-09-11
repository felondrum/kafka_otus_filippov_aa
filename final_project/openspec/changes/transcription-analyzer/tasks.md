## 1. Project Setup

- [ ] 1.1 Create Spring Boot 3 project structure (Java 21, Gradle Kotlin DSL) and verify gradlew is present
- [ ] 1.2 Add dependencies: spring-boot-starter, spring-kafka, spring-boot-starter-jdbc, avro, schema-registry-client, postgresql-driver
- [ ] 1.3 Add test dependencies: junit-jupiter, mockito, embedded-kafka, testcontainers-kafka, testcontainers-postgresql
- [ ] 1.4 Create application.yml with Kafka (consumer groups: summary-processor, enrichment-processor), PostgreSQL (JDBC), keyword dictionary (transcription.keywords list), and Spring Boot configuration
- [ ] 1.5 Create Dockerfile for transcription-analyzer (multi-stage build, linux/arm64/v8) and verify Docker build succeeds

## 2. Domain Models and Avro Schemas

- [ ] 2.1 Create RawTranscription Avro schema with fields: callId, text, quality, language
- [ ] 2.2 Create TranscriptionSummary Avro schema with fields: callId, problem, solution, sentiment, urgency, confidence
- [ ] 2.3 Create EnrichedTranscription Avro schema with fields: callId, transcriptionText, sentiment, urgency, problem, solution, confidence, segment, riskLevel, priority
- [ ] 2.4 Create CustomerProfile Avro schema with fields: phone (key), segment (enum: premium/standard/corporate), riskLevel (enum: low/medium/high)
- [ ] 2.5 Verify all Avro schemas are compatible with Schema Registry (BACKWARD mode)

## 3. Schema Registry Integration

- [ ] 3.1 Configure SchemaRegistryClient with URL from application.yml and verify connection to Schema Registry service on port 8085
- [ ] 3.2 Register RawTranscription Avro schema in Schema Registry with BACKWARD compatibility mode
- [ ] 3.3 Register TranscriptionSummary Avro schema in Schema Registry with BACKWARD compatibility mode
- [ ] 3.4 Register EnrichedTranscription Avro schema in Schema Registry with BACKWARD compatibility mode
- [ ] 3.5 Configure Kafka producers to use Schema Registry for Avro serialization
- [ ] 3.6 Integration test: produce raw transcription and verify schema in Schema Registry

## 4. Synthetic Transcription Generation (Producer)

- [ ] 4.1 Create TranscriptionProducer component that generates synthetic (fake) transcription text
- [ ] 4.2 Implement text generation: ~150 words per minute of call duration (placeholder text, no real audio)
- [ ] 4.3 Generate quality (0.0-1.0) and language (ru/en) parameters
- [ ] 4.4 Produce raw transcription to transcription.raw topic with callId as key and Avro format
- [ ] 4.5 Verify transcription text length correlates with call duration
- [ ] 4.6 Write unit test with Embedded Kafka that verifies raw transcription production

## 5. Synthetic Summary Generation (Keyword-based, No LLM)

- [ ] 5.1 Create SummaryGenerator component that generates synthetic (fake) summary via keyword matching (no real LLM)
- [ ] 5.2 Create Kafka consumer that reads from transcription.raw topic
- [ ] 5.3 Implement keyword extraction: card, loan, fraud, complaint, transfer, block, limit, payment, balance
- [ ] 5.4 Make keyword dictionary configurable via application.yml (list of keywords)
- [ ] 5.5 Implement keyword matching with case-insensitive search
- [ ] 5.6 Implement problem categorization (card_loss, credit_inquiry, complaint, fraud_suspected, other) based on keywords
- [ ] 5.7 Implement solution categorization (card_blocked, info_provided, escalation_created, transfer_to_specialist, resolved_on_call)
- [ ] 5.8 Implement sentiment analysis (positive, neutral, negative) based on keyword matching
- [ ] 5.9 Implement urgency detection (low, medium, high, critical) based on keywords like fraud, complaint, escalation
- [ ] 5.10 Implement confidence score generation: (matched_keywords / total_keywords_in_dictionary) × 0.5 + (1 if exact category match else 0) × 0.5, clamped to [0.0, 1.0]
- [ ] 5.11 Implement default values when no keywords match: problem=other, solution=resolved_on_call, sentiment=neutral, urgency=low, confidence=0.0
- [ ] 5.12 Produce summary to transcription.summary topic with callId as key and Avro format
- [ ] 5.13 Write unit tests for each categorization function and verify correctness

## 6. Customer Profile Broadcast Enrichment

- [ ] 6.1 Create Kafka consumer that reads customers.profile topic (compacted, key=phone)
- [ ] 6.2 Maintain in-memory map: phone → {segment, riskLevel}
- [ ] 6.3 Implement broadcast enrichment: for each summary event, lookup phone in customers.profile map
- [ ] 6.4 Calculate priority based on segment + riskLevel per the priority matrix:
  - premium + low → high, premium + medium → high, premium + high → critical
  - standard + low → normal, standard + medium → normal, standard + high → high
  - corporate + low → normal, corporate + medium → high, corporate + high → critical
- [ ] 6.5 Handle missing customer profile gracefully (default: segment=standard, riskLevel=low, priority=normal)
- [ ] 6.6 Write unit test that verifies broadcast enrichment with mock customers.profile data
- [ ] 6.7 Write unit test that verifies missing customer profile returns default values

## 7. Dual-Write Strategy

- [ ] 7.1 Create DualWriter component that writes to both Kafka and PostgreSQL
- [ ] 7.2 Implement Kafka producer for transcription.enriched topic (Avro, callId key)
- [ ] 7.3 Implement JDBC writer for PostgreSQL call_transcriptions table
- [ ] 7.4 Implement retry logic for JDBC write failures (3 attempts, exponential backoff)
- [ ] 7.5 Implement error handling: if JDBC fails after retries, log error and send to internal DLQ
- [ ] 7.6 Verify dual-write produces event to transcription.enriched
- [ ] 7.7 Verify dual-write inserts record into call_transcriptions table
- [ ] 7.8 Write integration test with Testcontainers that verifies both writes succeed
- [ ] 7.9 Write integration test: PostgreSQL down → Kafka write succeeds, error logged

## 8. Metadata Lifecycle Management

- [ ] 8.1 Create MetadataManager component that manages call status transitions
- [ ] 8.2 Implement status: TRANSCRIBING (on raw transcription produced)
- [ ] 8.3 Implement status: SUMMARIZING (on synthetic summary generation started)
- [ ] 8.4 Implement status: COMPLETED (on dual-write complete)
- [ ] 8.5 Produce metadata events to calls.metadata topic with callId as key and compaction
- [ ] 8.6 Note: PENDING status is set by call-processor (see call-processor spec), not by transcription-analyzer
- [ ] 8.7 Verify status transitions occur in correct order: TRANSCRIBING → SUMMARIZING → COMPLETED
- [ ] 8.8 Write unit test that verifies status transition sequence

## 9. Unit Tests

- [ ] 9.1 Write unit tests for TranscriptionProducer (text generation, quality, language) and verify all tests pass
- [ ] 9.2 Write unit tests for SummaryGenerator (problem, solution, sentiment, urgency, confidence extraction via keyword matching) and verify coverage
- [ ] 9.3 Write unit tests for keyword extraction and verify keyword matching logic
- [ ] 9.4 Write unit tests for MetadataManager (status transitions) and verify sequence
- [ ] 9.5 Write unit tests for DualWriter (Kafka + JDBC) and verify both writes
- [ ] 9.6 Achieve >70% code coverage and verify with ./gradlew testCoverage

## 10. Integration Tests

- [ ] 10.1 Create integration test suite with Testcontainers (Kafka + PostgreSQL)
- [ ] 10.2 Write end-to-end test: call event → raw transcription → summary → enriched → PostgreSQL
- [ ] 10.3 Write test for broadcast enrichment: call with customer profile → enriched event includes segment + riskLevel
- [ ] 10.4 Write test for missing customer profile: call without profile → default values used
- [ ] 10.5 Write test for metadata lifecycle: all status transitions occur in correct order
- [ ] 10.6 Write test for dual-write consistency: both Kafka and PostgreSQL receive data
- [ ] 10.7 Write test for JDBC failure: PostgreSQL down → Kafka write succeeds, error logged
- [ ] 10.8 Verify all integration tests pass with real Kafka and PostgreSQL containers

## 11. Docker and Deployment

- [ ] 11.1 Create Dockerfile with multi-stage build (Gradle build → runtime JRE) and verify image size < 300MB
- [ ] 11.2 Configure docker-compose service for transcription-analyzer with correct environment variables
- [ ] 11.3 Verify service starts and connects to Kafka cluster in Docker network
- [ ] 11.4 Verify service connects to PostgreSQL in Docker network
- [ ] 11.5 Verify full pipeline: call event → transcription → enrichment → PostgreSQL data visible

## 12. Bootstrap customers.profile

- [ ] 12.1 Create bootstrap-customers.sh script that reads CSV and produces to customers.profile topic via kafka-console-producer
- [ ] 12.2 Verify bootstrap script works with sample data (at least 10 customers with segment + riskLevel)
- [ ] 12.3 Verify customers.profile topic contains data after bootstrap (check in Kafdrop)
