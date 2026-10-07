## 1. PostgreSQL Schema Initialization

- [x] 1.1 Подключить init-reporting-nps.sql к PostgreSQL в docker-compose.yml (volume mount)
- [x] 1.2 Verify fraud_stats table created on PG startup
- [x] 1.3 Verify v_full_call_info view created on PG startup

## 2. Kafka Connect JDBC Sink Deployment

- [x] 2.1 Create infrastructure/kafka/kafka-connect-init.sh script
- [x] 2.2 Script waits for Kafka Connect and Schema Registry to be ready
- [x] 2.3 Deploy JDBC Sink connector via REST API (POST /connectors/kafka-connect-jdbc-sink)
- [x] 2.4 Connector config: pk.mode=RecordKey, insert.mode=upsert, tasks.max=6, transformations
- [x] 2.5 Script is idempotent — doesn't fail if connector already exists
- [x] 2.6 Add kafka-connect-init service to docker-compose.yml
- [x] 2.7 kafka-connect-init depends_on: kafka-connect, schema-registry

## 3. Reporting-NPS Consumer — Avro Support

- [x] 3.1 Update KafkaListenerConfig.java: StringDeserializer → ConfluentJSONDeserializer
- [x] 3.2 Add schema.registry.url and trusted.packages to consumer config
- [x] 3.3 Verify consumer can read Avro events from calls.metadata, transcription.enriched, calls.fraud-alerts
- [x] 3.4 Verify compile and test pass

## 4. Bootstrap Script

- [x] 4.1 Create infrastructure/e2e-bootstrap.sh script
- [x] 4.2 Script waits for all services to be healthy (call-processor, reporting-nps, etc.)
- [x] 4.3 Send test call event via POST /api/calls (use test-call.json)
- [x] 4.4 Send enriched transcription event via Kafka console-producer
- [x] 4.5 Send fraud alert event via Kafka console-producer
- [x] 4.6 Wait for consumers to process events (poll every 5s, max 60s)
- [x] 4.7 Verify GET /api/reports/daily returns total_calls > 0
- [x] 4.8 Verify GET /api/metadata/{callId} returns test call data
- [x] 4.9 Verify GET /api/sentiment/distribution returns sentiment counts
- [x] 4.10 Print PASS/FAIL summary for each step

## 5. Docker Compose Dependencies

- [x] 5.1 Add kafka-init dependency to reporting-nps service
- [x] 5.2 Add schema-init dependency to kafka-connect-init service
- [x] 5.3 Verify all service healthchecks are correct
- [x] 5.4 Verify profiles work correctly (core, full, monitoring)

## 6. Makefile Targets

- [x] 6.1 Add make e2e-setup: deploy + init-topics + register-schemas + kafka-connect-init
- [x] 6.2 Add make e2e-verify: run e2e-bootstrap.sh
- [x] 6.3 Add make e2e: full cycle (e2e-setup + e2e-verify)
- [x] 6.4 Add make e2e-cleanup: docker compose down -v

## 7. Integration Verification

- [x] 7.1 Run make e2e and verify all steps PASS
- [x] 7.2 Verify full pipeline: call → metadata → PG → daily report
- [x] 7.3 Verify transcription.enriched → Kafka Connect → PG → sentiment distribution
- [x] 7.4 Verify fraud-alerts → PG fraud_stats → agent report
