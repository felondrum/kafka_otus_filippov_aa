## 1. Infrastructure — Kafka Connect Service

- [x] 1.1 Add kafka-connect service to docker-compose.yml (confluentinc/cp-kafka-connect:latest) and verify container starts successfully
- [x] 1.2 Configure Kafka Connect properties (bootstrap.servers, group.id, key.converter, value.converter, key.converter.schemas.enable, value.converter.schemas.enable) and verify Connect REST API responds on port 8083
- [x] 1.3 Configure Schema Registry integration (schema.registry.url=http://schema-registry:8085) and verify connector can resolve JSON schemas
- [x] 1.4 Verify kafka-connect-user PostgreSQL user exists (created by infrastructure change, task 3.x in infrastructure/tasks.md) with minimal privileges (INSERT, SELECT on call_transcriptions only) and verify connection with psql
- [x] 1.5 Configure JDBC URL for kafka-connect-user PostgreSQL connection (local development, SSL optional) and verify connection works

## 2. JDBC Sink Connector Configuration

- [x] 2.1 Deploy JDBC Sink connector via REST API (POST /connectors/kafka-connect-jdbc-sink) with connector.class=io.confluent.connect.jdbc.JdbcSinkConnector and verify connector status is RUNNING
- [x] 2.2 Configure tasks.max=6 for partition-level parallelism and verify 6 connector tasks are created (matches 6 partitions of transcription.enriched topic)
- [x] 2.3 Configure connection.url with JDBC URL to PostgreSQL and verify connector can connect
- [x] 2.4 Configure table.name.format=call_transcriptions, pk.mode=RecordKey, pk.fields=call_id, auto.create=false, insert.mode=upsert and verify connector configuration is persisted
- [x] 2.5 Configure topics=transcription.enriched and verify connector starts consuming from the topic

## 3. Transformations (ExtractField, ReplaceString)

- [x] 3.1 Configure ExtractField transformation (transforms.extract.type=org.apache.kafka.connect.transforms.ExtractField$Value, transforms.extract.field=value) and verify event fields are extracted correctly
- [x] 3.2 Configure ReplaceString transformation (transforms.replace.type=org.apache.kafka.connect.transforms.ReplaceString$Value) and verify SQL injection characters (' , " , \) are escaped in text fields
- [x] 3.3 Verify end-to-end: transcription.enriched event → ExtractField + ReplaceString → PostgreSQL call_transcriptions row with correct types and escaped text
  - Note: JSON-to-SQL type conversion is handled automatically by Confluent JDBC Connector via Schema Registry (no explicit ConvertFieldSchema SMT)

## 4. Offset Management and Retry

- [x] 4.1 Verify Kafka Connect commits offsets to _connect_offsets topic and confirm offsets are committed periodically (every 5 minutes) and on connector shutdown
- [ ] 4.2 Test offset recovery: stop Kafka Connect, delete a committed event from transcription.enriched, restart connector — verify no duplicate writes (upsert mode)
- [x] 4.3 Configure retry logic (retry.backoff.ms=1000, max.retries=3) and verify connector retries failed writes with exponential backoff (1s, 2s, 4s)
- [x] 4.4 Configure DLQ topic (errors.deadletterqueue.topic.name=transcription.enriched.dlq) and verify failed events are sent to DLQ after max retries
  - Note: DLQ topic replication factor is defined in infrastructure change (topic creation), not in connector config
- [x] 4.5 Verify DLQ topic exists and contains events (if any failures occurred during testing)
  - Note: DLQ received events from pre-fix test attempts (callId=7d0d4670-6b6a-4d63-ab37-b0b4b9bf4d7a). Post-fix tests succeeded without DLQ entries.

## 5. Integration Testing

- [x] 5.1 Produce test event to transcription.enriched topic (JSON-encoded, key=callId) and verify it appears in PostgreSQL call_transcriptions table via Kafka Connect
  - Note: Test with UUID callId ("e2e-test-uuid-12345") — call_transcriptions has 1 row after fix.
  - Note: Test with non-UUID callId ("non-uuid-test-call") — generated UUID a4796a21-... inserted successfully.
- [ ] 5.2 Produce 10 test events with different callIds and verify all 10 rows exist in call_transcriptions with correct field values
- [ ] 5.3 Test upsert: produce event with existing callId and verify row is updated (not duplicated) in call_transcriptions
- [ ] 5.4 Test DualWriter fallback: stop Kafka Connect container, produce event to transcription.enriched — verify DualWriter still writes to PostgreSQL
- [ ] 5.5 Test connector restart: restart Kafka Connect container — verify connector resumes from last committed offset (no duplicate writes)
- [ ] 5.6 Test SQL injection prevention: produce event with transcription_text containing '; DROP TABLE call_transcriptions; — verify text is escaped in PostgreSQL (no table dropped)
  - Note: Confluent JDBC Connector uses PreparedStatement (parameterized queries) as the primary SQL injection protection; ReplaceString is an additional defense-in-depth layer

## 6. Health Check and Monitoring

- [x] 6.1 Add health check endpoint to Kafka Connect (built-in /health/live and /health/ready) and verify endpoints respond correctly
- [x] 6.2 Add Grafana dashboard panel for Kafka Connect connector status (RUNNING/PAUSED/FAILED) and verify panel shows correct status
- [x] 6.3 Configure alert on DLQ event rate > 0 (alert: KafkaConnectDLQEvents) and verify alert fires when DLQ receives events

## 7. Documentation

- [x] 7.1 Update ARCHITECTURE/04-data-flow.md: add Kafka Connect JDBC Sink path to transcription.enriched data flow diagram
- [x] 7.2 Update ARCHITECTURE/03-container-architecture.md: add kafka-connect container to container table
- [x] 7.3 Document the 3-path architecture (Kafka Connect + DualWriter + reporting-nps) in ARCHITECTURE/04-data-flow.md section 4.3.6
- [x] 7.4 Document kafka-connect-user database user and permissions in ARCHITECTURE/07-security.md
