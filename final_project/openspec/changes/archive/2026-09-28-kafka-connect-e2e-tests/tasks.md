## 1. Test Infrastructure Setup

- [x] 1.1 Create `infrastructure/kafka-connect-e2e-test.sh` with helper functions (pass/fail counters, service health checks, PostgreSQL queries) and verify script has correct shebang and executable permissions
- [x] 1.2 Add `make kafka-connect-e2e` target to `Makefile` and verify `make kafka-connect-e2e` executes the test script
- [x] 1.3 Verify `kafka-console-producer` binary is available in Confluent Platform Docker image and can connect to Schema Registry

## 2. Phase 1: Connector Health & Configuration

- [x] 2.1 Implement test: GET `/connectors/kafka-connect-jdbc-sink` → HTTP 200 and verify response contains connector metadata
- [x] 2.2 Implement test: GET `/connectors/kafka-connect-jdbc-sink/status` → `connector.status` is `RUNNING` and verify status field value
- [x] 2.3 Implement test: GET `/connectors/kafka-connect-jdbc-sink/config` → verify `insert.mode=upsert`, `pk.mode=RecordKey`, `tasks.max=6` and verify config JSON fields
- [x] 2.4 Implement test: GET `/connectors/kafka-connect-jdbc-sink/status` → verify `tasks` array has exactly 6 entries and verify task count
- [x] 2.5 Implement test: GET `http://localhost:8085/subjects` → HTTP 200 with registered subjects and verify Schema Registry responds

## 3. Phase 2: Data Flow — JSON to PostgreSQL

- [x] 3.1 Implement test: Produce 1 JSON event using `kafka-console-producer` with `EnrichedTranscription.json` schema and verify event is accepted by Schema Registry
- [x] 3.2 Implement polling mechanism: wait up to 60 seconds for row count in `call_transcriptions` to match expected count and verify row appears within timeout
- [x] 3.3 Implement test: Verify field mapping — `callId` → `call_id`, `sentiment` → `sentiment`, `confidence` → `confidence` and verify PostgreSQL column values match JSON fields
- [x] 3.4 Implement test: Produce 10 JSON events with unique `callId` values and verify exactly 10 rows exist in `call_transcriptions` with correct field values

## 4. Phase 3: Field Transformations (ExtractField + ReplaceString)

- [x] 4.1 Implement test: Produce event with nested structure and verify `ExtractField` flattens to flat columns in PostgreSQL
- [x] 4.2 Implement test: Produce event with `transcription_text` containing `'` (single quote) and verify PostgreSQL stores escaped value (no SQL injection)
- [x] 4.3 Implement test: Produce event with `transcription_text` containing `"; DROP TABLE call_transcriptions; --` and verify table remains intact, text is safely stored

## 5. Phase 4: Upsert Semantics

- [x] 5.1 Implement test: Produce event with `callId="upsert-test-1"`, wait for write, then produce event with same `callId` but different `sentiment` and verify row is updated
- [x] 5.2 Implement test: Produce 3 unique events + 1 duplicate event and verify exactly 3 rows exist (no duplicates)

## 6. Phase 5: Retry and Dead Letter Queue (DLQ)

- [x] 6.1 Implement test: Revoke INSERT permission from `kafka-connect-user`, produce event, verify connector retries with backoff (1s, 2s, 4s) and verify retry attempts logged
- [x] 6.2 Implement test: After max.retries=3, verify event is in `transcription.enriched.dlq` topic and verify DLQ topic contains failed event
- [x] 6.3 Implement test: Verify DLQ event includes error message and original payload and verify DLQ message structure
- [x] 6.4 Restore INSERT permission to `kafka-connect-user` and verify connector resumes normal operation

## 7. Phase 6: Offset Recovery

- [x] 7.1 Implement test: Produce 5 events, stop Kafka Connect container, verify offsets committed to `_connect_offsets` topic and verify offset commit exists
- [x] 7.2 Implement test: Restart Kafka Connect, produce 3 more events, verify total row count = 8 (5 + 3, no duplicates) and verify row count is exactly 8

## 8. Phase 7: DualWriter Fallback

- [x] 8.1 Implement test: Stop Kafka Connect container, produce event to `transcription.enriched`, wait up to 60 seconds and verify DualWriter writes to PostgreSQL
- [x] 8.2 Implement test: Restart Kafka Connect, verify connector resumes from offset without duplicate writes and verify no duplicate rows in `call_transcriptions`

## 9. Test Summary and CI/CD Integration

- [x] 9.1 Implement summary output: pass/fail counts, total tests, exit code 0/1 and verify Makefile target returns correct exit code
- [x] 9.2 Add optional phase skipping via environment variables (e.g., `KAFKA_CONNECT_E2E_SKIP_DLQ=1`) and verify skipped phases are reported in summary
