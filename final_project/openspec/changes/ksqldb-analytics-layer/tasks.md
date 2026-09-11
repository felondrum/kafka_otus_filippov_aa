## 1. Infrastructure — ksqlDB Server

- [ ] 1.1 Add ksqldb-server service to docker-compose.yml (confluentinc/cp-ksqldb-server:latest) and verify container starts successfully
- [ ] 1.2 Configure ksqlDB properties (bootstrap.servers, schema.registry.url, ksql.service.id, ksql.streams.state.dir, security.protocol=SASL_PLAINTEXT, sasl.mechanism=PLAIN, sasl.jaas.config) and verify REST API responds on port 8088
- [ ] 1.3 Configure Schema Registry integration (schema.registry.url=http://schema-registry:8085) and verify ksqlDB can resolve Avro schemas for calls.completed and calls.fraud-alerts
- [ ] 1.4 Configure ksqlDB to run in embedded mode (no separate CLI container) and verify CLI commands can be sent via REST API
- [ ] 1.5 Verify ksqlDB connects to Kafka cluster and can list available topics (calls.completed, calls.fraud-alerts)
- [ ] 1.6 Configure query timeout (ksql.query.timeout.ms=30000 or request.timeout.ms=30000) and verify long-running queries return HTTP 408 after 30 seconds

## 2. calls_completed_agg Stream

- [ ] 2.1 Create calls_completed_agg persistent stream via REST API (POST /ksql) with tumbling 5-minute window, 1-minute grace period, and verify stream status is STREAM_RUNNING
- [ ] 2.2 Verify stream consumes from calls.completed topic (Avro-encoded, key=callId) and groups by agentId field (defined in call-processor spec, CallEvent schema)
- [ ] 2.3 Verify stream computes per-agent metrics: call_count (COUNT), avg_nps_score (AVG), last_call_at (MAX) and emits to calls.completed.agg output topic
- [ ] 2.4 Produce 3 test events to calls.completed with same agentId within 5-minute window and verify calls.completed.agg receives updated record with call_count=3
- [ ] 2.5 Produce test events to calls.completed with different agentIds and verify separate aggregation records per agentId in output topic
- [ ] 2.6 Test window expiration: produce events, wait for 5-minute window + 1-minute grace period, verify final aggregation is emitted and window state is cleared
- [ ] 2.7 Test persistent query survival: restart ksqlDB container, produce events, verify stream resumes from last committed offset and continues aggregating

## 3. fraud_alerts_filtered Stream

- [ ] 3.1 Create fraud_alerts_filtered persistent stream via REST API (POST /ksql) with hopping 10-minute window, 1-minute step, severity filter (HIGH/MEDIUM only), and verify stream status is STREAM_RUNNING
- [ ] 3.2 Verify stream consumes from calls.fraud-alerts topic (Avro-encoded, key=phone) and filters out severity='LOW' events
- [ ] 3.3 Verify stream groups by phone, pattern, severity (note: fraud-alerts has key=phone, no agentId field) and computes per-group metrics: alert_count (COUNT)
- [ ] 3.4 Produce test fraud alert with severity='LOW' and verify event is dropped (NOT included in aggregation, NOT emitted to output topic)
- [ ] 3.5 Produce 5 test fraud alerts with severity='HIGH' for same phone="+79001234567"+pattern within 10-minute window and verify calls.fraud-alerts.agg receives updated record with alert_count=5
- [ ] 3.6 Produce test fraud alert with severity='MEDIUM' and verify event is processed and included in aggregation
- [ ] 3.7 Test hopping window step: produce events, wait for 1-minute step, verify partial aggregation is emitted to output topic (window does not wait for full 10 minutes)
- [ ] 3.8 Test persistent query survival: restart ksqlDB container, produce events, verify stream resumes and continues aggregating

## 4. Ad-hoc Queries via REST API

- [ ] 4.1 Test ad-hoc query: POST `SELECT * FROM calls_completed_agg WHERE agentId = 'agent-001'` to /ksql endpoint and verify response returns JSON array with current aggregation
- [ ] 4.2 Test ad-hoc query: POST `SELECT * FROM fraud_alerts_filtered WHERE severity = 'HIGH'` to /ksql endpoint and verify response returns JSON array with HIGH severity fraud aggregations
- [ ] 4.3 Test ad-hoc query: POST `SELECT agentId, call_count FROM calls_completed_agg ORDER BY call_count DESC LIMIT 10` and verify response returns top 10 agents by call count
- [ ] 4.4 Test query timeout: POST long-running query (SELECT * FROM calls_completed_agg without WHERE clause, large dataset simulation) and verify ksqlDB returns HTTP 408 after 30 seconds
- [ ] 4.5 Test invalid query: POST `SELECT * FROM nonexistent_stream` and verify ksqlDB returns HTTP 400 with error message
- [ ] 4.6 Test unsupported operation: POST `INSERT INTO calls_completed_agg ...` and verify ksqlDB returns HTTP 405 Method Not Allowed (streams are append-only)

## 5. Integration Testing

- [ ] 5.1 End-to-end: produce 10 calls.completed events to Kafka, verify calls_completed_agg stream processes all 10 and output topic contains correct aggregations
- [ ] 5.2 End-to-end: produce 10 calls.fraud-alerts events (mix of HIGH/MEDIUM/LOW), verify fraud_alerts_filtered stream processes only HIGH+MEDIUM and output topic contains correct aggregations
- [ ] 5.3 Integration: consume calls.completed.agg output topic via Kafka consumer and verify Avro schema matches stream definition (agentId:STRING, call_count:BIGINT, avg_nps_score:DOUBLE, last_call_at:LONG)
- [ ] 5.4 Integration: consume calls.fraud-alerts.agg output topic via Kafka consumer and verify Avro schema matches stream definition (phone:STRING, pattern:STRING, severity:STRING, alert_count:BIGINT)
- [ ] 5.5 Cross-service: verify reporting-nps is NOT affected by ksqlDB (reporting-nps CQRS read side continues to work independently)

## 6. Health Check and Monitoring

- [ ] 6.1 Add health check endpoint to ksqlDB (built-in /health/live and /health/ready) and verify endpoints respond correctly
- [ ] 6.2 Add Grafana dashboard panel for ksqlDB stream lag (calls_completed_agg lag, fraud_alerts_filtered lag) and verify panel shows correct lag values
- [ ] 6.3 Configure alert on stream lag > 60 seconds (alert: ksqlDBStreamLag) and verify alert fires when lag exceeds threshold

## 7. Documentation

- [ ] 7.1 Update ARCHITECTURE/04-data-flow.md: add ksqlDB streams to data flow diagram (calls.completed → ksqlDB → calls.completed.agg, calls.fraud-alerts → ksqlDB → calls.fraud-alerts.agg)
- [ ] 7.2 Update ARCHITECTURE/03-container-architecture.md: add ksqldb-server container to container table (if not already present)
- [ ] 7.3 Document ksqlDB REST API usage (port 8088, POST /ksql, request/response format) in ARCHITECTURE/04-data-flow.md
- [ ] 7.4 Document ksqlDB output topics (calls.completed.agg, calls.fraud-alerts.agg) and their schemas in ARCHITECTURE/04-data-flow.md
