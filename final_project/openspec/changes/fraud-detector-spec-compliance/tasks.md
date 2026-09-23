## 1. Schema Registry + Avro Output

- [ ] 1.1 Verify FraudAlert schema is NOT registered in Schema Registry (subjects: calls.completed-value, calls.metadata-value exist; calls.fraud-alerts-value missing)
- [ ] 1.2 Add Schema Registry schema registration on startup (idempotent: check subjects, register if missing) — verify schema registered at calls.fraud-alerts-value
- [ ] 1.3 Configure KafkaAvroSerializer for output topic in StreamsTopologyConfig — verify calls.fraud-alerts topic receives Avro-serialized FraudAlert
- [ ] 1.4 Verify Avro serialization with console-consumer (strings | grep ANOMALOUS) — verify output contains Avro binary (not pipe-delimited String)
- [ ] 1.5 Verify reporting-nps consumer compatibility (Avro format) — verify no changes needed in reporting-nps

## 2. Short Call Filter

- [ ] 2.1 Add `.filter((key, value) -> extractDuration(value) >= 5)` at topology entry point before branching to processors — verify short calls (< 5s) are filtered out
- [ ] 2.2 Verify short calls produce no alerts for any pattern — verify calls.fraud-alerts topic has no events for duration < 5s
- [ ] 2.3 Write unit test for short call filter — verify test passes

## 3. DSL Hopping Window (Frequent Calls)

- [ ] 3.1 Rewrite FrequentCallsProcessor: use `KStream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))` → `.windowedBy(TimeWindows.ofSizeAndAdvance(60000, 10000))` → `.count(Materialized.as("frequent-calls-count"))` — verify RocksDB state store "frequent-calls-count" is created
- [ ] 3.2 Process windowed results: `.toStream()` → `.filter(count > 5 && !isExpired)` → map to Avro FraudAlert — verify FREQUENT_CALLS alert with count field and severity=MEDIUM
- [ ] 3.3 Verify window expiration behavior (1 min size, 10s advance) — verify overlapping windows work correctly
- [ ] 3.4 Write integration test: 6 calls from same phone in 30 sec → FREQUENT_CALLS alert — verify alert in calls.fraud-alerts
- [ ] 3.5 Write integration test: 5 calls from same phone → no alert — verify no alert produced

## 4. Processor API (NPS Escalation)

- [ ] 4.1 Create NpsEscalationProcessor: implement `Processor<String, String>` with `KeyValueStore<String, NpsState>` — verify StateStore "nps-escalation-store" is created
- [ ] 4.2 Implement `init()`: get StateStore reference with TTL config — verify store persists to RocksDB
- [ ] 4.3 Implement `process()`: update counter, check threshold (>= 3), forward alert to "fraud-alerts" — verify NPS_ESCALATION alert with count and severity=HIGH
- [ ] 4.4 Implement TTL cleanup: `purgeExpired()` or periodic task for 24h inactivity — verify stale entries removed
- [ ] 4.5 Write integration test: 3x NPS < 2 in 24 hours → NPS_ESCALATION alert — verify alert in calls.fraud-alerts
- [ ] 4.6 Write integration test: 2x NPS < 2 → no alert — verify no alert produced

## 5. Severity Levels + Alert Format

- [ ] 5.1 Update AnomalousDurationProcessor: severity=LOW in FraudAlert — verify alert has severity=LOW
- [ ] 5.2 Update FrequentCallsProcessor: severity=MEDIUM in FraudAlert — verify alert has severity=MEDIUM
- [ ] 5.3 Update NpsEscalationProcessor: severity=HIGH in FraudAlert — verify alert has severity=HIGH
- [ ] 5.4 Verify all alerts include: callId, phone, pattern, count, timestamp, severity — verify FraudAlert Avro schema compliance

## 6. Integration Tests

- [ ] 6.1 Add dependency: kafka-streams-test-utils (TopologyTestDriver) — verify test compile succeeds
- [ ] 6.2 Write FrequentCallsIntegrationTest: DSL windowing with EmbeddedKafka — verify 6+ calls → alert, 5 calls → no alert
- [ ] 6.3 Write NpsEscalationIntegrationTest: Processor API with EmbeddedKafka — verify 3x NPS < 2 → alert, 2x → no alert
- [ ] 6.4 Write AnomalousDurationIntegrationTest: filter + Avro output — verify duration > 300 → alert
- [ ] 6.5 Write StateRecoveryTest: restart service → state restored from RocksDB — verify processing continues without data loss
- [ ] 6.6 Write AvroSerializationTest: Schema Registry integration — verify FraudAlert schema registered, Avro output valid
- [ ] 6.7 Run `./gradlew test` — verify all tests pass
- [ ] 6.8 Verify code coverage > 70% with `./gradlew testCoverage` — verify coverage report shows > 70%

## 7. Docker Deployment + End-to-End

- [ ] 7.1 Build Docker image: `docker compose build fraud-detector` — verify image builds successfully
- [ ] 7.2 Deploy to Docker: `docker compose up -d fraud-detector` — verify container healthy
- [ ] 7.3 Verify Kafka Streams topology initialized (logs: "Kafka Streams instance started") — verify StreamThread-1 running
- [ ] 7.4 Send test: anomalous duration (600s) → verify ANOMALOUS_DURATION alert in calls.fraud-alerts
- [ ] 7.5 Send test: NPS escalation (3x NPS < 2) → verify NPS_ESCALATION alert
- [ ] 7.6 Send test: frequent calls (6+ in 1 min) → verify FREQUENT_CALLS alert
- [ ] 7.7 Send test: short call (3s) → verify NO alert produced
- [ ] 7.8 Verify all alerts have correct Avro format and severity levels — verify calls.fraud-alerts topic contains valid Avro

## 8. Failure Log + Documentation

- [ ] 8.1 Update failure-log.md with new issues found during implementation (R-58+) — verify new entries added
- [ ] 8.2 Update original fraud-detector change status if needed — verify tasks.md reflects completed work
- [ ] 8.3 Archive fraud-detector-spec-compliance change — verify change archived with all tasks complete
