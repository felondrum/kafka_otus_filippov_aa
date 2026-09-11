## 1. Docker Compose Core — Kafka Cluster

- [ ] 1.1 Create docker-compose.yml with 3 Kafka brokers in KRaft mode and verify all 3 brokers start successfully. Map Kafka Connect external port to 8086 to avoid conflict with transcription-analyzer (8083).
- [ ] 1.2 Configure SASL/PLAIN authentication for Kafka brokers and verify clients without credentials are rejected
- [ ] 1.3 Configure controller quorum across 3 brokers and verify cluster forms correctly
- [ ] 1.4 Set up Docker volumes for Kafka data (kafka-1-data, kafka-2-data, kafka-3-data) and verify data persists after restart

## 2. Docker Compose — Schema Registry

- [ ] 2.1 Add Schema Registry service to docker-compose.yml and verify it starts on port 8085
- [ ] 2.2 Configure Schema Registry to connect to Kafka cluster and verify schema registration works
- [ ] 2.3 Set BACKWARD compatibility mode and verify incompatible schema changes are rejected

## 3. Docker Compose — PostgreSQL

- [ ] 3.1 Add PostgreSQL 15 service to docker-compose.yml and verify it starts on port 5432
- [ ] 3.2 Create init-db.sql script with call_metadata and call_transcriptions tables and verify tables are created on first start
- [ ] 3.3 Set up foreign key constraint between call_transcriptions.call_id and call_metadata.call_id and verify constraint is enforced
- [ ] 3.4 Set up Docker volume for PostgreSQL data and verify data persists after restart
- [ ] 3.5 Create kafka-connect-user PostgreSQL user with INSERT/SELECT on call_transcriptions only and verify user can connect with psql

## 4. Docker Compose — Monitoring Stack

- [ ] 4.1 Add Prometheus service to docker-compose.yml and verify it starts on port 9090
- [ ] 4.2 Create prometheus.yml configuration with scrape targets for Kafka brokers, PostgreSQL, and infrastructure services (microservice targets as commented placeholders for future changes)
- [ ] 4.3 Add Grafana service to docker-compose.yml and verify it starts on port 3000 with Prometheus data source
- [ ] 4.4 Add Kafdrop service to docker-compose.yml and verify it starts on port 9000 and connects to Kafka cluster

## 5. Docker Compose — Profiles and Network

- [ ] 5.1 Create Docker Compose profiles (full, core, monitoring, load-test) and verify each profile deploys the correct subset of services
- [ ] 5.2 Create dedicated Docker network (call-platform-net) and verify all services connect to it
- [ ] 5.3 Configure DNS-based service discovery and verify services can reach each other by container name

## 6. Kafka Topics Initialization

- [ ] 6.1 Create topic initialization script (or Topic Manager) to create all 8 topics with correct configuration
- [ ] 6.2 Create stream topics (calls.completed, calls.fraud-alerts, transcription.raw, transcription.summary, transcription.enriched, calls.dlq) with Avro format, replication=3, partitions=6
- [ ] 6.3 Create compacted topics (calls.metadata, customers.profile) with Avro format, replication=3, partitions=6, cleanup.policy=compact and verify cleanup policy is applied
- [ ] 6.4 Verify all topics are visible in Kafdrop after initialization
- [ ] 6.5 Create ksqlDB output topics (calls.completed.agg, calls.fraud-alerts.agg) with Avro format, replication=3, partitions=6
- [ ] 6.6 Create DLQ topic (transcription.enriched.dlq) with JSON format, replication=3, partitions=3
- [ ] 6.7 Verify ksqlDB output topics are visible in Kafdrop

## 7. Health Checks and Dependencies

- [ ] 7.1 Add health check configurations to all services in docker-compose.yml
- [ ] 7.2 Configure depends_on with health conditions for service startup order
- [ ] 7.3 Verify Kafka brokers are healthy before dependent services start
- [ ] 7.4 Verify PostgreSQL is ready before dependent services start
- [ ] 7.5 Verify Schema Registry is ready before dependent services start

## 8. Makefile and Development Tools

- [ ] 8.1 Create Makefile with deploy target (docker compose --profile full up -d) and verify it starts all services
- [ ] 8.2 Create Makefile target for health check (docker compose ps / curl to health endpoints) and verify it reports service status
- [ ] 8.3 Create Makefile target for logs (docker compose logs -f) and verify it streams logs
- [ ] 8.4 Create Makefile target for stop (docker compose --profile full down) and verify all services stop cleanly
- [ ] 8.5 Create Makefile target for clean (docker compose down -v) and verify volumes are removed

## 9. ARM64 Optimization

- [ ] 9.1 Configure all Docker images for linux/arm64/v8 platform and verify no emulation warnings in Docker
- [ ] 9.2 Configure tmpfs for Kafka broker RocksDB state store and verify no emulation warnings on ARM64
- [ ] 9.3 Test full stack on Apple Silicon and verify all services run without architecture errors

## 10. Verification and Documentation

- [ ] 10.1 Run `make all` (or equivalent) and verify entire stack starts within 2 minutes
- [ ] 10.2 Verify all 8 Kafka topics are created and visible in Kafdrop
- [ ] 10.3 Verify Prometheus collects metrics from at least Kafka and PostgreSQL
- [ ] 10.4 Verify Grafana loads and displays dashboards with Prometheus data
- [ ] 10.5 Create README section with quick start instructions and verify commands work end-to-end
