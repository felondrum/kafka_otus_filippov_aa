## 1. Fix JMX Exporter Configuration

- [x] 1.1 Expand `jmx-exporter-config.yml` with broker topic metrics rules (messagesinpersec, bytesinpersec, bytesoutpersec) and verify the YAML parses without errors
- [x] 1.2 Add network metrics rules (kafka_network_servermetrics_bytesinpersec, bytesoutpersec) and verify YAML validity
- [x] 1.3 Add request latency rules (kafka_network_requestmetrics_totaltimems_avg for Produce, FetchConsumer, FetchLeader) and verify YAML validity
- [x] 1.4 Add consumer group lag rules (kafka_consumer_group_metrics_total_lag) and partition leadership rules (kafka_server_replicamanager_underreplicatedpartitions, isrshrinkagerate) and verify YAML validity
- [x] 1.5 Keep existing catch-all JMX pattern as fallback and verify final config has 15+ rules

## 2. Add Micrometer Dependencies to Services

- [x] 2.1 Add `io.micrometer:micrometer-registry-prometheus` to `call-processor/build.gradle.kts` and verify `./gradlew :call-processor:dependencies --configuration runtimeClasspath` includes the dependency
- [x] 2.2 Add `io.micrometer:micrometer-registry-prometheus` to `fraud-detector/build.gradle.kts` and verify dependency resolution succeeds
- [x] 2.3 Add `io.micrometer:micrometer-registry-prometheus` to `transcription-analyzer/build.gradle.kts` and verify dependency resolution succeeds
- [x] 2.4 Verify reporting-nps already has the dependency (no changes needed)

## 3. Enable Prometheus Scrape Configs

- [x] 3.1 Uncomment and update `prometheus.yml` scrape configs for call-processor:8081, fraud-detector:8082, transcription-analyzer:8083, reporting-nps:8084 with correct metrics_path `/actuator/prometheus`
- [x] 3.2 Add kafka-connect:8083 to infrastructure scrape config
- [x] 3.3 Verify Prometheus configuration is valid with `curl -X POST http://localhost:9090/-/reload` (after restart) and check Prometheus logs for no errors

## 4. Create Per-Service Dashboards

- [x] 4.1 Create `call-processor.json` dashboard with JVM memory/CPU panels, Kafka producer send rate/latency, and HTTP API request rate/latency panels
- [x] 4.2 Create `fraud-detector.json` dashboard with JVM metrics, Kafka Streams records processed/sec, throughput, and state store size panels
- [x] 4.3 Create `transcription-analyzer.json` dashboard with JVM metrics, Kafka consumer fetch rate/commit latency, and processing time panels
- [x] 4.4 Create `reporting-nps.json` dashboard with JVM metrics, NPS report generation rate, database query performance, and cache hit ratio panels

## 5. Create System Overview Dashboard

- [x] 5.1 Create `system-overview.json` dashboard with Row 1: service health stat panels (4 services + 3 brokers + postgres + kafka-connect)
- [x] 5.2 Add Row 2: topic throughput timeseries (messages/sec grouped by topic)
- [x] 5.3 Add Row 3: database write rates timeseries (INSERT/UPDATE/sec grouped by table)
- [x] 5.4 Add Row 4: consumer group lag timeseries (grouped by consumer group)
- [x] 5.5 Add Row 5: Kafka network throughput (bytes in/out) and Row 6: HTTP request latency per service

## 6. Fix Existing Dashboards with Template Variables

- [x] 6.1 Add `$broker` template variable to `kafka-overview.json` and update all panel queries to use `instance=~"$broker"` filtering
- [x] 6.2 Add `$connector` template variable to `kafka-connect-jdbc-sink.json` and replace all hardcoded `connector="kafka-connect-jdbc-sink"` with `connector=~"$connector"`
- [x] 6.3 Add `$database` template variable to `postgres-overview.json` and replace all hardcoded `datname="call_platform"` with `datname=~"$database"`

## 7. Validate and Test

- [x] 7.1 Restart infrastructure with `docker compose --profile full up -d` and verify all scrape targets are UP in Prometheus `/targets` page
- [x] 7.2 Verify Kafka overview dashboard shows data (not "No Data") in Grafana
- [x] 7.3 Verify PostgreSQL overview dashboard shows data in Grafana
- [x] 7.4 Verify all 4 per-service dashboards show JVM metrics in Grafana
- [x] 7.5 Verify system overview dashboard shows topic throughput and consumer lag in Grafana
- [x] 7.6 Verify template variables work (filter by broker, connector, database) in Grafana
