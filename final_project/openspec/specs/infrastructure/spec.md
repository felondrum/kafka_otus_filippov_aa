## Purpose

Определяет требования к развёртыванию и управлению инфраструктурой платформы обработки звонков колл-центра: Kafka-кластер, PostgreSQL, Schema Registry и инструменты мониторинга.

## ADDED Requirements

### Requirement: Kafka Cluster Deployment

The system SHALL deploy a 3-broker Apache Kafka cluster in KRaft mode (no ZooKeeper) with SASL/PLAIN authentication and ACL support.

#### Scenario: Kafka cluster starts with 3 brokers
- **WHEN** Docker Compose is executed with the `full` profile
- **THEN** 3 Kafka brokers (kafka-1, kafka-2, kafka-3) start in KRaft mode with controller quorum

#### Scenario: SASL authentication is enforced on EXTERNAL listener
- **WHEN** a client connects to Kafka via EXTERNAL listener (port 9093) without credentials
- **THEN** the connection is rejected with an authentication error
- **NOTE:** INTERNAL listener (broker-to-broker, port 9092) uses PLAINTEXT for local development simplicity

#### Scenario: Topics are created with correct configuration
- **WHEN** the infrastructure is initialized
- **THEN** all 8 topics are created with replication factor 3 and 6 partitions

### Requirement: Schema Registry

The system SHALL deploy a Confluent Schema Registry with BACKWARD compatibility mode for Avro schemas.

#### Scenario: Schema Registry starts and connects to Kafka
- **WHEN** Docker Compose is executed
- **THEN** Schema Registry starts on port 8085 with BACKWARD compatibility mode and connects to Kafka via SASL_PLAINTEXT

#### Scenario: Schema validation rejects incompatible changes
- **WHEN** a producer attempts to register a schema with BREAKING changes (e.g., removing a required field)
- **THEN** the Schema Registry rejects the registration with HTTP 409 Conflict

### Requirement: PostgreSQL Database

The system SHALL deploy PostgreSQL 15 as the analytical database for call metadata and transcriptions.

#### Scenario: PostgreSQL starts with initialized schema
- **WHEN** Docker Compose is executed
- **THEN** PostgreSQL starts on port 5432 with database `call_platform` and two tables: `call_metadata` and `call_transcriptions`

#### Scenario: Foreign key constraints are enforced
- **WHEN** a transcription record references a non-existent call
- **THEN** PostgreSQL rejects the insert with a foreign key violation

### Requirement: Monitoring Stack

The system SHALL deploy Prometheus, Grafana, and Kafdrop for metrics collection, visualization, and Kafka inspection.

#### Scenario: Prometheus collects metrics
- **WHEN** the infrastructure is running
- **THEN** Prometheus scrapes metrics from Kafka brokers (JMX exporter), PostgreSQL (postgres_exporter), and infrastructure services (Prometheus, Grafana, Kafdrop)

#### Scenario: Grafana displays dashboards
- **WHEN** Grafana starts
- **THEN** it connects to Prometheus as a data source and loads preconfigured dashboards

#### Scenario: Kafdrop shows Kafka topics
- **WHEN** Kafdrop starts
- **THEN** it displays all Kafka topics, partitions, consumers, and offsets

### Requirement: Docker Compose Profiles

The system SHALL support Docker Compose profiles to deploy different subsets of infrastructure.

#### Scenario: Full profile deploys all services
- **WHEN** `docker compose --profile full up -d` is executed
- **THEN** all infrastructure services (Kafka cluster, Schema Registry, PostgreSQL, Prometheus, Grafana, Kafdrop) start

#### Scenario: Core profile deploys only essential services
- **WHEN** `docker compose --profile core up -d` is executed
- **THEN** only Kafka cluster, Schema Registry, and PostgreSQL start (no monitoring, no microservices)

#### Scenario: Monitoring profile deploys only monitoring tools
- **WHEN** `docker compose --profile monitoring up -d` is executed
- **THEN** only Prometheus, Grafana, and Kafdrop start

#### Scenario: Load-test profile deploys test infrastructure
- **WHEN** `docker compose --profile load-test up -d` is executed
- **THEN** only Kafka cluster, Schema Registry, PostgreSQL, and monitoring services (Prometheus, Grafana, Kafdrop) start (no microservices)

### Requirement: Kafka Topics

The system SHALL create and configure 10 Kafka topics with specific types, keys, and formats.

#### Scenario: Stream topics are created
- **WHEN** infrastructure is initialized
- **THEN** topics `calls.completed`, `calls.fraud-alerts`, `transcription.raw`, `transcription.summary`, `transcription.enriched` are created as stream topics with Avro format, and `calls.dlq` is created as a stream topic with JSON format

#### Scenario: ksqlDB output topics are created
- **WHEN** ksqlDB persistent streams are created (ksqldb-analytics-layer change)
- **THEN** topics `calls.completed.agg` (Avro, key=agentId, partitions=6, replication=3) and `calls.fraud-alerts.agg` (Avro, key=phone:pattern:severity, partitions=6, replication=3) are created with correct schema and partitioning

#### Scenario: Compacted topics are created
- **WHEN** infrastructure is initialized
- **THEN** topics `calls.metadata` and `customers.profile` are created as compacted topics with Avro format

#### Scenario: All topics have consistent partitioning
- **WHEN** infrastructure is initialized
- **THEN** all 10 topics have 6 partitions and replication factor 3 (matching infrastructure standard)

### Requirement: Health Checks

The system SHALL provide health check endpoints for all services to support orchestration and monitoring.

#### Scenario: Kafka broker health
- **WHEN** Kafka broker is running
- **THEN** it responds to health check commands (e.g., `kafka-broker-api-versions.sh`)

#### Scenario: PostgreSQL health
- **WHEN** PostgreSQL is running
- **THEN** it accepts connections on port 5432 and responds to simple queries

### Requirement: Network Isolation

The system SHALL use a dedicated Docker network for all platform services.

#### Scenario: All services connect to platform network
- **WHEN** Docker Compose is executed
- **THEN** all services are connected to the `call-platform-net` bridge network

#### Scenario: Services communicate via DNS
- **WHEN** a service needs to reach another service
- **THEN** it can use container names as hostnames (e.g., `kafka-1:9092`, `postgres:5432`)

### Requirement: Persistent Volumes

The system SHALL use Docker volumes for all persistent data to survive container restarts.

#### Scenario: Kafka data persists across restarts
- **WHEN** a Kafka broker container is restarted
- **THEN** its data is restored from the `kafka-N-data` volume

#### Scenario: PostgreSQL data persists across restarts
- **WHEN** the PostgreSQL container is restarted
- **THEN** all data is restored from the `postgres-data` volume
