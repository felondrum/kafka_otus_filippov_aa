# 6. Infrastructure Architecture

## 6.1. Docker Compose структура

### 6.1.1. Базовая структура

```yaml
# docker-compose.yml (основной файл)
services:
  # Kafka Cluster (KRaft, 3 brokers)
  kafka-1:
    image: confluentinc/cp-kafka:7.6.1
    environment:
      - KAFKA_NODE_ID=1
      - KAFKA_PROCESS_ROLES=broker,controller
      - KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
      - KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:29092,INTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,INTERNAL://kafka-1:9092
      - KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      - KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3
      - KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3
      - KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2
      - KAFKA_SASL_ENABLED_MECHANISMS=PLAIN
      - KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL=PLAIN
      - KAFKA_USERNAME=kafka-user
      - KAFKA_PASSWORD=kafka-secret
    volumes:
      - kafka-1-data:/var/lib/kafka/data
    ports:
      - "9092:9092"
    networks:
      - call-platform-net
    profiles:
      - full

  kafka-2:
    image: confluentinc/cp-kafka:7.6.1
    environment:
      - KAFKA_NODE_ID=2
      - KAFKA_PROCESS_ROLES=broker,controller
      - KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
      - KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:29092,INTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9094,INTERNAL://kafka-2:9092
      - KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      - KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3
      - KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3
      - KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2
      - KAFKA_SASL_ENABLED_MECHANISMS=PLAIN
      - KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL=PLAIN
      - KAFKA_USERNAME=kafka-user
      - KAFKA_PASSWORD=kafka-secret
    volumes:
      - kafka-2-data:/var/lib/kafka/data
    ports:
      - "9094:9092"
    networks:
      - call-platform-net
    profiles:
      - full

  kafka-3:
    image: confluentinc/cp-kafka:7.6.1
    environment:
      - KAFKA_NODE_ID=3
      - KAFKA_PROCESS_ROLES=broker,controller
      - KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
      - KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:29092,INTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9096,INTERNAL://kafka-3:9092
      - KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      - KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3
      - KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3
      - KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2
      - KAFKA_SASL_ENABLED_MECHANISMS=PLAIN
      - KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL=PLAIN
      - KAFKA_USERNAME=kafka-user
      - KAFKA_PASSWORD=kafka-secret
    volumes:
      - kafka-3-data:/var/lib/kafka/data
    ports:
      - "9096:9092"
    networks:
      - call-platform-net
    profiles:
      - full

  # Schema Registry
  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.1
    environment:
      - SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://kafka-1:9092,PLAINTEXT://kafka-2:9092,PLAINTEXT://kafka-3:9092
      - SCHEMA_REGISTRY_HOST_NAME=schema-registry
      - SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
    ports:
      - "8081:8081"
    networks:
      - call-platform-net
    profiles:
      - full

  # PostgreSQL
  postgres:
    image: postgres:15.8-alpine
    environment:
      - POSTGRES_DB=call_platform
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./init-db.sql:/docker-entrypoint-initdb.d/init-db.sql
    ports:
      - "5432:5432"
    networks:
      - call-platform-net
    profiles:
      - full

  # Микросервисы
  call-processor:
    build: ./call-processor
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
      - SCHEMA_REGISTRY_URL=http://schema-registry:8081
      - SPRING_PROFILES_ACTIVE=docker
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
      - schema-registry
    ports:
      - "8081:8081"
    networks:
      - call-platform-net
    profiles:
      - full

  fraud-detector:
    build: ./fraud-detector
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
      - SPRING_PROFILES_ACTIVE=docker
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
    ports:
      - "8082:8082"
    networks:
      - call-platform-net
    profiles:
      - full

  transcription-analyzer:
    build: ./transcription-analyzer
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
      - SCHEMA_REGISTRY_URL=http://schema-registry:8081
      - SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/call_platform
      - SPRING_DATASOURCE_USERNAME=postgres
      - SPRING_DATASOURCE_PASSWORD=postgres
      - SPRING_PROFILES_ACTIVE=docker
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
      - schema-registry
      - postgres
    ports:
      - "8083:8083"
    networks:
      - call-platform-net
    profiles:
      - full

  reporting-nps:
    build: ./reporting-nps
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
      - SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/call_platform
      - SPRING_DATASOURCE_USERNAME=postgres
      - SPRING_DATASOURCE_PASSWORD=postgres
      - SPRING_PROFILES_ACTIVE=docker
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
      - postgres
    ports:
      - "8084:8084"
    networks:
      - call-platform-net
    profiles:
      - full

  # Мониторинг
  prometheus:
    image: prom/prometheus:v2.53.0
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus-data:/prometheus
    ports:
      - "9090:9090"
    networks:
      - call-platform-net
    profiles:
      - full

  grafana:
    image: grafana/grafana:11.0.0
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana-data:/var/lib/grafana
      - ./monitoring/grafana/dashboards:/etc/grafana/provisioning/dashboards
    ports:
      - "3000:3000"
    networks:
      - call-platform-net
    profiles:
      - full

  kafdrop:
    image: obsidiandynamics/kafdrop:4.1.0
    environment:
      - KAFKA_BROKERCONNECT=kafka-1:9092,kafka-2:9092,kafka-3:9092
      - JVM_OPTS=-Xms32m -Xmx128m
    ports:
      - "9000:9000"
    networks:
      - call-platform-net
    profiles:
      - full
```

### 6.1.2. Profiles для разных сценариев

| Profile | Состав | Назначение |
|---------|--------|------------|
| `full` | Все сервисы + мониторинг | Полный стек для разработки |
| `core` | Kafka + PostgreSQL + 4 микросервиса | Базовая функциональность без мониторинга |
| `monitoring` | Prometheus + Grafana + Kafdrop | Только инструменты мониторинга |
| `load-test` | Kafka + call-processor + monitoring | Для нагрузочного тестирования |

## 6.2. Ресурсы (CPU/RAM на контейнер)

| Компонент | Memory | CPU | Образ (ARM64) |
|-----------|--------|-----|---------------|
| Kafka (3 шт) | 1.5 GB | 1.5 vCPU | `confluentinc/cp-kafka:7.6.1` |
| call-processor | 1 GB | 1 vCPU | `openjdk:21-slim` |
| fraud-detector | 2 GB | 1.5 vCPU | `openjdk:21-slim` |
| transcription-analyzer | 2 GB | 1.5 vCPU | `openjdk:21-slim` |
| reporting-nps | 1 GB | 1 vCPU | `openjdk:21-slim` |
| PostgreSQL | 2 GB | 1 vCPU | `postgres:15.8-alpine` |
| Schema Registry | 0.5 GB | 0.5 vCPU | `confluentinc/cp-schema-registry:7.6.1` |
| Prometheus | 0.5 GB | 0.5 vCPU | `prom/prometheus:v2.53.0` |
| Grafana | 0.5 GB | 0.5 vCPU | `grafana/grafana:11.0.0` |
| Kafdrop | 0.25 GB | 0.25 vCPU | `obsidiandynamics/kafdrop:4.1.0` |
| **Итого** | **~14 GB** | **~10 vCPU** | |

## 6.3. Сеть

```
┌─────────────────────────────────────────────────────────────┐
│              Docker Network: call-platform-net               │
│                     (bridge mode)                            │
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │ kafka-1  │  │ kafka-2  │  │ kafka-3  │                  │
│  │ :9092    │  │ :9094    │  │ :9096    │                  │
│  └──────────┘  └──────────┘  └──────────┘                  │
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │schema-   │  │ postgres │  │ call-    │                  │
│  │registry  │  │          │  │processor │                  │
│  │ :8081    │  │ :5432    │  │ :8081    │                  │
│  └──────────┘  └──────────┘  └──────────┘                  │
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │fraud-   │  │transcrip-│  │reporting-│                  │
│  │detector  │  │tion-    │  │nps       │                  │
│  │ :8082    │  │analyzer  │  │ :8084    │                  │
│  │          │  │ :8083    │  │          │                  │
│  └──────────┘  └──────────┘  └──────────┘                  │
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │prometheus│  │ grafana  │  │ kafdrop  │                  │
│  │ :9090    │  │ :3000    │  │ :9000    │                  │
│  └──────────┘  └──────────┘  └──────────┘                  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

- Все контейнеры подключены к `call-platform-net`
- DNS resolution по имени контейнера (kafka-1, postgres, etc.)
- Внешний доступ через маппинг портов на `localhost`

## 6.4. Тома (Volumes)

| Volume | Путь внутри контейнера | Назначение |
|--------|----------------------|------------|
| `kafka-1-data` | `/var/lib/kafka/data` | Данные брокера Kafka 1 |
| `kafka-2-data` | `/var/lib/kafka/data` | Данные брокера Kafka 2 |
| `kafka-3-data` | `/var/lib/kafka/data` | Данные брокера Kafka 3 |
| `postgres-data` | `/var/lib/postgresql/data` | Данные PostgreSQL |
| `prometheus-data` | `/prometheus` | Данные Prometheus |
| `grafana-data` | `/var/lib/grafana` | Конфигурация Grafana |

**Оптимизация RocksDB:**

```yaml
fraud-detector:
  volumes:
    - rocksdb-data:/tmp/kafka-streams
  tmpfs:
    - /tmp/kafka-streams:fsize=104857600  # 100MB tmpfs для RocksDB
```

## 6.5. Оптимизация для Mac Apple Silicon (M1)

- Использовать только ARM64 образы (не эмулировать x86)
- Отключать неиспользуемые сервисы через profiles
- Использовать `tmpfs` для временных данных (ускорение RocksDB)
- Настроить Docker Compose profiles для разных сценариев
