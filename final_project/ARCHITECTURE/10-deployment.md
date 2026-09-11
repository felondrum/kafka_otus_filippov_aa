# 10. Deployment Model

## 10.1. Local development

### 10.1.1. Требования к инфраструктуре

| Ресурс | Минимум | Рекомендуется |
|--------|---------|--------------|
| Память | 12 GB | 16 GB |
| CPU | 4 ядра | 6 ядер |
| Диск | 50 GB свободных | 100 GB свободных |

### 10.1.2. Быстрый старт

```bash
# 1. Клонировать репозиторий
git clone <repo-url>
cd final_project

# 2. Собрать и запустить
make all

# 3. Проверить здоровье
make health

# 4. Открыть сервисы
#    - Call Processor API:      http://localhost:8081
#    - Fraud Detector:          http://localhost:8082
#    - Transcription Analyzer:  http://localhost:8083
#    - Reporting NPS API:       http://localhost:8084
#    - Grafana:                 http://localhost:3000 (admin/admin)
#    - Kafdrop:                 http://localhost:9000
#    - Prometheus:              http://localhost:9000
```

### 10.1.3. Тестирование с curl

```bash
# Отправить звонок
curl -X POST http://localhost:8081/api/calls \
  -H "Content-Type: application/json" \
  -d '{
    "callId": "550e8400-e29b-41d4-a716-446655440000",
    "phone": "+79001234567",
    "duration": 180,
    "agentId": "agent-1",
    "npsScore": 8,
    "correlationId": "test-123"
  }'

# Проверить статус звонка
curl http://localhost:8084/api/metadata/550e8400-e29b-41d4-a716-446655440000

# Получить дневную сводку
curl http://localhost:8084/api/reports/daily

# Получить статистику по агенту
curl http://localhost:8084/api/reports/agent/agent-1

# Получить распределение тональности
curl http://localhost:8084/api/sentiment/distribution
```

### 10.1.4. Сценарий демонстрации (10 минут)

| Время | Действие |
|-------|----------|
| 0:00 | `make deploy` — запуск всех сервисов |
| 1:00 | `make health` — проверка здоровья |
| 2:00 | Открыть Kafdrop — показать топики и партиции |
| 3:00 | Открыть Grafana — показать дашборды |
| 4:00 | `curl POST /api/calls` — отправить звонок |
| 5:00 | `curl GET /api/metadata/{callId}` — показать статус |
| 6:00 | Показать Kafka lag — показать что lag < 100 |
| 7:00 | Симулировать фрод (6 звонков за 30 сек) |
| 8:00 | Проверить `calls.fraud-alerts` в Kafdrop |
| 9:00 | Показать PostgreSQL данные |
| 10:00 | `make down` — остановка |

## 10.2. Resource Allocation

### 10.2.1. Docker resources limits

```yaml
# docker-compose.yml (resources)
services:
  call-processor:
    deploy:
      resources:
        limits:
          memory: 1G
          cpus: '1'
        reservations:
          memory: 512M
          cpus: '0.5'

  fraud-detector:
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1.5'
        reservations:
          memory: 1G
          cpus: '1'

  transcription-analyzer:
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1.5'
        reservations:
          memory: 1G
          cpus: '1'

  reporting-nps:
    deploy:
      resources:
        limits:
          memory: 1G
          cpus: '1'
        reservations:
          memory: 512M
          cpus: '0.5'

  postgres:
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1'
        reservations:
          memory: 1G
          cpus: '0.5'
```

### 10.2.2. JVM options

```bash
# call-processor (1G limit)
JAVA_OPTS=-Xms256m -Xmx512m -XX:+UseG1GC

# fraud-detector (2G limit, RocksDB)
JAVA_OPTS=-Xms512m -Xmx1G -XX:+UseG1GC -XX:MaxMetaspaceSize=256m

# transcription-analyzer (2G limit)
JAVA_OPTS=-Xms512m -Xmx1G -XX:+UseG1GC -XX:MaxMetaspaceSize=256m

# reporting-nps (1G limit)
JAVA_OPTS=-Xms256m -Xmx512m -XX:+UseG1GC
```

## 10.3. Profiles для разных окружений

### 10.3.1. application-docker.yml

```yaml
# application-docker.yml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:kafka-1:9092,kafka-2:9092,kafka-3:9092}
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
      properties:
        schema.registry.url: ${SCHEMA_REGISTRY_URL:http://schema-registry:8081}
        sasl.mechanism: PLAIN
        sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka-user" password="kafka-secret";
        security.protocol: SASL_PLAINTEXT
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
      properties:
        schema.registry.url: ${SCHEMA_REGISTRY_URL:http://schema-registry:8081}
        sasl.mechanism: PLAIN
        sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka-user" password="kafka-secret";
        security.protocol: SASL_PLAINTEXT

  datasource:
    url: jdbc:postgresql://${POSTGRES_HOST:postgres}:5432/call_platform
    username: ${POSTGRES_USER:postgres}
    password: ${POSTGRES_PASSWORD:postgres}
    hikari:
      maximum-pool-size: 10
      minimum-idle: 5

  jpa:
    hibernate:
      ddl-auto: update
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
```

### 10.3.2. application-dev.yml

```yaml
# application-dev.yml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer

  datasource:
    url: jdbc:postgresql://localhost:5432/call_platform
    username: postgres
    password: postgres
    hikari:
      maximum-pool-size: 5
      minimum-idle: 2

  jpa:
    hibernate:
      ddl-auto: update
    show-sql: true
```

### 10.3.3. application-prod.yml

```yaml
# application-prod.yml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS}
    producer:
      properties:
        sasl.mechanism: PLAIN
        sasl.jaas.config: ${KAFKA_SASL_JAAS_CONFIG}
        security.protocol: SASL_PLAINTEXT
    consumer:
      properties:
        sasl.mechanism: PLAIN
        sasl.jaas.config: ${KAFKA_SASL_JAAS_CONFIG}
        security.protocol: SASL_PLAINTEXT

  datasource:
    url: jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}
    username: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}
    hikari:
      maximum-pool-size: 20
      minimum-idle: 10
      connection-timeout: 30000
      idle-timeout: 600000
      max-lifetime: 1800000

  jpa:
    hibernate:
      ddl-auto: validate
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
        jdbc:
          time_zone: UTC
```

## 10.4. Загрузка профилей

| Profile | Сценарий | docker-compose |
|---------|----------|----------------|
| `full` | Полный стек для разработки | `make deploy` |
| `core` | Без мониторинга | `make deploy-core` |
| `monitoring` | Только мониторинг | `make deploy-monitoring` |
| `load-test` | Нагрузочное тестирование | `docker compose --profile load-test up -d` |

## 10.5. Восстановление после сбоев

### 10.5.1. Восстановление Kafka Streams State Store

```bash
# Остановить сервис
docker compose stop fraud-detector

# Удалить повреждённый State Store
docker exec -it $(docker compose ps -q fraud-detector) rm -rf /tmp/kafka-streams/fraud-detector/*

# Перезапустить
docker compose start fraud-detector

# State Store восстановится из Kafka логов
# Время восстановления: ~30 сек для 1000 записей
```

### 10.5.2. Восстановление PostgreSQL

```bash
# Резервное копирование
docker exec postgres pg_dump -U postgres call_platform > backup_$(date +%Y%m%d).sql

# Восстановление
docker exec -i postgres psql -U postgres call_platform < backup_20260909.sql
```

### 10.5.3. Hard Shutdown (перезапуск Docker Desktop)

```bash
# После перезапуска Docker Desktop
docker compose up -d

# Все сервисы восстановятся автоматически:
# - Kafka: переизбрание лидера, восстановление ISR
# - Kafka Streams: восстановление State Store из changelog topics
# - PostgreSQL: восстановление из volume
# - Микросервисы: переподключение к Kafka и PostgreSQL
```

## 10.6. Масштабирование

### 10.6.1. Горизонтальное масштабирование

```yaml
# docker-compose.yml (scale)
services:
  call-processor:
    deploy:
      replicas: 3

  reporting-nps:
    deploy:
      replicas: 3
```

```bash
# Запустить 3 экземпляра call-processor
docker compose up -d --scale call-processor=3

# Запустить 3 экземпляра reporting-nps
docker compose up -d --scale reporting-nps=3
```

### 10.6.2. Вертикальное масштабирование

```yaml
# Увеличить ресурсы fraud-detector (тяжёлый Streams-обработчик)
services:
  fraud-detector:
    deploy:
      resources:
        limits:
          memory: 4G    # было 2G
          cpus: '2'     # было 1.5
```

## 10.7. Чеклист деплоя

| Шаг | Действие | Статус |
|-----|----------|--------|
| 1 | Проверить требования к инфраструктуре (RAM, CPU, Disk) | ☐ |
| 2 | Запустить `docker compose config` для валидации | ☐ |
| 3 | Запустить `make build` для сборки | ☐ |
| 4 | Запустить `make test` для тестирования | ☐ |
| 5 | Запустить `make deploy` для развёртывания | ☐ |
| 6 | Проверить `make health` — все сервисы здоровы | ☐ |
| 7 | Открыть Grafana — дашборды загружены | ☐ |
| 8 | Открыть Kafdrop — топики созданы | ☐ |
| 9 | Отправить тестовый звонок через curl | ☐ |
| 10 | Проверить статус звонка через API | ☐ |
| 11 | Проверить PostgreSQL — данные записаны | ☐ |
| 12 | Проверить Kafka lag — lag < 100 | ☐ |
