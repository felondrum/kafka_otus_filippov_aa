# 7. Security Architecture

## 7.1. Аутентификация Kafka (SASL/PLAIN)

### 7.1.1. Конфигурация брокеров

```yaml
# Kafka brokers
KAFKA_SASL_ENABLED_MECHANISMS=PLAIN
KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL=PLAIN
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:SASL_PLAINTEXT,INTERNAL:SASL_PLAINTEXT,CONTROLLER:SASL_PLAINTEXT

# Пользователи (KRaft mode)
KAFKA_SUPER_USERS=User:kafka-admin
KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND=false
```

### 7.1.2. Пользователи и роли

| Пользователь | Роль | Права |
|-------------|------|-------|
| `kafka-admin` | Super User | Полный доступ ко всем ресурсам |
| `call-processor-user` | Producer | Write: calls.completed, calls.metadata |
| `fraud-detector-user` | Consumer + Producer | Read: calls.completed; Write: calls.fraud-alerts |
| `transcription-analyzer-user` | Consumer + Producer | Read: calls.completed, customers.profile; Write: transcription.raw, transcription.summary, transcription.enriched, calls.metadata |
| `reporting-nps-user` | Consumer | Read: calls.metadata, calls.fraud-alerts, transcription.enriched |
| `postgres-user` | Kafka Connect | JDBC (PostgreSQL) |

### 7.1.3. Клиентская конфигурация

```text
# call-processor
spring.kafka.producer.properties.sasl.mechanism=PLAIN
spring.kafka.producer.properties.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="call-processor-user" password="${KAFKA_PASSWORD}";
spring.kafka.producer.properties.security.protocol=SASL_PLAINTEXT

# fraud-detector
spring.kafka.consumer.properties.sasl.mechanism=PLAIN
spring.kafka.consumer.properties.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="fraud-detector-user" password="${KAFKA_PASSWORD}";
spring.kafka.consumer.properties.security.protocol=SASL_PLAINTEXT
spring.kafka.producer.properties.sasl.mechanism=PLAIN
spring.kafka.producer.properties.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="fraud-detector-user" password="${KAFKA_PASSWORD}";
spring.kafka.producer.properties.security.protocol=SASL_PLAINTEXT
```

## 7.2. ACL (Access Control Lists)

### 7.2.1. Правила ACL

```bash
# call-processor: producer
kafka-acls.sh --authorizer-properties bootstrap-server=localhost:9092 \
  --add --allow-principal User:call-processor-user \
  --producer --topic calls.completed --topic calls.metadata

# fraud-detector: consumer + producer
kafka-acls.sh --authorizer-properties bootstrap-server=localhost:9092 \
  --add --allow-principal User:fraud-detector-user \
  --consumer --topic calls.completed --group fraud-detector-group \
  --producer --topic calls.fraud-alerts

# transcription-analyzer: consumer + producer
kafka-acls.sh --authorizer-properties bootstrap-server=localhost:9092 \
  --add --allow-principal User:transcription-analyzer-user \
  --consumer --topic calls.completed --topic customers.profile --group transcription-analyzer-group \
  --producer --topic transcription.raw --topic transcription.summary --topic transcription.enriched --topic calls.metadata

# reporting-nps: consumer
kafka-acls.sh --authorizer-properties bootstrap-server=localhost:9092 \
  --add --allow-principal User:reporting-nps-user \
  --consumer --topic calls.metadata --topic calls.fraud-alerts --topic transcription.enriched --group reporting-nps-group

# DLQ: все сервисы могут писать
kafka-acls.sh --authorizer-properties bootstrap-server=localhost:9092 \
  --add --allow-principal User:call-processor-user \
  --producer --topic calls.dlq
kafka-acls.sh --authorizer-properties bootstrap-server=localhost:9092 \
  --add --allow-principal User:fraud-detector-user \
  --producer --topic calls.dlq
kafka-acls.sh --authorizer-properties bootstrap-server=localhost:9092 \
  --add --allow-principal User:transcription-analyzer-user \
  --producer --topic calls.dlq
```

### 7.2.2. Интерброкер коммуникация

```bash
# Inter-broker (KRaft mode)
kafka-acls.sh --authorizer-properties bootstrap-server=localhost:9092 \
  --add --allow-principal User:kafka-admin \
  --consumer --topic __cluster-metadata --group __cluster-metadata
```

## 7.3. Schema Registry совместимость

### 7.3.1. Конфигурация

```yaml
schema-registry:
  environment:
    - SCHEMA_REGISTRY_COMPATIBILITY_LEVEL=BACKWARD
    - SCHEMA_REGISTRY_HOST_NAME=schema-registry
    - SCHEMA_REGISTRY_LOG4J_ROOT_LOGLEVEL=INFO
```

### 7.3.2. Правила совместимости

| Свойство | Значение | Описание |
|----------|----------|----------|
| `compatibility.level` | `BACKWARD` | Новые схемы совместимы со старыми потребителями |
| `compatibility.group.level` | `TOPIC` | Совместимость на уровне топика |
| `subject.name.strategy` | `TOPIC_RECORD_VALUE` | Именование схем: `{topic}-value` |

### 7.3.3. Avro-схемы

```text
// calls.completed-value.avsc
{
  "namespace": "ru.otus.callplatform.schema",
  "type": "record",
  "name": "CallCompleted",
  "fields": [
    {"name": "callId", "type": "string"},
    {"name": "phone", "type": "string"},
    {"name": "duration", "type": "int"},
    {"name": "agentId", "type": "string"},
    {"name": "npsScore", "type": ["null", "int"], "default": null},
    {"name": "timestamp", "type": "long"},
    {"name": "correlationId", "type": "string"}
  ]
}
```

```text
// calls.fraud-alerts-value.avsc
{
  "namespace": "ru.otus.callplatform.schema",
  "type": "record",
  "name": "FraudAlert",
  "fields": [
    {"name": "alertId", "type": "string"},
    {"name": "phone", "type": "string"},
    {"name": "callId", "type": "string"},
    {"name": "alertType", "type": "string"},
    {"name": "score", "type": "double"},
    {"name": "timestamp", "type": "long"},
    {"name": "details", "type": ["null", "string"], "default": null}
  ]
}
```

## 7.4. Валидация входных данных (call-processor)

### 7.4.1. Правила валидации

| Поле | Правило | Описание |
|------|---------|----------|
| `callId` | `NOT_NULL, NOT_EMPTY, UUID format` | Уникальный идентификатор звонка |
| `phone` | `NOT_NULL, NOT_EMPTY, regex: ^\\+?[0-9]{10,15}$` | Валидный номер телефона |
| `duration` | `NOT_NULL, > 0, <= 3600` | Длительность (1 сек — 1 час) |
| `agentId` | `NOT_NULL, NOT_EMPTY` | Идентификатор агента |
| `npsScore` | `nullable, >= 1, <= 10` | NPS-оценка (1-10) |
| `correlationId` | `nullable, NOT_EMPTY` | Correlation ID для tracing |

### 7.4.2. Java-валидация

```java
public record CallRequest(
    @NotBlank @Pattern(regexp = "^[0-9a-fA-F-]{36}$") String callId,
    @NotBlank @Pattern(regexp = "^\\+?[0-9]{10,15}$") String phone,
    @Min(value = 1, message = "Duration must be > 0")
    @Max(value = 3600, message = "Duration must be <= 3600")
    Integer duration,
    @NotBlank String agentId,
    @Min(value = 1, message = "NPS must be >= 1")
    @Max(value = 10, message = "NPS must be <= 10")
    Integer npsScore,
    @Pattern(regexp = "^[0-9a-fA-F-]{36}$") String correlationId
) {}
```

## 7.5. Защита REST API (Spring Security)

### 7.5.1. Конфигурация безопасности

```java
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            .csrf(csrf -> csrf.disable())
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/api/health", "/actuator/**").permitAll()
                .requestMatchers("/api/calls").hasRole("CALLER")
                .requestMatchers("/api/admin/**").hasRole("ADMIN")
                .requestMatchers("/api/reports/**").hasAnyRole("ADMIN", "CALLER")
                .anyRequest().authenticated()
            )
            .httpBasic(Customizer.withDefaults())
            .sessionManagement(session -> session
                .sessionCreationPolicy(SessionCreationPolicy.STATELESS)
            );
        return http.build();
    }
}
```

### 7.5.2. Аутентификация

| Сервис | Логин | Пароль | Роли |
|--------|-------|--------|------|
| call-processor | caller | ${CALLER_PASSWORD} | ROLE_CALLER |
| reporting-nps | admin | ${ADMIN_PASSWORD} | ROLE_ADMIN, ROLE_CALLER |

## 7.6. Защита PostgreSQL

### 7.6.1. Пользователи БД

| Пользователь | Права |
|-------------|-------|
| `postgres` | Superuser (dev only) |
| `reporting-nps-user` | SELECT, INSERT на call_metadata, call_transcriptions |
| `transcription-analyzer-user` | INSERT на call_transcriptions |
| `kafka-connect-user` | SELECT, INSERT на все таблицы (JDBC Sink) |

### 7.6.2. Инициализация БД

```sql
-- init-db.sql
CREATE USER reporting_nps_user WITH PASSWORD 'reporting-nps-secret';
CREATE USER transcription_analyzer_user WITH PASSWORD 'transcription-analyzer-secret';
CREATE USER kafka_connect_user WITH PASSWORD 'kafka-connect-secret';

GRANT CONNECT ON DATABASE call_platform TO reporting_nps_user;
GRANT CONNECT ON DATABASE call_platform TO transcription_analyzer_user;
GRANT CONNECT ON DATABASE call_platform TO kafka_connect_user;

GRANT USAGE ON SCHEMA public TO reporting_nps_user;
GRANT USAGE ON SCHEMA public TO transcription_analyzer_user;
GRANT USAGE ON SCHEMA public TO kafka_connect_user;

GRANT SELECT, INSERT ON call_metadata TO reporting_nps_user;
GRANT SELECT, INSERT ON call_transcriptions TO reporting_nps_user;
GRANT INSERT ON call_transcriptions TO transcription_analyzer_user;
GRANT SELECT, INSERT ON call_metadata, call_transcriptions TO kafka_connect_user;
```

## 7.7. Корреляционные ID (Tracing)

### 7.7.1. Flow correlation ID

```
Caller → POST /api/calls {correlationId: "abc-123"}
    │
    ▼
call-processor (adds correlationId to Kafka headers)
    │
    ├── calls.completed (header: correlationId=abc-123)
    │       │
    │       ├── fraud-detector (preserves correlationId)
    │       │       │
    │       │       └── calls.fraud-alerts (header: correlationId=abc-123)
    │       │
    │       └── transcription-analyzer (preserves correlationId)
    │               │
    │               └── transcription.enriched (header: correlationId=abc-123)
    │
    └── calls.metadata (header: correlationId=abc-123)
```

### 7.7.2. Реализация

```java
// Producer interceptor
public class CorrelationIdInterceptor implements ProducerInterceptor<String, CallCompleted> {
    @Override
    public ProducerRecord<String, CallCompleted> onSend(ProducerRecord<String, CallCompleted> record) {
        String correlationId = MDC.get("correlationId");
        if (correlationId != null) {
            record.headers().add(new Header("correlation-id", correlationId.getBytes()));
        }
        return record;
    }
    // ...
}
```

## 7.8. Security Summary

| Аспект | Механизм |
|--------|----------|
| Kafka аутентификация | SASL/PLAIN |
| Kafka авторизация | ACL (per-user, per-topic) |
| Schema Registry | BACKWARD compatibility |
| REST API аутентификация | HTTP Basic + Spring Security |
| REST API авторизация | Role-based (CALLER, ADMIN) |
| Валидация входных данных | Bean Validation (JSR-380) |
| БД аутентификация | PostgreSQL users with passwords |
| БД авторизация | GRANT per user/table |
| Tracing | Correlation ID через весь пайплайн |
| DLQ | Изоляция битых сообщений |
