## Context

reporting-nps change реализовал все 100 tasks, но полный e2e pipeline не работает в Docker из-за:
1. init-reporting-nps.sql не подключён к PostgreSQL
2. Kafka Connect JDBC Sink не настроен автоматически
3. Нет bootstrap-скрипта для проверки полного цикла
4. reporting-nps consumer использует StringDeserializer вместо ConfluentJSONDeserializer

## Decisions

### Decision 1: Avro Consumer vs JSON Consumer

**Choice:** ConfluentJSONDeserializer (Avro support)

**Rationale:**
- Production-like: call-processor и transcription-analyzer шлют Avro events
- ConfluentJSONDeserializer десериализует Avro в Map<String, Object> — тот же API что и StringDeserializer
- Не нужно генерировать Avro POJO classes для consumers
- Schema Registry integration: Avro schemas уже зарегистрированы через schema-init

**Alternatives:**
- StringDeserializer + JSON parsing — работает только с JSON events, не production-like
- Avro POJO classes — слишком сложно, требует генерации из .avsc schema

### Decision 2: Kafka Connect JDBC Sink Configuration

**Choice:** Confluent JDBC Sink с pk.mode=RecordKey, insert.mode=upsert

**Rationale:**
- Соответствует design.md kafka-connect-jdbc-sink change
- Upsert обеспечивает idempotent writes
- pk.mode=RecordKey использует Kafka record key (callId) как primary key
- tasks.max=6 соответствует 6 partition topic

**Config:**
```json
{
  "name": "kafka-connect-jdbc-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "6",
    "connection.url": "jdbc:postgresql://postgres:5432/call_platform",
    "connection.user": "kafka-connect-user",
    "connection.password": "kafka-connect-secret",
    "topics": "transcription.enriched",
    "table.name.format": "call_transcriptions",
    "pk.mode": "RecordKey",
    "pk.fields": "call_id",
    "auto.create": "false",
    "insert.mode": "upsert",
    "transforms": "extract,replace",
    "transforms.extract.type": "org.apache.kafka.connect.transforms.ExtractField$Value",
    "transforms.extract.field": "value",
    "transforms.replace.type": "org.apache.kafka.connect.transforms.ReplaceString$Value"
  }
}
```

### Decision 3: Bootstrap Script Format

**Choice:** JSON events (для простоты bootstrap)

**Rationale:**
- Bootstrap script использует curl для отправки JSON events в Kafka
- Не нужно генерировать Avro в bootstrap — достаточно JSON для тестирования
- reporting-nps consumer с ConfluentJSONDeserializer работает и с JSON, и с Avro
- Kafka topic accepts both formats (schema registry BACKWARD compatibility)

### Decision 4: Service Dependencies

**Choice:** Explicit depends_on with health checks

**Rationale:**
- PostgreSQL: healthcheck pg_isready
- Kafka Connect: depends_on postgres, kafka-1/2/3
- reporting-nps: depends_on kafka-1/2/3, postgres, kafka-init
- kafka-connect-init: depends_on kafka-connect, schema-registry

## Risks / Trade-offs

| Risk | Mitigation |
|------|-----------|
| ConfluentJSONDeserializer может не работать без Schema Registry | Schema Registry запущен до consumers, schemas зарегистрированы |
| Kafka Connect JDBC Sink может не подключиться к PG | kafka-connect-user создан в init-db.sql, healthcheck на PG |
| Bootstrap script может запуститься до готовности сервисов | Retry loop с wait (max 120s) для каждого сервиса |
| Dual-write (Kafka Connect + reporting-nps) создаст дубликаты | insert.mode=upsert + pk.mode=RecordKey обеспечивает idempotency |
