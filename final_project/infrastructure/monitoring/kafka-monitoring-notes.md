# Kafka Monitoring - Topics, Partitions & Throughput

## Проблема
Kafka Exporter не может подключиться к Kafka broker'ам т.к. Kafka использует SASL_PLAINTEXT аутентификацию.

## Решение
Использовать Spring Kafka метрики из микросервисов + JVM метрики из Kafka brokers.

## Что доступно

### JVM метрики Kafka brokers (через JMX exporter)
- `jvm_memory_bytes_used{job="kafka"}` - использование памяти
- `process_cpu_seconds_total{job="kafka"}` - CPU
- `jvm_gc_collection_seconds_count{job="kafka"}` - GC
- `jvm_threads_live_threads{job="kafka"}` - потоки

### Spring Kafka метрики из сервисов
- `spring_kafka_template_seconds_count{job="..."}` - Kafka Producer metrics
- `spring_kafka_listener_seconds_count{job="..."}` - Kafka Consumer metrics
- `http_server_requests_seconds_count{job="..."}` - HTTP throughput

## Топики и партиции

### Посмотреть топики через Kafdrop
- URL: http://localhost:9000
- Показать все топики, партиции, реплики

### Посмотреть consumer lag
- Kafdrop показывает lag для каждого consumer group
- Или использовать: `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe`

## Пропускная способность

### Producer throughput (из call-processor)
```promql
sum(rate(spring_kafka_template_seconds_count{job="call-processor"}[5m]))
```

### Consumer throughput (из transcription-analyzer, reporting-nps)
```promql
sum(rate(spring_kafka_listener_seconds_count{job="transcription-analyzer"}[5m]))
sum(rate(spring_kafka_listener_seconds_count{job="reporting-nps"}[5m]))
```

### Общий throughput по всем сервисам
```promql
sum by(job) (rate(spring_kafka_template_seconds_count[5m]))
sum by(job) (rate(spring_kafka_listener_seconds_count[5m]))
```

## Kafka Connect

Kafka Connect metrics доступны через `/admin/metrics` endpoint:
```promql
kafka_connect_connector_tasks_active
kafka_connect_connector_tasks_total
kafka_connect_connector_tasks_failed
```

## Следующие шаги для полноценного Kafka Exporter

1. Настроить SASL_PLAINTEXT для kafka-exporter:
   ```yaml
   environment:
     KAFKA_SERVERS: "kafka-1:9092,kafka-2:9092,kafka-3:9092"
     KAFKA_SASL_ENABLED: "true"
     KAFKA_SASL_MECHANISM: "PLAIN"
     KAFKA_SASL_JAAS_CONFIG: "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";"
   ```

2. Или добавить PLAINTEXT listener без аутентификации для internal monitoring:
   ```
   KAFKA_LISTENERS: INTERNAL://0.0.0.0:9092,MONITORING://0.0.0.0:9099,EXTERNAL://0.0.0.0:9093
   KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-1:9092,MONITORING://kafka-1:9099,EXTERNAL://localhost:9092
   ```
