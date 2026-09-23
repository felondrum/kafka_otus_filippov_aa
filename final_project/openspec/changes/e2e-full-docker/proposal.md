## Why

Полный end-to-end pipeline не работает в Docker: reporting-nps consumer не подключён к PostgreSQL (нет fraud_stats и v_full_call_info), Kafka Connect JDBC Sink connector не настроен автоматически, нет bootstrap-скрипта для проверки полного цикла от POST /api/calls до GET /api/reports/daily.

## What Changes

- Подключение init-reporting-nps.sql к PostgreSQL (fraud_stats + v_full_call_info)
- Автоматический деплой Kafka Connect JDBC Sink connector через kafka-connect-init.sh
- Bootstrap-скрипт e2e-bootstrap.sh для проверки полного pipeline
- Исправление reporting-nps consumer на ConfluentJSONDeserializer (Avro support)
- docker-compose: зависимости, volumes, new services
- Makefile: targets e2e-setup, e2e-verify, e2e

## Impact

- Зависит от: reporting-nps, infrastructure, kafka-connect-jdbc-sink
- Docker: PostgreSQL volumes, kafka-connect-init service, e2e-bootstrap service
- Makefile: новые targets
