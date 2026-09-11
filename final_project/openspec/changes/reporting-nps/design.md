## Context

Проект — событийно-ориентированная платформа обработки звонков банковского колл-центра. Текущее состояние: репозиторий пустой, архитектура описана в `ARCHITECTURE/`. Change `infrastructure` развёртывает Kafka и PostgreSQL, change `call-processor` реализует входную точку, change `fraud-detector` реализует антифрод, change `transcription-analyzer` реализует транскрипцию и enrichment. Последний шаг — reporting-nps: CQRS read side с REST API для отчётности.

## Goals / Non-Goals

**Goals:**
- Spring Boot 3 микросервис на Java 21, сборка через Gradle (Kotlin DSL)
- Kafka Consumer: чтение calls.metadata, transcription.enriched, calls.fraud-alerts
- CQRS read side: обновление PostgreSQL (call_metadata, call_transcriptions, fraud stats)
- REST API: daily reports, agent reports, metadata queries, sentiment distribution
- Database schema: call_metadata, call_transcriptions, view v_full_call_info
- Aggregations + caching (Caffeine) for fast queries
- Unit-тесты (JUnit 5, Mockito, Embedded Kafka)
- Integration-тесты (Testcontainers: Kafka + PostgreSQL)
- Docker build и deployment

**Non-Goals:**
- Real-time dashboard (Grafana dashboards are part of infrastructure)
- Write operations via REST API (все команды идут через Kafka)
- Complex OLAP queries (PostgreSQL агрегации достаточно для MVP)
- Export to CSV/PDF (future enhancement)
- WebSocket / Server-Sent Events for real-time updates

## Decisions

### Decision 1: Spring Data JPA vs Plain JDBC

**Выбор:** Spring Data JPA (with PostgreSQL).

**Альтернативы:**
- Plain JDBC (JdbcTemplate) — быстрее, но более verbose
- jOOQ — type-safe SQL, но дополнительный dependency

**Обоснование:**
- Spring Data JPA — стандарт для Spring Boot проектов
- Entity-классы автоматически маппятся на таблицы
- Repository pattern упрощает CRUD операции
- Для MVP производительность JPA достаточна (read-heavy, не write-heavy)
- В будущем можно оптимизировать до JDBC если нужно

### Decision 2: Caching — Caffeine vs Redis

**Выбор:** Caffeine (in-memory caching).

**Альтернативы:**
- Redis — distributed cache, но добавляет зависимость
- No cache — просто, но каждый запрос идёт в PostgreSQL

**Обоснование:**
- Single-instance deployment (horizontal scaling — future)
- Caffeine — fastest in-memory cache, zero external dependencies
- TTL-based eviction (5 min for metadata, 1 min for reports)
- В будущем при horizontal scaling можно перейти на Redis

### Decision 3: Database Schema — JPA auto-ddl vs Flyway

**Выбор:** JPA auto-ddl для MVP (create-drop / update), Flyway для production.

**Альтернативы:**
- Только Flyway — migration scripts, но больше boilerplate для MVP
- Только JPA auto-ddl — просто, но нет versioned migrations

**Обоснование:**
- MVP: JPA auto-ddl достаточно для быстрой разработки
- view v_full_call_info создаётся через @Query или raw SQL
- В будущем migration к Flyway при переходе на production
- init-db.sql из infrastructure change создаёт базовые таблицы

### Decision 4: Consumer Group Strategy

**Выбор:** Один consumer group "reporting-nps" с 3 @KafkaListener beans (по одному на topic).

**Альтернативы:**
- 3 отдельных consumer group — изоляция, но сложнее monitoring
- 1 @KafkaListener с 3 topics — проще, но harder to scale per-topic

**Обоснование:**
- Один consumer group упрощает monitoring и offset management
- 3 @KafkaListener beans (по одному на topic) для логической изоляции
- Каждый @KafkaListener имеет @KafkaListener(topicName = "...", groupId = "reporting-nps")
- Архитектура требует separate processing per topic

### Decision 5: Aggregation Strategy

**Выбор:** SQL aggregation (PostgreSQL GROUP BY) + caching.

**Альтернативы:**
- In-memory aggregation (Java streams) — быстрее, но не масштабируется
- Materialized views — fast queries, но сложнее maintenance

**Обоснование:**
- SQL GROUP BY — нативный механизм PostgreSQL, оптимизирован
- Caching снижает нагрузку при частых запросах
- Materialized views — future optimization если SQL aggregation слишком медленный
- Для MVP (до 10K calls/day) SQL aggregation достаточно

## Risks / Trade-offs

| Risk | Mitigation |
|------|-----------|
| JPA performance under high read load | Caching (Caffeine); connection pool tuning (HikariCP); eventual migration to read replicas |
| Caffeine cache not shared across instances | Single-instance deployment; future: switch to Redis if horizontal scaling needed |
| Kafka consumer lag under high throughput | Consumer concurrency (max.poll.records, fetch.min.bytes); monitoring lag via Prometheus |
| Database schema drift between environments | Flyway migration scripts (future); init-db.sql for local development |
| Cache invalidation complexity | Event-driven invalidation (on Kafka consume); TTL-based fallback (5 min / 1 min) |

## Migration Plan

**Шаги развёртывания:**
1. Убедиться, что infrastructure, call-processor, fraud-detector, transcription-analyzer запущены
2. Собрать JAR: `cd reporting-nps && ./gradlew clean build`
3. Запустить сервис: `docker compose up reporting-nps`
4. Проверить health: `curl http://localhost:8084/api/health`
5. Проверить REST API endpoints:
   - GET http://localhost:8084/api/reports/daily
   - GET http://localhost:8084/api/metadata/{callId}
   - GET http://localhost:8084/api/sentiment/distribution
6. Проверить PostgreSQL: `psql -h localhost -U postgres -d call_platform -c "SELECT * FROM v_full_call_info LIMIT 5;"`

**Rollback:**
- `docker compose down reporting-nps`
- Вернуть предыдущую версию JAR (если есть)
- Данные в PostgreSQL не теряются (PostgreSQL persistent volume)

## Open Questions

| # | Вопрос | Варианты | Статус |
|---|--------|----------|--------|
| O-1 | FK integrity: как handle out-of-order events (transcription before metadata)? | 1) Buffer orphan events + retry (выбрано, см. spec) 2) DEFERRABLE FK 3) Document ordering assumption | Решено: buffer + retry |
| O-2 | Fraud correlation: как correlate phone-keyed fraud alerts with callId-keyed metadata? | 1) Lookup call_metadata by callId from fraud alert body (выбрано) 2) Maintain phone→callId mapping in local state | Решено: callId lookup |
| O-3 | FraudStats storage model: pre-aggregated table или on-demand query? | 1) Pre-aggregated fraud_stats table (выбрано) 2) On-demand JOIN | Решено: pre-aggregated |
| O-4 | Cache invalidation: какие cache entries invalidate per event type? | Defined mapping in spec (metadataCache, reportCache, sentimentCache) | Решено: explicit mapping |
| O-5 | v_full_call_info usage: использовать в API endpoints или удалить? | 1) Использовать в daily/agent reports (выбрано) 2) Удалить view | Решено: использовать в API |
| O-6 | Pagination: какие default page size и max page size? | Default=50, Max=200 (рекомендуется для L-5) | Предложено: 50/200 |
