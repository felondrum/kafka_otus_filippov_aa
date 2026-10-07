## Context

Проект — событийно-ориентированная платформа обработки звонков банковского колл-центра. Текущее состояние: репозиторий пустой, архитектура описана в `ARCHITECTURE/`. Change `infrastructure` развёртывает Kafka и PostgreSQL, change `call-processor` реализует входную точку, change `fraud-detector` реализует антифрод. Следующий шаг — transcription-analyzer: самый сложный сервис (Producer + Streams + Consumer + JDBC dual-write).

## Goals / Non-Goals

**Goals:**
- Spring Boot 3 микросервис на Java 21, сборка через Gradle (Kotlin DSL)
- Producer: генерация синтетического текста транскрипции (фиктивный текст на основе duration), отправка в transcription.raw
- Kafka Streams: чтение transcription.raw, broadcast enrichment с customers.profile
- Synthetic summary generation: problem, solution, sentiment, urgency, confidence extraction через keyword matching (фиктивная, без реальной LLM)
- Dual-write: Kafka (transcription.enriched) + JDBC (PostgreSQL call_transcriptions)
- Metadata lifecycle: TRANSCRIBING → SUMMARIZING → COMPLETED (PENDING устанавливается call-processor)
- Обновление calls.metadata топика
- Keyword extraction из текста транскрипции
- Unit-тесты (JUnit 5, Mockito, Embedded Kafka)
- Integration-тесты (Testcontainers: Kafka + PostgreSQL)
- Docker build и deployment

**Non-Goals:**
- Реальная LLM интеграция и ML модели (только keyword matching)
- Реальная аудио-обработка (текст генерируется синтетически, placeholder)
- Real-time transcription streaming (batch processing per call)

## Decisions

### Decision 1: Synthetic Summary Generation Approach

**Выбор:** Keyword-based extraction (эвристики на основе регулярных выражений). Полностью фиктивная генерация — без реальной LLM.

**Альтернативы:**
- Реальная LLM API (OpenAI, YandexGPT) — точнее, но зависимость от внешнего сервиса, cost, latency
- Local ML model (BERT) — точнее keyword-based, но сложнее развёртывание, больше ресурсов

**Обоснование:**
- MVP: keyword-based достаточно для демонстрации pipeline
- Нет зависимости от внешнего API (работает offline)
- Предсказуемая latency (< 100ms на звонок)
- Легко тестировать и отлаживать
- Текстовая генерация полностью синтетическая — нет реального разговора, текст placeholder

### Decision 2: Dual-Write vs Kafka Connect Only

**Выбор:** Ручной JDBC writer + Kafka Connect JDBC Sink (дублирование для надёжности).

**Альтернативы:**
- Только Kafka Connect JDBC Sink — проще, но меньше контроля над транзакциями
- Только ручной JDBC writer — проще код, но нет Kafka event для reporting-nps

**Обоснование:**
- Архитектура требует dual-write (см. `ARCHITECTURE/04-data-flow.md`)
- Kafka enriched event нужен для reporting-nps real-time processing
- JDBC writer нужен для per-call archival в PostgreSQL
- Дублирование повышает надёжность (если один путь падает, другой работает)

### Decision 3: Synthetic Transcription Text Generation

**Выбор:** Полностью фиктивная генерация placeholder текста на основе duration. Без аудио, без STT.

**Альтернативы:**
- Реальная транскрипция (WebRTC + STT) — сложно, требует аудио-пайплайна
- Pre-defined templates — более реалистично, но фиксированный набор сценариев

**Обоснование:**
- MVP: placeholder текст достаточен для тестирования pipeline
- Длина текста пропорциональна duration (150 слов/мин)
- Текстовая генерация полностью синтетическая — нет реального аудио, только placeholder
- В будущем можно подключить реальную STT-систему без изменения downstream

### Decision 4: Broadcast Enrichment vs KTable Join

**Выбор:** Broadcast customers.profile в каждый event.

**Альтернативы:**
- Kafka Streams KTable join — требует matching keys (callId ≠ phone), нужен repartition
- External lookup (Redis/PostgreSQL) — добавляет зависимость, сложнее

**Обоснование:**
- transcription.summary key=callId, customers.profile key=phone — keys не совпадают
- KTable join потребовал бы repartition topic (key transformation callId → phone)
- Broadcast проще: customers.profile — маленькая таблица (< 1M записей), помещается в память
- customers.profile — compacted topic, идеально для broadcast (последний статус по каждому phone)
- Обработка missing profile gracefully (default values)

### Decision 5: Metadata Status Transitions

**Выбор:** Синхронные статус-переходы (каждый шаг блокирует следующий до успеха).

**Альтернативы:**
- Async статусы (каждый шаг независим) — выше throughput, но сложнее трейсинг
- Event-driven статусы (каждый шаг публикует событие статуса) — более EDA, но больше топиков

**Обоснование:**
- Синхронные переходы проще для реализации и отладки
- calls.metadata — compacted topic, latest status wins
- Lifecycle: TRANSCRIBING → SUMMARIZING → COMPLETED (последовательный, PENDING ставится call-processor)
- reporting-nps читает metadata для отображения текущего статуса

### Decision 6: Self-Referential Topology

**Выбор:** Сервис produces и consumes собственные topics (transcription.raw, transcription.summary, transcription.enriched).

**Альтернативы:**
- Разделить на два сервиса (Producer + Streams) — больше сервисов, сложнее deployment
- Использовать separate consumer groups — требует extra configuration

**Обоснование:**
- MVP: один сервис проще в deployment и debugging
- Consumer groups изолированы: TranscriptionProducer (no group), SummaryConsumer (group: summary-processor), EnrichmentConsumer (group: enrichment-processor)
- Каждый consumer group читает только свои topics, не читает собственные output events
- Topic creation order: transcription.raw → transcription.summary → transcription.enriched (на startup)

### Decision 7: customers.profile Data Source

**Выбор:** Bootstrap CSV script через kafka-console-producer для MVP.

**Альтернативы:**
- Admin REST API (новый сервис) — real-time, но требует новый change
- call-processor REST endpoint — удобно, но расширяет scope call-processor
- Manual kafka-console-producer — работает, но неудобно для bulk data

**Обоснование:**
- MVP: customers.profile — small table (< 1M records), bootstrap один раз
- CSV script: `cat customers.csv | kafka-console-producer --topic customers.profile` — 0 кода
- В production: admin REST API или event-driven update из CRM
- customers.profile — compacted topic, consumer получает последнее значение по каждому phone

### Decision 8: Broadcast Refresh Strategy

**Выбор:** Event-driven через Kafka consumer customers.profile.

**Альтернативы:**
- Periodic poll (e.g., every 5 min) — проще, но eventual consistency
- Event-driven (Kafka consumer) — real-time, 1 listener method

**Обоснование:**
- customers.profile — compacted topic, Kafka consumer гарантирует delivery
- In-memory ConcurrentHashMap: O(1) lookup per event
- Event-driven = real-time consistency, нет stale data
- Код: 1 @KafkaListener method + ConcurrentHashMap.putIfAbsent

### Decision 9: Keyword Dictionary Storage

**Выбор:** application.yml через Spring Boot @Value list.

**Альтернативы:**
- Hardcode в коде — просто, но требует recompile при изменении
- application.yml — конфигурация без recompilation
- Lazy loading из файла — динамическое обновление, но сложнее

**Обоснование:**
- application.yml: `transcription.keywords: [card, loan, fraud, ...]` — 3 строки кода
- 500 keywords = ~30KB YAML, ~50KB в памяти — negligible
- @Value("${transcription.keywords}") — стандартный Spring Boot паттерн
- В production: можно перейти на lazy loading или external config server

### Decision 10: Consumer Group Naming

**Выбор:** `summary-processor`, `enrichment-processor`.

**Альтернативы:**
- Service-level groups (transcription-analyzer-summary, transcription-analyzer-enrichment) — длинные
- Short names (summary, enrichment) — могут конфликтовать с другими сервисами
- Unique names (summary-processor, enrichment-processor) — уникальные, понятные

**Обоснование:**
- summary-processor: consumer group для transcription.raw → transcription.summary
- enrichment-processor: consumer group для customers.profile broadcast
- Уникальные, не конфликтуют с call-processor, fraud-detector, reporting-nps
- Consumer group name = spring.kafka.consumer.group-id в application.yml

## Risks / Trade-offs

| Risk | Mitigation |
|------|-----------|
| Synthetic summary generation неточен (keyword-based) | keyword dictionary configurable via application.yml; tuning для production; future: real LLM API (если понадобится) |
| Dual-write может привести к inconsistency | Retry на уровне JDBC; DLQ для failed writes; manual reconciliation script (future) |
| Transcription text generation синтетический | Placeholder текст достаточен для pipeline testing; future: real STT integration (если понадобится) |
| Broadcast customers.profile увеличивает размер event | customers.profile small table (< 1M records); event size increase < 1KB; acceptable for MVP |
| PostgreSQL JDBC connection pool exhausted under load | HikariCP connection pool configured (max=20, minIdle=5); monitoring pool metrics |
| Self-referential topology consumer group conflicts | Separate consumer groups per stage; verify no self-consumption in integration tests |

## Migration Plan

**Шаги развёртывания:**
1. Убедиться, что infrastructure, call-processor и fraud-detector запущены
2. Собрать JAR: `cd transcription-analyzer && ./gradlew clean build`
3. Запустить сервис: `docker compose up transcription-analyzer`
4. Отправить тестовый звонок через POST /api/calls
5. Проверить transcription.raw в Kafdrop
6. Проверить transcription.summary в Kafdrop
7. Проверить transcription.enriched в Kafdrop
8. Проверить call_transcriptions в PostgreSQL (psql или DBeaver)
9. Проверить metadata lifecycle в Kafdrop (calls.metadata topic)

**Rollback:**
- `docker compose down transcription-analyzer`
- Вернуть предыдущую версию JAR (если есть)
- Данные в PostgreSQL не теряются (PostgreSQL persistent volume)
