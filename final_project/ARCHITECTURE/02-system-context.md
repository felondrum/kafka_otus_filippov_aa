# 2. System Context (C4 Level 1)

## 2.1. Описание

Система обрабатывает звонки банковского колл-центра в реальном времени. Внешние акторы взаимодействуют с системой через REST API или загружают данные администрирования.

## 2.2. ASCII-диаграмма

```
┌──────────────┐       ┌─────────────────────────────────────┐
│   Caller     │──────▶│                                     │
│  (REST API)  │  POST │         CALL PLATFORM               │
│              │       │   (Event-Driven Kafka Platform)     │
└──────────────┘       │                                     │
                       │  ┌─────────┐  ┌─────────┐          │
                       │  │ Services│  │ Storage │          │
                       │  └─────────┘  └─────────┘          │
                       │                                     │
                       └─────────────────────────────────────┘
                                ▲      │
                                │      ▼
                       ┌──────────────┐
                       │   Admin      │──────▶ PostgreSQL
                       │  (REST API)  │      (Analytics DB)
                       └──────────────┘

                       ┌──────────────┐
                       │ Monitoring   │◀──── Kafka Metrics
                       │  System      │      (Prometheus)
                       │ (Grafana)    │
                       └──────────────┘
```

## 2.3. Внешние акторы

| Актор | Роль | Взаимодействие |
|-------|------|----------------|
| **Caller (REST)** | Внешняя система колл-центра | Отправляет события о завершённых звонках через REST API |
| **Admin** | Оператор колл-центра | Загружает профили клиентов, управляет настройками, просматривает отчёты |
| **Monitoring System** | Prometheus + Grafana | Собирает метрики Kafka, JVM, приложения |

## 2.4. Внешние системы

| Система | Тип связи | Описание |
|---------|-----------|----------|
| **LLM Service (симуляция)** | In-process | Транскрибация и суммаризация диалогов (симуляция LLM в transcription-analyzer) |
| **PostgreSQL** | JDBC (Kafka Connect + direct) | Аналитическое хранилище для отчётности и архивации |
| **Schema Registry** | HTTP API | Централизованное хранение Avro-схем событий |

## 2.5. Связи

```
Caller ──[HTTPS POST /api/calls]──▶ call-processor
Admin  ──[HTTPS POST /api/admin/customers]──▶ call-processor
Admin  ──[HTTPS GET /api/reports/*]──▶ reporting-nps
call-processor ──[Kafka calls.completed]──▶ fraud-detector
call-processor ──[Kafka calls.completed]──▶ transcription-analyzer
call-processor ──[Kafka calls.metadata]──▶ reporting-nps
fraud-detector ──[Kafka calls.fraud-alerts]──▶ reporting-nps
transcription-analyzer ──[Kafka transcription.enriched]──▶ reporting-nps
transcription-analyzer ──[JDBC]──▶ PostgreSQL
Monitoring System ──[HTTP /metrics]──▶ все сервисы + Kafka
```
