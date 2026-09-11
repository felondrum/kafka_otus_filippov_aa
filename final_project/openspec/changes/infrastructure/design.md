## Context

Проект — событийно-ориентированная платформа обработки звонков банковского колл-центра на базе Apache Kafka. Текущее состояние: репозиторий пустой (нет исходного кода), но есть полная архитектурная документация в папке `ARCHITECTURE/` (14 документов, C4 Level 1-2) и описание проекта в `draft.md`. Первый шаг — развёртывание инфраструктуры.

## Goals / Non-Goals

**Goals:**
- Развёртывание Kafka-кластера (3 брокера, KRaft, SASL/PLAIN)
- Развёртывание PostgreSQL 15 с инициализацией схемы
- Schema Registry для Avro-схем
- Мониторинг: Prometheus + Grafana + Kafdrop
- Docker Compose конфигурация с profiles для разных сценариев
- Makefile для управления жизненным циклом
- Health checks и зависимости между сервисами
- ARM64-совместимость (Mac Apple Silicon)

**Non-Goals:**
- Реализация микросервисов (call-processor, fraud-detector, и т.д.)
- CI/CD pipeline (описан в `ARCHITECTURE/09-cicd.md`, но не реализуется в этом change)
- Нагрузочное и хаос-тестирование (описано в `ARCHITECTURE/13-load-testing.md` и `14-chaos-testing.md`)
- Production deployment (только local development)
- Kubernetes / cloud orchestration

## Decisions

### Decision 1: Docker Compose вместо Kubernetes

**Выбор:** Docker Compose для локальной разработки.

**Альтернативы:**
- Kubernetes (minikube/kind) — избыточно для локальной разработки, сложная настройка
- Прямой запуск через brew — нет изоляции, сложно управлять зависимостями

**Обоснование:**
- Все сервисы в одном репозитории, Docker Compose — стандарт для monorepo
- ARM64-совместимые образы работают нативно на Apple Silicon
- Profiles позволяют включать/выключать компоненты без изменения конфигурации
- Простой старт: `make deploy` вместо `kubectl apply -f`

### Decision 2: Kafka KRaft вместо ZooKeeper

**Выбор:** Apache Kafka 7.6.1 в KRaft режиме (без ZooKeeper).

**Альтернативы:**
- ZooKeeper — устаревший подход, требует дополнительного сервиса

**Обоснование:**
- KRaft — новый стандарт Kafka, отпадает необходимость в ZooKeeper
- Упрощённая инфраструктура (3 брокера вместо 3+1)
- Совместимость с Confluent образов
- Архитектура уже определена в `ARCHITECTURE/01-overview.md`

### Decision 3: SASL/PLAIN для локальной разработки

**Выбор:** SASL/PLAIN аутентификация для Kafka.

**Альтернативы:**
- PLAINTEXT — нет аутентификации, небезопасно
- mTLS — сложно настроить локально, требует CA

**Обоснование:**
- Архитектура требует безопасности (см. `ARCHITECTURE/07-security.md`)
- SASL/PLAIN достаточно для локальной разработки
- Единый подход для local и production (разные credentials)
- ACL для разделения прав сервисов

### Decision 4: ARM64 образы для Apple Silicon

**Выбор:** Только ARM64-совместимые образы (без эмуляции x86).

**Альтернативы:**
- x86_64 образы с Rosetta — медленнее, сложнее отладка

**Обоснование:**
- Mac разработчиков в команде (Apple Silicon)
- ARM64 образы работают нативно, без эмуляции
- Confluent образы поддерживают linux/arm64/v8
- PostgreSQL 15 имеет официальный ARM64 образ

### Decision 5: Docker Compose Profiles

**Выбор:** 4 профиля: `full`, `core`, `monitoring`, `load-test`.

**Обоснование:**
- Разные сценарии использования требуют разного набора сервисов
- `core` — для быстрого старта без мониторинга (меньше ресурсов)
- `monitoring` — для отладки только мониторинга
- `load-test` — для нагрузочного тестирования инфраструктуры (Kafka + PostgreSQL + monitoring)
- `full` — полный стек для разработки

### Decision 6: 8 топиков с replication=3, partitions=6

**Выбор:** Все 8 топиков с репликацией 3 и 6 партициями.

**Обоснование:**
- Репликация 3 — отказоустойчивость (выдерживает потерю 1 брокера)
- 6 партиций — параллелизм для consumer groups до 6 экземпляров
- Единая конфигурация упрощает управление
- Соответствует требованиям архитектуры (`ARCHITECTURE/04-data-flow.md`)

## Risks / Trade-offs

| Risk | Mitigation |
|------|-----------|
| Высокое потребление ресурсов (~14 GB RAM) | Docker Compose profiles для отключения неиспользуемых сервисов; tmpfs для RocksDB |
| Долгий старт (Kafka + 3 брокера + PostgreSQL) | Health checks с retry; `depends_on` в docker-compose; `make deploy` с ожиданием |
| Конфликты портов при параллельных проектах | Все порты маппятся на localhost; уникальные порты для каждого брокера (9092, 9094, 9096) |
| Loss of data on container restart | Persistent volumes для Kafka и PostgreSQL; replication factor 3 |
| ARM64 образы могут быть не все доступны | Проверка образов перед стартом; fallback на x86_64 с Rosetta (только если необходимо) |

## Migration Plan

**Шаги развёртывания:**
1. `make deploy` — запуск всех сервисов через Docker Compose
2. Ожидание health checks (Kafka brokers, PostgreSQL, Schema Registry)
3. Инициализация топиков (через Topic Manager в call-processor или скрипт)
4. Проверка: `make health` — все сервисы healthy
5. Проверка: `make logs` — нет ошибок в логах

**Rollback:**
- `make down` — остановка всех сервисов
- `make clean` — удаление volumes (осторожно: потеря данных)

**Zero-downtime для инфраструктуры:**
- Не применимо для local development
- Для production: blue-green deployment через Kubernetes (будущий change)

## Open Questions

Нет — все технические решения определены в архитектурной документации.
