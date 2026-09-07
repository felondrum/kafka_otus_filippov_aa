# Домашнее задание №5: Kafka Connect + Debezium PostgreSQL CDC

## Описание

В этом задании разворачивается **Kafka Connect** с **Debezium PostgreSQL CDC Source Connector** для репликации изменений из PostgreSQL в Kafka в реальном времени.

## Цель

Научиться настраивать интеграцию между PostgreSQL и Kafka с помощью Debezium для capture data changes (CDC — Change Data Capture).

## Архитектура решения

```
┌──────────────┐     CDC events        ┌──────────────────┐     Kafka records     ┌─────────────────┐
│              │  ──────────────────>  │                  │  ──────────────────>  │                 │
│  PostgreSQL  │                       │  Kafka Connect   │                       │  Kafka Topic    │
│  (users)     │  INSERT/UPDATE/DELETE │  + Debezium      │                       │  (otusdb.       │
│              │                       │  Connector       │                       │   public.users) │
└──────────────┘                       └──────────────────┘                       └─────────────────┘
```

### Как работает Debezium CDC

Debezium отслеживает WAL (Write-Ahead Log) PostgreSQL и фиксирует все изменения данных:
- **INSERT** → сообщение с `op: "c"` (create)
- **UPDATE** → сообщение с `op: "u"` (update)
- **DELETE** → сообщение с `op: "d"` (delete)
- **Initial Snapshot** → сообщение с `op: "r"` (read) при первом подключении

## Функционал

Инфраструктура выполняет следующие действия:

1. **Запуск Kafka** — KRaft режим (broker + controller), порт 9092 (внутренний), 9093 (внешний)
2. **Запуск PostgreSQL** — база данных `otus`, `wal_level=logical` (требование Debezium)
3. **Создание тестовой таблицы** — `users` с полями: id, name, email, age, created_at
4. **Настройка Debezium CDC Source Connector** — мониторинг таблицы `public.users`, плагин `pgoutput`
5. **Запуск Kafka Connect** — distributed mode (с storage topics)
6. **Tестирование CDC**:
   - Начальный снапшот существующих записей
   - Вставка новых записей (INSERT)
   - Обновление записей (UPDATE)
   - Удаление записей (DELETE)
7. **Проверка** — все изменения появляются в Kafka топике

## Технологии

- **Apache Kafka 4.0.0** — распределённая платформа потоковой передачи данных
- **Kafka Connect (distributed)** — фреймворк для интеграции с внешними системами
- **Debezium Connector for PostgreSQL 2.5.5** — CDC connector для PostgreSQL
- **PostgreSQL 16** — реляционная база данных
- **Kafdrop 4.0.1** — веб-интерфейс для визуализации Kafka
- **Docker Compose** — оркестрация контейнеров

## Структура проекта

```
hw5/
├── docker-compose.yml           # Docker Compose для запуска инфраструктуры
├── connect-config.json          # Конфигурация Debezium PostgreSQL CDC Connector
├── register-connector.sh        # Скрипт регистрации connector через REST API
├── setup-db.sh                  # Скрипт создания тестовой таблицы в PostgreSQL
├── test-all.sh                  # Главный тестовый скрипт (с автоматической очисткой)
└── README.md                    # Документация
```

## Установка и запуск

### 1. Запуск инфраструктуры

```bash
cd hw5
docker-compose up -d
```

Запустит 3 контейнера:
- `kafka-new-hw5` — Kafka KRaft (broker + controller)
- `postgres-hw5` — PostgreSQL 16 (wal_level=logical)
- `kafka-connect-hw5` — Kafka Connect (distributed mode) с Debezium PostgreSQL connector

### 2. Проверка статуса контейнеров

```bash
docker ps
```

Все контейнеры должны быть в статусе `Up`.

### 3. Настройка PostgreSQL

```bash
bash setup-db.sh
```

Создаст таблицу `users` и заполнит её 5 тестовыми записями.

### 4. Регистрация Debezium Connector

```bash
bash register-connector.sh
```

Зарегистрирует Debezium PostgreSQL CDC Source Connector через Kafka Connect REST API.

### 5. Тестирование (автоматический сценарий)

```bash
bash test-all.sh
```

Выполнит полный цикл тестирования с автоматической очисткой:
1. **Очистка** — удаление предыдущих connector'ов, топиков, таблиц
2. Проверка запущенных контейнеров
3. Настройка PostgreSQL
4. Регистрация connector
5. Ожидание начального снапшота
6. Тестирование INSERT (вставка)
7. Тестирование UPDATE (обновление)
8. Тестирование DELETE (удаление)
9. Итоговый отчёт с PASS/FAIL

### 6. Ручная проверка через console-consumer

```bash
# В другом терминале
docker exec -it kafka-new-hw5 bash

# Чтение из топика (внутренний порт 9092)
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic otusdb.public.users \
    --from-beginning
```

### 7. Ручное тестирование изменений

```bash
# Вставка новой записи
docker exec -e PGPASSWORD=root postgres-hw5 psql -U root -d otus -c \
    "INSERT INTO users (name, email, age) VALUES ('Test User', 'test@example.com', 20);"

# Обновление записи
docker exec -e PGPASSWORD=root postgres-hw5 psql -U root -d otus -c \
    "UPDATE users SET age = 21 WHERE name = 'Test User';"

# Удаление записи
docker exec -e PGPASSWORD=root postgres-hw5 psql -U root -d otus -c \
    "DELETE FROM users WHERE name = 'Test User';"
```

Каждое изменение появится в Kafka топике в виде CDC-сообщения.

### 8. Kafdrop — визуализация Kafka

Kafdrop доступен по адресу: http://localhost:9000

Позволяет просматривать топики, сообщения, consumer groups.

### 9. Остановка

```bash
docker-compose down
```

## Конфигурация Debezium Connector

Файл `connect-config.json`:

| Параметр | Значение | Описание |
|----------|----------|----------|
| `connector.class` | `io.debezium.connector.postgresql.PostgresConnector` | Класс connector'а |
| `database.hostname` | `postgres` | Имя хоста PostgreSQL в Docker сети |
| `database.port` | `5432` | Порт PostgreSQL |
| `database.user` | `root` | Пользователь PostgreSQL |
| `database.password` | `root` | Пароль PostgreSQL |
| `database.dbname` | `otus` | Имя базы данных |
| `database.server.name` | `otusdb` | Уникальное имя сервера (префикс топика) |
| `database.server.id` | `184054` | Уникальный ID для replication slot |
| `topic.prefix` | `otusdb` | Префикс для Kafka топиков |
| `schema.include.list` | `public` | Мониторящиеся схемы |
| `table.include.list` | `public.users` | Мониторящиеся таблицы |
| `snapshot.mode` | `initial` | Режим снапшота (только начальный) |
| `plugin.name` | `pgoutput` | Плагин replication (PostgreSQL 10+) |
| `slot.name` | `debezium_slot` | PostgreSQL replication slot |
| `heartbeat.interval.ms` | `10000` | Интервал heartbeat сообщений |

## Примеры CDC-сообщений

### Начальный снапшот (op: "r")
```json
{
  "before": null,
  "after": {
    "id": 1,
    "name": "Ivan Ivanov",
    "email": "ivan@example.com",
    "age": 25,
    "created_at": "2025-01-15T10:30:00"
  },
  "op": "r",
  "ts_ms": 1705312200000
}
```

### Вставка (op: "c")
```json
{
  "before": null,
  "after": {
    "id": 6,
    "name": "Olga Novikova",
    "email": "olga@example.com",
    "age": 27,
    "created_at": "2025-01-15T10:35:00"
  },
  "op": "c",
  "ts_ms": 1705312500000
}
```

### Обновление (op: "u")
```json
{
  "before": {
    "id": 1,
    "name": "Ivan Ivanov",
    "email": "ivan@example.com",
    "age": 25,
    "created_at": "2025-01-15T10:30:00"
  },
  "after": {
    "id": 1,
    "name": "Ivan Ivanov",
    "email": "ivan_new@example.com",
    "age": 26,
    "created_at": "2025-01-15T10:30:00"
  },
  "op": "u",
  "ts_ms": 1705312800000
}
```

### Удаление (op: "d")
```json
{
  "before": {
    "id": 5,
    "name": "Dmitry Volkov",
    "email": "dmitry@example.com",
    "age": 35,
    "created_at": "2025-01-15T10:30:00"
  },
  "after": null,
  "op": "d",
  "ts_ms": 1705313100000
}
```

## Управление connector через REST API

### Список connector'ов
```bash
curl -s http://localhost:8083/connectors | python3 -m json.tool
```

### Статус connector'а
```bash
curl -s http://localhost:8083/connectors/postgres-connector/status | python3 -m json.tool
```

### Перезапуск connector'а
```bash
curl -s -X PUT http://localhost:8083/connectors/postgres-connector/restart?force=true
```

### Удаление connector'а
```bash
curl -s -X DELETE http://localhost:8083/connectors/postgres-connector
```

## Список Kafka топиков

После запуска connector создаются следующие топики:
- `otusdb.public.users` — CDC-сообщения для таблицы `public.users`
- `connect-configs` — конфигурация connector'ов (автоматически)
- `connect-offsets` — оффсеты connector'ов (автоматически)
- `connect-status` — статус connector'ов (автоматически)

Просмотр топиков:
```bash
docker exec -it kafka-new-hw5 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --list
```

## Результат

- ✅ Kafka запущена в Docker (KRaft режим)
- ✅ PostgreSQL запущен с `wal_level=logical`
- ✅ Таблица `users` создана с тестовыми данными
- ✅ Debezium PostgreSQL CDC Connector зарегистрирован и работает (plugin: pgoutput)
- ✅ Kafka Connect запущен в distributed mode
- ✅ Kafdrop доступен для визуализации (http://localhost:9000)
- ✅ Начальный снапшот записей доступен в Kafka
- ✅ INSERT-операции реплицируются в Kafka (`op: "c"`)
- ✅ UPDATE-операции реплицируются в Kafka (`op: "u"`)
- ✅ DELETE-операции реплицируются в Kafka (`op: "d"`)
- ✅ Тестовый скрипт `test-all.sh` проверяет весь функционал
