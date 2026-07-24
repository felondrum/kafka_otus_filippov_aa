# Домашнее задание №3: Транзакционная отправка сообщений в Kafka

## Описание

В этом задании реализовано приложение для демонстрации транзакционной работы с Apache Kafka. Приложение выполняет транзакционную отправку сообщений в два топика с возможностью подтверждения (commit) и отмены (abort) транзакций.

## Функционал

Приложение выполняет следующие действия:

1. **Очистка и инициализация топиков:**
   - Удаляет существующие топики `topic1` и `topic2` (если есть)
   - Создаёт новые топики с 3 партициями и replication-factor=1

2. **Транзакционная отправка сообщений:**
   - **Первая транзакция (commit):** отправляет по 5 сообщений в каждый топик и подтверждает
   - **Вторая транзакция (abort):** отправляет по 2 сообщения в каждый топик и отменяет

3. **Чтение сообщений:**
   - Читает только подтверждённые транзакции
   - Сообщения из отменённой транзакции не доступны для чтения

## Технологии

- **Apache Kafka 4.0.0** — распределённая платформа потоковой передачи данных
- **Kafka Clients 3.4.0** — Java клиент для работы с Kafka
- **Gradle** — система сборки проекта
- **SLF4J + Logback** — фреймворки для логирования

## Структура проекта

```
hw3/
├── build.gradle.kts           # Конфигурация Gradle
├── docker-compose.yml         # Docker Compose для запуска Kafka
├── README.md                  # Документация
├── src/
│   └── main/
│       ├── java/
│       │   ├── Main.java             # Точка входа в приложение
│       │   └── TransactionHomeWork.java  # Основной класс с логикой транзакций
│       └── resources/
│           └── logback.xml          # Конфигурация логирования
└── gradle/
    └── wrapper/               # Gradle Wrapper
```

## Установка и запуск

### 1. Запуск Kafka через Docker Compose

```bash
cd hw3
docker-compose up -d
```

### 2. Проверка статуса контейнера

```bash
docker ps
```

Контейнер `kafka-new` должен быть в статусе `Up`.

### 3. Сборка проекта

```bash
./gradlew build
```

### 4. Запуск приложения

```bash
./gradlew run
```

## Как это работает

### Транзакционная отправка

Kafka транзакции обеспечивают **атомарность** операций:

1. **InitProducerId** — инициализация продюсера с уникальным `transactional.id`
2. **beginTransaction** — начало транзакции
3. **Produce** — отправка сообщений (не видны для consumer'ов)
4. **commitTransaction** / **abortTransaction** — подтверждение или отмена

### Изоляция транзакций

Consumer настраивается с параметром:

> props.put("isolation.level", "read_committed");


Это гарантирует, что будут прочитаны только сообщения из **подтверждённых** транзакций. Сообщения из отменённых транзакций будут пропущены.

### Схема работы

```
Producer:                                    Consumer:
┌─────────────────────────────────┐         ┌─────────────────────────────┐
│ beginTransaction()              │         │ isolation.level=read_committed
│                                 │         │                             │
│ send(5 messages to topic1)      │         │ READS: 5 messages from TX1  │
│ send(5 messages to topic2)      │         │                         ✅  │
│                                 │         │                             │
│ commitTransaction()             │         │ SKIPS: 2 messages from TX2  │
│                         ✅      │         │ (aborted transaction)       │
├─────────────────────────────────┤         │                         ✅  │
│ beginTransaction()              │         └─────────────────────────────┘
│                                 │
│ send(2 messages to topic1)      │
│ send(2 messages to topic2)      │
│                                 │
│ abortTransaction()              │
│                         ❌      │
└─────────────────────────────────┘
```

## Основные методы класса TransactionHomeWork

### `run()`
Выполняет полный цикл работы: очистка топиков, отправка сообщений, чтение.

### `cleanupAndInitializeTopics()`
Удаляет существующие топики и создаёт новые с помощью AdminClient.

### `produceTransactions()`
Выполняет две транзакции:
1. 5 сообщений в каждый топик → commit
2. 2 сообщения в каждый топик → abort

### `consumeMessages()`
Читает и отображает только подтверждённые сообщения.

## Логирование

По умолчанию используется уровень логирования `INFO`. Все отладочные сообщения Kafka скрыты.

### Пример логов

```
11:12:51.700 [main] INFO Main -- ========================================
11:12:51.700 [main] INFO Main -- Запуск приложения транзакционной отправки в Kafka
11:12:51.701 [main] INFO Main -- Выполнение полного цикла работы с Kafka...
11:12:51.829 [main] INFO org.apache.kafka.clients.producer.KafkaProducer -- [Producer clientId=producer-transaction-home-work-1784009571701, transactionalId=transaction-home-work-1784009571701] Instantiated a transactional producer.
11:12:51.977 [kafka-producer-network-thread | producer-transaction-home-work-1784009571701] INFO org.apache.kafka.clients.producer.internals.TransactionManager -- [Producer clientId=producer-transaction-home-work-1784009571701, transactionalId=transaction-home-work-1784009571701] Discovered transaction coordinator localhost:9093 (id: 1 rack: null)
11:12:51.978 [main] INFO TransactionHomeWork -- Начало первой транзакции (commit)
11:12:52.012 [main] INFO TransactionHomeWork -- Сообщение отправлено в топик topic1 [partition: 0, offset: 0]
11:12:52.013 [main] INFO TransactionHomeWork -- Сообщение отправлено в топик topic2 [partition: 1, offset: 0]
...
11:12:52.045 [main] INFO TransactionHomeWork -- Первая транзакция подтверждена (commit)
11:12:52.045 [main] INFO TransactionHomeWork -- Начало второй транзакции (abort)
...
11:12:52.078 [main] INFO TransactionHomeWork -- Вторая транзакция отменена (abort)
11:12:52.080 [main] INFO TransactionHomeWork -- Начало чтения сообщений из топиков: topic1, topic2
11:12:52.095 [main] INFO TransactionHomeWork -- Прочитано: топик=topic1, partition=0, offset=0, ключ=null, значение=TX1-Message-1-Topic1
11:12:52.095 [main] INFO TransactionHomeWork -- Прочитано: топик=topic2, partition=1, offset=0, ключ=null, значение=TX1-Message-1-Topic2
...
11:12:52.123 [main] INFO TransactionHomeWork -- Всего прочитано: 10 сообщений
```

## Результат

- ✅ Kafka запущена в Docker Desktop
- ✅ Все методы для задания написаны в одном классе (`TransactionHomeWork`)
- ✅ Вся оркестрация выполнения в классе `Main`
- ✅ Каждый метод имеет документацию и комментарии
- ✅ Транзакции работают корректно: commit → видно, abort → не видно
