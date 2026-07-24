# Домашнее задание №4: Агрегация событий по сессиям в Kafka Streams

## Описание

В этом задании реализовано приложение на базе **Kafka Streams**, которое выполняет агрегацию событий по сессиям с таймаутом в 5 секунд. Приложение считает количество событий с одинаковыми ключами в рамках каждой сессии и выводит результаты в отдельный топик.

## Функционал

Приложение выполняет следующие действия:

1. **Создание топиков:**
   - `events` — входной топик для получения событий
   - `events-aggregated` — выходной топик для результатов агрегации

2. **Потоковая обработка:**
   - Читает события из топика `events`
   - Группирует события по ключу
   - Агрегирует события в сессии с таймаутом 5 секунд (Session Windows)
   - Считает количество событий в каждой сессии
   - Записывает результаты в топик `events-aggregated`

3. **Генерация тестовых данных:**
   - Создаёт 50 событий для 5 разных ключей (user1-user5)
   - Каждому ключу соответствует по 10 событий
   - Использует библиотеку JavaFaker для генерации случайных значений

## Архитектура решения

### Сессионные окна (Session Windows)

Используется `SessionWindows.ofInactivityGapWithNoGrace(5 seconds)`, что означает:
- Все события с одинаковым ключом, поступившие в течение 5 секунд, объединяются в одну сессию
- Если в течение 5 секунд новых событий не поступает, сессия закрывается и агрегируется
- Новые события с тем же ключом создают новую сессию

### Схема обработки

```
Events Topic (events)              Event Aggregator              Aggregated Topic (events-aggregated)
─────────────────────              ───────────────────              ──────────────────────────────────

user1: event1 ──────────────────┐
                                │
user1: event2 ──────┬───────────┤Group By Key                   user1: count = 10
                                │                               (1 session, 5 events)
user1: event3 ──────┤           │
                    │           │
user2: event1 ──────┤           │
                    │           ▼
user2: event2 ──────┤       SessionWindows                    user2: count = 10
                    │       (5 sec timeout)                   (1 session, 5 events)
user3: event1 ──────┤           │
                    │           │
user3: event3 ──────┤           │                               user3: count = 10
                    │           ▼                               (1 session, 5 events)
user4: event1 ──────┤       Count()
                    │           │
user4: event2 ──────┤           │                               user4: count = 10
                    │           ▼                               (1 session, 5 events)
user5: event1 ──────┤      ToStream()
                    │           │
user5: event5 ──────┘           ▼                               user5: count = 10
                            MapValues                         (1 session, 5 events)
                                │
                                ▼
                        Events Aggregated Topic
```

## Технологии

- **Apache Kafka 4.0.0** — распределённая платформа потоковой передачи данных
- **Kafka Streams 4.3.1** — библиотека для построения stream processing приложений
- **Java 21** — язык программирования
- **Gradle** — система сборки проекта
- **SLF4J + Logback** — фреймворки для логирования
- **Project Lombok** — упрощение кода (аннотации @Data, @Builder)
- **Google Gson** — сериализация/десериализация объектов JSON
- **JavaFaker** — генерация фейковых данных для тестирования

## Структура проекта

```
hw4/
├── build.gradle.kts           # Конфигурация Gradle
├── docker-compose.yml         # Docker Compose для запуска Kafka
├── README.md                  # Документация
├── src/
│   └── main/
│       ├── java/ru/otus/filippov/
│       │   ├── Application.java                  # Точка входа в приложение
│       │   ├── EventSessionAggregator.java       # Основной класс обработки потока
│       │   ├── model/
│       │   │   ├── Event.java                    # Модель события
│       │   │   ├── EventKey.java                 # Ключ для агрегации
│       │   │   ├── EventKeySerde.java            # Serde для EventKey
│       │   │   └── EventSerde.java               # Serde для Event
│       │   ├── producer/
│       │   │   └── MockEventProducer.java        # Генератор тестовых событий
│       │   └── utils/
│       │       ├── AbstractProducer.java         # Базовый класс продюсера
│       │       └── Utils.java                    # Утилиты и конфигурация
│       └── resources/
│           └── logback.xml                       # Конфигурация логирования
└── gradle/
    └── wrapper/                                 # Gradle Wrapper
```

## Установка и запуск

### 1. Запуск Kafka через Docker Compose

```bash
cd hw4
docker-compose up -d
```

### 2. Проверка статуса контейнера

```bash
docker ps
```

Контейнер `kafka-new-hw4` должен быть в статусе `Up`.

### 3. Сборка проекта

```bash
./gradlew build
```

### 4. Запуск приложения

```bash
./gradlew run
```

### 5. Проверка результатов

После завершения работы приложения можно проверить содержимое топика агрегированных данных:

```bash
# В другом терминале, пока Kafka запущена
docker exec -it kafka-new-hw4 bash

# Запустить console consumer для выходного топика
kafka-console-consumer.sh --bootstrap-server localhost:9093 \
    --topic events-aggregated \
    --from-beginning
```

## Как это работает

### События (Event)

Событие содержит три поля:
- `key` — ключ для группировки (user1-user5)
- `value` — случайное значение, сгенерированное Faker
- `timestamp` — время создания события

### Агрегация по сессиям

1. **Group By Key**: События группируются по полю `key` из Event
2. **Session Windows**: Kafka Streams объединяет события в сессии на основе времени поступления
3. **Count**: Для каждой сессии подсчитывается количество событий
4. **Map to Result**: Преобразование Windowed<EventKey> в простую пару (key, count)

### Пример работы

Приложение генерирует 50 событий:
```
user1: 10 событий → 1 сессия → count = 10
user2: 10 событий → 1 сессия → count = 10
user3: 10 событий → 1 сессия → count = 10
user4: 10 событий → 1 сессия → count = 10
user5: 10 событий → 1 сессия → count = 10
```

Результат записывается в топик `events-aggregated`:
```
user1: 10
user2: 10
user3: 10
user4: 10
user5: 10
```

## Настройки Kafka Streams

### Сессионные окна
- **Timeout**: 5 секунд
- **Grace**: По умолчанию (без запаса времени)

### Конфигурация топиков
- **Partitions**: 1 (по умолчанию)
- **Replication Factor**: 1

### Потоковая обработка
- **Processing Thread**: Один поток по умолчанию
- **State Store**: Используется встроенное KeyValueStore

## Основные методы классов

### `EventSessionAggregator.startSession(StreamsBuilder)`

Создаёт потоковую обработку:
1. Создаёт KStream из топика `events`
2. Группирует события по ключу
3. Применяет Session Windows
4. Считает события в каждой сессии
5. Преобразует результат и записывает в выходной топик

### `MockEventProducer.sendEvents()`

Генерирует тестовые события:
1. Итерирует по 5 ключам (user1-user5)
2. Для каждого ключа создаёт 10 событий
3. Отправляет события в Kafka с небольшой задержкой

### `Utils.runEventsApp(...)`

Управляет жизненным циклом приложения:
1. Пересоздаёт топики
2. Запускает Kafka Streams
3. Запускает продюсер
4. Ждёт завершения обработки (30 секунд для сессий)
5. Корректно останавливает приложение

## Логирование

Используется уровень логирования `INFO`. Основные сообщения:

```
INFO EventSessionAggregator -- Starting event aggregation...
INFO EventSessionAggregator -- Session counts created, creating stream...
INFO Application -- App Started
INFO Application -- Starting Kafka Streams...
INFO MockEventProducer -- Sending events...
INFO MockEventProducer -- Sent event with key: user1, value: ...
INFO Application -- Waiting for session windows to close...
INFO EventSessionAggregator -- Window {key=user1, start=..., end=...}: 10
INFO Application -- Shutdown complete
```

## Используемые Serde

### `EventKeySerde`
- Сериализует/десериализует ключ агрегации
- Использует JSON через Gson

### `EventSerde`
- Сериализует/десериализует событие
- Использует JSON через Gson

## Ограничения и улучшения

### Текущие ограничения
- Используется только 1 партиция для входного топика (ограничивает параллелизм)
- Отсутствует обработка ошибок при отправке сообщений
- Жёстко заданное время ожидания (30 секунд)

### Возможные улучшения
- Увеличение количества партиций для параллельной обработки
- Добавление обработкиLate Data (опция grace period в окнах)
- Настройка через конфигурационный файл
- Добавление metrics и health checks
- Использование строгой гарантии доставки (exactly-once)

## Результат

- ✅ Kafka запущена в Docker
- ✅ Созданы входной и выходной топики
- ✅ Реализована потоковая агрегация событий по сессиям
- ✅ Все модели и Serde корректно настроены
- ✅ Генератор тестовых данных работает стабильно
- ✅ Результаты агрегации доступны в выходном топике
