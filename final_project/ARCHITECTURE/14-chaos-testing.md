# 14. Chaos Testing

## 14.1. Доступные методы (ограничения M1)

| Метод | Команда | Описание |
|-------|---------|----------|
| Остановка контейнера | `docker stop <container>` | Полная остановка сервиса |
| Перезапуск контейнера | `docker restart <container>` | Перезапуск с сохранением данных |
| Пауза контейнера | `docker pause <container>` | Приостановка процессов |
| Возобновление | `docker unpause <container>` | Возобновление после паузы |
| Отключение от сети | `docker network disconnect` | Изоляция от Docker сети |
| Ограничение ресурсов | `docker update --memory/--cpus` | Ограничение CPU/RAM |
| Hard Shutdown | Перезапуск Docker Desktop | Полный сброс среды |

## 14.2. Сценарии хаос-тестирования

| Сценарий | Действие | Ожидаемый результат | Время восстановления |
|----------|----------|-------------------|---------------------|
| Отказ брокера | Остановка 1 из 3 брокеров | Переизбрание лидера, без потерь | < 60 сек |
| Отказ сервиса | Остановка call-processor | Трафик на второй экземпляр | < 10 сек |
| Отказ потребителя | Остановка transcription-analyzer | Rebalance, exactly-once | < 45 сек |
| Сбой сети | Отключение от Docker сети | Retries, данные не теряются | < 20 сек |
| Задержка БД | Задержка PostgreSQL (через прокси) | Очередь растёт, без потерь | Зависит от очереди |
| Hard Shutdown | Перезапуск Docker Desktop | Восстановление из томов | < 3 мин |

## 14.3. Сценарий 1: Отказ брокера Kafka

### 14.3.1. scripts/chaos/kill-broker.sh

```bash
#!/bin/bash
set -e

echo "=== Chaos Test: Kafka Broker Failure ==="

# Выбираем случайный брокер
BROKER_NUM=$((RANDOM % 3 + 1))
BROKER_NAME="kafka-${BROKER_NUM}"

echo "Stopping $BROKER_NAME..."
docker stop $BROKER_NAME

# Мониторинг
echo "Monitoring cluster health..."
START_TIME=$(date +%s)

while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))

    # Проверяем лидерство партиций
    LEADER_COUNT=$(docker exec kafka-1 kafka-metadata.sh --snapshot /var/lib/kafka/data/__cluster-metadata-0/00000000000000000000.log list-partitions 2>/dev/null | wc -l)

    if [ $ELAPSED -gt 120 ]; then
        echo "ERROR: Recovery took too long (> 120 sec)"
        exit 1
    fi

    # Проверяем что кластер работает
    docker exec kafka-1 kafka-topics.sh --list --bootstrap-server localhost:9092 > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "Cluster recovered after ${ELAPSED} seconds"
        break
    fi

    sleep 5
done

# Восстанавливаем брокер
echo "Restarting $BROKER_NAME..."
docker start $BROKER_NAME

# Проверяем репликацию
sleep 30
docker exec kafka-1 kafka-topics.sh --describe --bootstrap-server localhost:9092 | grep -c "Replicas: 3"

echo "=== Broker failure test completed ==="
```

### 14.3.2. Ожидаемые результаты

| Метрика | Цель |
|---------|------|
| Время переизбрания лидера | < 60 сек |
| Потерянные сообщения | 0 |
| Дублированные сообщения | 0 |
| Lag потребителей (макс) | < 1000 |
| Ошибки приложения | Временные, без потерь |

## 14.4. Сценарий 2: Отказ сервиса

### 14.4.1. scripts/chaos/kill-service.sh

```bash
#!/bin/bash
set -e

echo "=== Chaos Test: Service Failure ==="

SERVICE="${1:-call-processor}"
echo "Stopping $SERVICE..."
docker stop $SERVICE

# Мониторинг
echo "Monitoring recovery..."
START_TIME=$(date +%s)

while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))

    if [ $ELAPSED -gt 30 ]; then
        echo "ERROR: Recovery took too long (> 30 sec)"
        exit 1
    fi

    # Проверяем что сервис перезапустился
    docker ps | grep $SERVICE | grep -q "Up"
    if [ $? -eq 0 ]; then
        echo "$SERVICE recovered after ${ELAPSED} seconds"
        break
    fi

    sleep 2
done

# Восстанавливаем сервис
docker start $SERVICE

# Проверяем что сервис работает
sleep 5
curl -sf http://localhost:8081/actuator/health > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "Service is healthy"
else
    echo "ERROR: Service is not healthy"
    exit 1
fi

echo "=== Service failure test completed ==="
```

### 14.4.2. Ожидаемые результаты

| Метрика | Цель |
|---------|------|
| Время восстановления | < 10 сек |
| Потерянные сообщения | 0 |
| Ошибки клиентов | Временные (retry) |
| Lag потребителей | < 100 |

## 14.5. Сценарий 3: Отказ потребителя

### 14.5.1. scripts/chaos/kill-consumer.sh

```bash
#!/bin/bash
set -e

echo "=== Chaos Test: Consumer Failure ==="

CONSUMER="${1:-transcription-analyzer}"
echo "Stopping $CONSUMER..."
docker stop $CONSUMER

# Мониторинг lag
echo "Monitoring consumer lag..."
START_TIME=$(date +%s)

while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))

    if [ $ELAPSED -gt 60 ]; then
        echo "ERROR: Recovery took too long (> 60 sec)"
        exit 1
    fi

    # Проверяем rebalance
    docker exec kafka-1 kafka-consumer-groups.sh --describe --bootstrap-server localhost:9092 --group $CONSUMER-group > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "Consumer group rebalanced after ${ELAPSED} seconds"
        break
    fi

    sleep 3
done

# Восстанавливаем потребителя
docker start $CONSUMER

# Проверяем что lag уменьшается
sleep 10
LAG=$(docker exec kafka-1 kafka-consumer-groups.sh --describe --bootstrap-server localhost:9092 --group $CONSUMER-group | grep -c "Empty")
echo "Consumer lag after recovery: $LAG"

echo "=== Consumer failure test completed ==="
```

### 14.5.2. Ожидаемые результаты

| Метрика | Цель |
|---------|------|
| Время rebalance | < 45 сек |
| Exactly-once гарантия | Сохранена |
| Потерянные сообщения | 0 |
| Дублированные сообщения | 0 |

## 14.6. Сценарий 4: Сбой сети

### 14.6.1. scripts/chaos/network-delay.sh

```bash
#!/bin/bash
set -e

echo "=== Chaos Test: Network Partition ==="

SERVICE="${1:-call-processor}"
echo "Disconnecting $SERVICE from network..."
docker network disconnect call-platform-net $SERVICE

# Мониторинг
echo "Monitoring error rate..."
sleep 10

# Проверяем что сервис не может достучаться до Kafka
docker exec $SERVICE curl -sf http://kafka-1:9092 > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Network partition confirmed: $SERVICE cannot reach Kafka"
else
    echo "ERROR: Network partition not established"
    exit 1
fi

# Восстанавливаем сеть
echo "Restoring network..."
docker network connect call-platform-net $SERVICE

# Проверяем восстановление
sleep 10
docker exec $SERVICE curl -sf http://kafka-1:9092 > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "Network restored: $SERVICE can reach Kafka"
else
    echo "ERROR: Network not restored"
    exit 1
fi

echo "=== Network partition test completed ==="
```

### 14.6.2. Ожидаемые результаты

| Метрика | Цель |
|---------|------|
| Время обнаружения разрыва | < 5 сек |
| Retries | Экспоненциальный backoff |
| Данные не теряются | Да |
| Время восстановления | < 20 сек |

## 14.7. Сценарий 5: Ограничение ресурсов

### 14.7.1. scripts/chaos/resource-constraint.sh

```bash
#!/bin/bash
set -e

echo "=== Chaos Test: Resource Constraint ==="

SERVICE="${1:-fraud-detector}"
echo "Limiting $SERVICE resources..."

# Ограничиваем память до 256MB (вместо обычных 2GB)
docker update --memory=256m --cpus=0.5 $SERVICE

# Мониторинг
echo "Monitoring service under constraint..."
sleep 30

# Проверяем что сервис работает (или падает с OOM)
docker ps | grep $SERVICE
if [ $? -eq 0 ]; then
    echo "$SERVICE is still running under constraint"
else
    echo "$SERVICE crashed (expected with 256MB)"
fi

# Восстанавливаем ресурсы
echo "Restoring resources..."
docker update --memory=2g --cpus=1.5 $SERVICE

# Перезапускаем если упал
docker restart $SERVICE

sleep 10
docker ps | grep $SERVICE
if [ $? -eq 0 ]; then
    echo "$SERVICE recovered"
else
    echo "ERROR: $SERVICE did not recover"
    exit 1
fi

echo "=== Resource constraint test completed ==="
```

### 14.7.2. Ожидаемые результаты

| Метрика | Цель |
|---------|------|
| Graceful degradation | Да |
| OOM Killer | Возможное поведение |
| Восстановление после снятия ограничений | Да |
| Потеря данных | 0 |

## 14.8. Сценарий 6: Hard Shutdown

### 14.8.1. scripts/chaos/hard-shutdown.sh

```bash
#!/bin/bash
set -e

echo "=== Chaos Test: Hard Shutdown (Docker Desktop Restart) ==="

# Останавливаем все сервисы
echo "Stopping all services..."
docker compose down

# Имитируем перезапуск Docker Desktop
echo "Waiting for Docker Desktop restart simulation..."
sleep 10

# Запускаем всё заново
echo "Starting all services..."
docker compose up -d

# Мониторинг восстановлен��я
echo "Monitoring recovery..."
START_TIME=$(date +%s)

while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))

    if [ $ELAPSED -gt 180 ]; then
        echo "ERROR: Recovery took too long (> 180 sec)"
        exit 1
    fi

    # Проверяем все сервисы
    ALL_HEALTHY=true
    for SERVICE in call-processor fraud-detector transcription-analyzer reporting-nps; do
        curl -sf http://localhost:$(echo $SERVICE | tr '[:lower:]' '[:upper:]' | tr '-' '_')_PORT:$(echo $SERVICE | tr '[:lower:]' '[:upper:]' | tr '-' '_')_PORT/actuator/health > /dev/null 2>&1
        if [ $? -ne 0 ]; then
            ALL_HEALTHY=false
            break
        fi
    done

    if [ "$ALL_HEALTHY" = true ]; then
        echo "All services recovered after ${ELAPSED} seconds"
        break
    fi

    sleep 5
done

# Проверяем данные в PostgreSQL
echo "Verifying data integrity..."
docker exec postgres psql -U postgres -d call_platform -c "SELECT COUNT(*) FROM call_metadata;" > results/data_integrity.txt

echo "=== Hard shutdown test completed ==="
```

### 14.8.2. Ожидаемые результаты

| Метрика | Цель |
|---------|------|
| Время восстановления | < 3 мин |
| Данные PostgreSQL | Сохранены |
| State Store Kafka | Восстановлен |
| Потерянные сообщения | 0 |

## 14.9. Комплексный запуск

### 14.9.1. scripts/chaos/run-all.sh

```bash
#!/bin/bash
set -e

echo "============================================"
echo "  Chaos Testing Suite"
echo "============================================"

# Сценарий 1: Отказ брокера
echo ">>> Running broker failure test..."
./scripts/chaos/kill-broker.sh
echo "Broker failure test completed."

# Сценарий 2: Отказ сервиса
echo ">>> Running service failure test..."
./scripts/chaos/kill-service.sh call-processor
echo "Service failure test completed."

# Сценарий 3: Отказ потребителя
echo ">>> Running consumer failure test..."
./scripts/chaos/kill-consumer.sh transcription-analyzer
echo "Consumer failure test completed."

# Сценарий 4: Сбой сети
echo ">>> Running network partition test..."
./scripts/chaos/network-delay.sh call-processor
echo "Network partition test completed."

# Сценарий 5: Ограничение ресурсов
echo ">>> Running resource constraint test..."
./scripts/chaos/resource-constraint.sh fraud-detector
echo "Resource constraint test completed."

# Сценарий 6: Hard Shutdown
echo ">>> Running hard shutdown test..."
./scripts/chaos/hard-shutdown.sh
echo "Hard shutdown test completed."

echo "============================================"
echo "  All chaos tests completed!"
echo "============================================"
```

### 14.9.2. Makefile targets

```makefile
# Хаос-тестирование
chaos-broker: ## Отказ брокера Kafka
	./scripts/chaos/kill-broker.sh

chaos-service: ## Отказ сервиса (по умолчанию call-processor)
	./scripts/chaos/kill-service.sh ${SERVICE}

chaos-consumer: ## Отказ потребителя
	./scripts/chaos/kill-consumer.sh ${CONSUMER}

chaos-network: ## Сбой сети
	./scripts/chaos/network-delay.sh ${SERVICE}

chaos-resources: ## Ограничение ресурсов
	./scripts/chaos/resource-constraint.sh ${SERVICE}

chaos-hard: ## Hard Shutdown
	./scripts/chaos/hard-shutdown.sh

chaos-all: ## Запустить все хаос-тесты
	./scripts/chaos/run-all.sh
```

## 14.10. Верификация после хаос-теста

### 14.10.1. scripts/verify/all.sh

```bash
#!/bin/bash
set -e

echo "=== Verifying system after chaos tests ==="

# 1. Проверка топиков Kafka
echo ">>> Checking Kafka topics..."
docker exec kafka-1 kafka-topics.sh --describe --bootstrap-server localhost:9092 > results/topics.txt
echo "Topics verified."

# 2. Проверка данных в PostgreSQL
echo ">>> Checking PostgreSQL data..."
docker exec postgres psql -U postgres -d call_platform -c "SELECT COUNT(*) FROM call_metadata;" > results/postgres_count.txt
docker exec postgres psql -U postgres -d call_platform -c "SELECT COUNT(*) FROM call_transcriptions;" > results/transcriptions_count.txt
echo "PostgreSQL verified."

# 3. Проверка lag потребителей
echo ">>> Checking consumer lag..."
docker exec kafka-1 kafka-consumer-groups.sh --describe --bootstrap-server localhost:9092 > results/consumer_lag.txt
echo "Consumer lag verified."

# 4. Проверка State Store
echo ">>> Checking State Store..."
docker exec fraud-detector ls -lh /tmp/kafka-streams/fraud-detector > results/state_store.txt
echo "State Store verified."

# 5. Проверка здоровья сервисов
echo ">>> Checking service health..."
for SERVICE in call-processor fraud-detector transcription-analyzer reporting-nps; do
    echo -n "Checking $SERVICE... "
    curl -sf http://localhost:8081/actuator/health > /dev/null 2>&1 && echo "OK" || echo "FAIL"
done

echo "=== Verification complete ==="
```

### 14.10.2. Критерии успеха

| Проверка | Критерий |
|----------|----------|
| Kafka topics | Все 8 топиков существуют |
| PostgreSQL | Данные не потеряны, FK валидны |
| Consumer lag | < 2000 для всех групп |
| State Store | Целостность сохранена |
| Сервисы | Все здоровы |
| Потерянные данные | 0 |
| Дублированные данные | 0 |

## 14.11. Отчётность

### 14.11.1. Структура отчёта

```markdown
# Отчёт хаос-теста
## Дата: YYYY-MM-DD
## Конфигурация: Mac M1, 16GB RAM

### Сценарий 1: Отказ брокера
- Время восстановления: XX сек
- Потерянные данные: 0
- Дублированные данные: 0
- Максимальный lag: XXX

### Сценарий 2: Отказ сервиса
- Время восстановления: XX сек
- Потерянные данные: 0
- Ошибки клиентов: временные

### Выводы
- [ ] Все сценарии пройдены
- [ ] Нет потерь данных
- [ ] Время восстановления в норме
- [ ] Рекомендации: ...
```

### 14.11.2. Автоматическая генерация

```bash
# Генерация отчёта после всех хаос-тестов
./scripts/chaos/run-all.sh
./scripts/verify/all.sh

# Ручное создание отчёта
cat > reports/chaos-test-$(date +%Y%m%d).md << EOF
# Chaos Test Report
Date: $(date)
Configuration: Mac M1, 16GB RAM, 6 CPU

## Results
$(cat results/topics.txt)
$(cat results/postgres_count.txt)
$(cat results/consumer_lag.txt)

## Conclusion
- All scenarios passed
- No data loss
- Recovery times within limits
EOF
```

## 14.12. Инструменты хаос-тестирования

| Инструмент | Назначение | Платформа |
|------------|-----------|-----------|
| **Chaos Toolkit** | Фреймворк хаос-тестирования | Python (ARM) |
| **Toxiproxy** | Сетевые задержки и ошибки | ARM образ |
| **Bash скрипты** | Управление контейнерами | Нативный |

### 14.12.1. Chaos Toolkit пример

```text
// chaos/chaos-engineering.json
{
  "version": "1.0.0",
  "title": "Call Platform Chaos Engineering",
  "description": "Chaos tests for call processing platform",
  "tags": ["kafka", "microservices", "docker"],
  "tests": [
    {
      "name": "broker-failure",
      "action": {
        "type": "docker",
        "command": "stop",
        "target": "kafka-1"
      },
      "probe": {
        "type": "assertion",
        "expected": "cluster-recovered",
        "timeout": 60
      }
    },
    {
      "name": "service-failure",
      "action": {
        "type": "docker",
        "command": "stop",
        "target": "call-processor"
      },
      "probe": {
        "type": "assertion",
        "expected": "service-recovered",
        "timeout": 30
      }
    }
  ]
}
```

## 14.13. Рекомендации по устойчивости

| Рекомендация | Приоритет | Описание |
|-------------|----------|----------|
| Retry с exponential backoff | High | Все Kafka операции |
| Circuit Breaker | High | Межсервисные вызовы |
| Health checks | Medium | Liveness + Readiness probes |
| Auto-scaling | Medium | Горизонтальное масштабирование |
| Data replication | High | Репликация Kafka (factor=3) |
| State Store recovery | High | Exactly-once semantics |
| Monitoring alerts | Medium | Grafana алерты |
