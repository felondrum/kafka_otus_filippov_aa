# 13. Load Testing

## 13.1. Целевые показатели производительности

Адаптировано для M1 (ARM64):

| Метрика | Нормальная нагрузка | Пиковая нагрузка |
|---------|-------------------|------------------|
| Throughput | 5000 звонков/час (≈1.4 зв/сек) | до 100 зв/сек |
| Сквозная обработка | < 45 сек | < 90 сек |
| Lag потребителей | < 2000 | < 8000 |
| CPU использование | < 70% | < 90% |
| Memory использование | < 80% | < 95% |

## 13.2. Сценарии нагрузочного тестирования

| Сценарий | Интенсивность | Длительность | Критерии успеха |
|----------|--------------|--------------|-----------------|
| Базовый | 1.5 зв/сек | 2 часа | Lag < 500, CPU < 70%, память < 80% |
| Пиковый | 100 зв/сек | 5 мин | Lag < 8000, восстановление < 10 мин, нет потерь |
| Длительный | 2 зв/сек | 8 часов | State Store < 3GB, нет утечек памяти |
| С ошибками | 5 зв/сек (5% битых) | 30 мин | DLQ заполняется, деградация < 25% |

## 13.3. Инструменты

| Инструмент | Назначение | Совместимость |
|------------|-----------|---------------|
| **k6** | Нагрузочный тест HTTP | ARM64 native |
| **kcat (kafkacat)** | Стрельба напрямую в Kafka | ARM64 |
| **kafka-producer-perf-test** | Встроенный тест Kafka | ARM64 |
| **Docker stats** | Мониторинг ресурсов | ARM64 |
| **Prometheus + Grafana** | Сбор и визуализация метрик | ARM64 образы |

## 13.4. Сценарий 1: Базовая нагрузка

### 13.4.1. Конфигурация

```javascript
// scripts/load-test-basic.js (k6)
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('error_rate');

export const options = {
    stages: [
        { duration: '5m', target: 50 },   // ramp-up
        { duration: '2h', target: 50 },   // steady state
        { duration: '5m', target: 0 },    // ramp-down
    ],
    thresholds: {
        http_req_duration: ['p(95)<45000'],  // p95 < 45 сек
        http_req_failed: ['rate<0.01'],       // < 1% ошибок
        error_rate: ['rate<0.05'],            // < 5% ошибок
    },
};

const PAYLOAD = JSON.stringify({
    callId: '550e8400-e29b-41d4-a716-446655440000',
    phone: '+79001234567',
    duration: 180,
    agentId: 'agent-1',
    npsScore: 8,
    correlationId: 'load-test-{{__V}}',
});

export default function () {
    const res = http.post('http://localhost:8081/api/calls', PAYLOAD, {
        headers: { 'Content-Type': 'application/json' },
    });

    const success = check(res, {
        'status is 202': (r) => r.status === 202,
        'response time < 45s': (r) => r.timings.duration < 45000,
    });

    errorRate.add(!success);
    sleep(1);  // ~1 зв/сек
}
```

### 13.4.2. Запуск

```bash
# Запуск базового теста
k6 run scripts/load-test-basic.js

# С отчётом
k6 run --out json=results/basic.json scripts/load-test-basic.js

# С Grafana
k6 run --out json=results/basic.json scripts/load-test-basic.js
# Grafana автоматически соберёт метрики из Prometheus
```

### 13.4.3. Ожидаемые результаты

| Метрика | Цель | Факт |
|---------|------|------|
| Обработано звонков | ~10,000 | - |
| p50 latency | < 20 сек | - |
| p95 latency | < 45 сек | - |
| p99 latency | < 60 сек | - |
| Ошибки 5xx | < 0.1% | - |
| Lag потребителей | < 500 | - |
| CPU (средний) | < 70% | - |
| Memory (средний) | < 80% | - |

## 13.5. Сценарий 2: Пиковая нагрузка

### 13.5.1. Конфигурация

```javascript
// scripts/load-test-peak.js (k6)
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('error_rate');

export const options = {
    stages: [
        { duration: '1m', target: 100 },  // rapid ramp-up
        { duration: '5m', target: 100 },  // peak
        { duration: '2m', target: 0 },    // cool-down
    ],
    thresholds: {
        http_req_duration: ['p(95)<90000'],  // p95 < 90 сек
        http_req_failed: ['rate<0.001'],      // < 0.1% ошибок
        error_rate: ['rate<0.05'],            // < 5% ошибок
    },
};

const PAYLOAD = JSON.stringify({
    callId: '550e8400-e29b-41d4-a716-446655440000',
    phone: '+79001234567',
    duration: 180,
    agentId: 'agent-1',
    npsScore: 8,
    correlationId: 'peak-test-{{__V}}',
});

export default function () {
    const res = http.post('http://localhost:8081/api/calls', PAYLOAD, {
        headers: { 'Content-Type': 'application/json' },
    });

    const success = check(res, {
        'status is 202': (r) => r.status === 202,
        'response time < 90s': (r) => r.timings.duration < 90000,
    });

    errorRate.add(!success);
    sleep(0.01);  // ~100 зв/сек
}
```

### 13.5.2. Запуск

```bash
# Запуск пикового теста
k6 run scripts/load-test-peak.js

# Мониторинг в реальном времени
k6 run scripts/load-test-peak.js | tee results/peak.log
```

### 13.5.3. Ожидаемые результаты

| Метрика | Цель | Факт |
|---------|------|------|
| Обработано звонков | ~30,000 | - |
| p50 latency | < 45 сек | - |
| p95 latency | < 90 сек | - |
| p99 latency | < 120 сек | - |
| Ошибки 5xx | < 0.1% | - |
| Lag потребителей | < 8000 | - |
| CPU (пик) | < 90% | - |
| Memory (пик) | < 95% | - |
| Восстановление lag | < 10 мин | - |
| Потерянные данные | 0% | - |

## 13.6. Сценарий 3: Длительная нагрузка (Soak)

### 13.6.1. Конфигурация

```javascript
// scripts/load-test-soak.js (k6)
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('error_rate');

export const options = {
    stages: [
        { duration: '5m', target: 100 },   // ramp-up
        { duration: '8h', target: 100 },   // steady state (8 часов)
        { duration: '5m', target: 0 },     // ramp-down
    ],
    thresholds: {
        http_req_duration: ['p(95)<45000'],
        http_req_failed: ['rate<0.001'],
        error_rate: ['rate<0.05'],
    },
};

const PAYLOAD = JSON.stringify({
    callId: '550e8400-e29b-41d4-a716-446655440000',
    phone: '+79001234567',
    duration: 180,
    agentId: 'agent-1',
    npsScore: 8,
    correlationId: 'soak-test-{{__V}}',
});

export default function () {
    const res = http.post('http://localhost:8081/api/calls', PAYLOAD, {
        headers: { 'Content-Type': 'application/json' },
    });

    check(res, {
        'status is 202': (r) => r.status === 202,
    });

    errorRate.add(!res.status === 202);
    sleep(0.6);  // ~1.67 зв/сек
}
```

### 13.6.2. Мониторинг утечек памяти

```bash
# Мониторинг ресурсов во время теста
docker stats --no-stream &
STATS_PID=$!

# Запуск теста
k6 run scripts/load-test-soak.js

# Остановка мониторинга
kill $STATS_PID

# Анализ результатов
docker stats --format "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}" > results/memory_report.txt
```

### 13.6.3. Критерии успеха

| Метрика | Цель |
|---------|------|
| State Store размер | < 3 GB |
| Memory утечка | < 10% за 8 часов |
| CPU стабильность | без роста |
| Lag стабильность | без роста |
| Ошибки | 0 критических |

## 13.7. Сценарий 4: Тест с ошибками

### 13.7.1. Конфигурация

```javascript
// scripts/load-test-errors.js (k6)
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.0.0/index.js';

const errorRate = new Rate('error_rate');

export const options = {
    stages: [
        { duration: '5m', target: 300 },   // ramp-up
        { duration: '30m', target: 300 },  // steady state (5 зв/сек)
        { duration: '5m', target: 0 },     // ramp-down
    ],
    thresholds: {
        http_req_duration: ['p(95)<45000'],
        http_req_failed: ['rate<0.10'],    // до 10% ошибок допустимо
        error_rate: ['rate<0.15'],         // < 15% ошибок
    },
};

const VALID_PAYLOAD = JSON.stringify({
    callId: '550e8400-e29b-41d4-a716-446655440000',
    phone: '+79001234567',
    duration: 180,
    agentId: 'agent-1',
    npsScore: 8,
    correlationId: 'error-test-{{__V}}',
});

const INVALID_PAYLOADS = [
    JSON.stringify({ callId: 'invalid', phone: 'bad', duration: -1, agentId: '', npsScore: 15 }),
    JSON.stringify({ callId: '', phone: '', duration: 0, agentId: '', npsScore: 0 }),
    JSON.stringify({}),  // missing all fields
];

export default function () {
    // 5% битых сообщений
    if (randomIntBetween(1, 100) <= 5) {
        const invalidPayload = INVALID_PAYLOADS[randomIntBetween(0, INVALID_PAYLOADS.length - 1)];
        const res = http.post('http://localhost:8081/api/calls', invalidPayload, {
            headers: { 'Content-Type': 'application/json' },
        });
        check(res, {
            'invalid call returns 400': (r) => r.status === 400,
        });
    } else {
        const res = http.post('http://localhost:8081/api/calls', VALID_PAYLOAD, {
            headers: { 'Content-Type': 'application/json' },
        });
        const success = check(res, {
            'status is 202': (r) => r.status === 202,
        });
        errorRate.add(!success);
    }

    sleep(0.2);  // ~5 зв/сек
}
```

### 13.7.2. Ожидаемые результаты

| Метрика | Цель |
|---------|------|
| DLQ заполнение | Битые сообщения попадают в DLQ |
| Деградация производительности | < 25% |
| Валидные звонки обрабатываются | Да |
| Ошибки 5xx | < 1% |
| Lag потребителей | < 2000 |

## 13.8. Мониторинг во время тесто��

### 13.8.1. Сбор метрик

```bash
# scripts/collect-metrics.sh
#!/bin/bash

echo "=== Collecting metrics during load test ==="

# Docker stats
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}" > results/docker_stats.txt &
DOCKER_STATS_PID=$!

# Prometheus queries
echo "Querying Prometheus..."
curl -s "http://localhost:9090/api/v1/query?query=app_kafka_consumer_lag" > results/prometheus_lag.json
curl -s "http://localhost:9090/api/v1/query?query=kafka_server_brokertopicmetrics_messagesinpersec" > results/prometheus_throughput.json
curl -s "http://localhost:9090/api/v1/query?query=jvm_memory_bytes_used" > results/prometheus_memory.json

# Kafka metrics
kafka-consumer-perf-test.sh \
    --broker-list localhost:9092 \
    --topic calls.completed \
    --messages 1000000 \
    --print-metrics > results/kafka_perf.txt

# Остановка сбора
kill $DOCKER_STATS_PID

echo "=== Metrics collected ==="
```

### 13.8.2. Grafana дашборды для мониторинга

| Дашборд | Метрики |
|---------|---------|
| Kafka Cluster | Throughput, Lag, Under-replicated partitions |
| Calls Status | Total calls, Success rate, NPS distribution |
| Consumer Lag | Lag by group, Lag trend |
| JVM Metrics | Heap usage, GC pauses, Thread count |
| System Resources | CPU, Memory, Disk I/O |

## 13.9. Генерация отчётов

### 13.9.1. scripts/generate-report.sh

```bash
#!/bin/bash

TEST_TYPE="${1:-basic}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT_DIR="tests/reports"
REPORT_FILE="$REPORT_DIR/load-test-${TEST_TYPE}-${TIMESTAMP}.md"

mkdir -p "$REPORT_DIR"

cat > "$REPORT_FILE" << EOF
# Отчёт нагрузочного теста: $TEST_TYPE
# Дата: $(date)
# Конфигурация: Mac M1, 16GB RAM, 6 CPU

## Результаты

### Throughput
\`\`\`
$(cat results/throughput.txt)
\`\`\`

### Latency
| Метрика | Значение |
|---------|----------|
| p50 | \$(cat results/p50.txt) сек |
| p95 | \$(cat results/p95.txt) сек |
| p99 | \$(cat results/p99.txt) сек |

### Lag потребителей
| Группа | Макс lag |
|--------|----------|
\$(cat results/consumer_lag.txt)

### Ресурсы
| Компонент | CPU (средний) | Memory (средний) |
|-----------|--------------|------------------|
\$(cat results/resource_usage.txt)

## Выводы
- [ ] Все критерии успеха выполнены
- [ ] Нет утечек памяти
- [ ] Нет потерь данных
- [ ] Lag в пределах нормы

## Рекомендации
- [Добавить рекомендации на основе результатов]
EOF

echo "Report generated: $REPORT_FILE"
```

### 13.9.2. JSON отчёт k6

```bash
# k6 с JSON output
k6 run --out json=results/load-test-$(date +%Y%m%d).json scripts/load-test-basic.js

# Конвертация в отчёт
k6 summary tools results/load-test-$(date +%Y%m%d).json > results/summary.txt
```

## 13.10. Автоматизация

### 13.10.1. scripts/run-load-tests.sh

```bash
#!/bin/bash
set -e

echo "============================================"
echo "  Load Testing Suite"
echo "============================================"

# Базовый тест
echo ">>> Running basic load test (2 hours)..."
k6 run scripts/load-test-basic.js
echo "Basic test complete."

# Пиковый тест
echo ">>> Running peak load test (5 min)..."
k6 run scripts/load-test-peak.js
echo "Peak test complete."

# Тест с ошибками
echo ">>> Running error injection test (30 min)..."
k6 run scripts/load-test-errors.js
echo "Error test complete."

# Сбор метрик
echo ">>> Collecting metrics..."
./scripts/collect-metrics.sh

# Генерация отчёта
echo ">>> Generating report..."
./scripts/generate-report.sh all

echo "============================================"
echo "  All load tests completed!"
echo "============================================"
```

### 13.10.2. Makefile targets

```makefile
# Нагрузочное тестирование
load-test-basic: ## Запустить базовый нагрузочный тест
	k6 run scripts/load-test-basic.js

load-test-peak: ## Запустить пиковую нагрузку
	k6 run scripts/load-test-peak.js

load-test-soak: ## Запустить длительный тест (8 часов)
	k6 run scripts/load-test-soak.js

load-test-errors: ## Запустить тест с ошибками
	k6 run scripts/load-test-errors.js

load-test-all: ## Запустить все нагрузочные тесты
	./scripts/run-load-tests.sh

load-test-report: ## Сгенерировать отчёт
	./scripts/generate-report.sh all
```

## 13.11. Критерии успеха (M1)

| Метрика | Порог |
|---------|-------|
| Lag (норма) | < 2000 |
| Lag (пик) | < 8000 |
| Сквозная обработка (p95) | < 45 сек |
| Потеря данных | 0% |
| Ошибки 5xx | < 0.1% |
| Потребление памяти | < 80% |
| Время восстановления (после пика) | < 10 мин |
