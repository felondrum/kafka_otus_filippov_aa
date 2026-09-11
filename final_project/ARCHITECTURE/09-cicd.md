# 9. CI/CD Pipeline

## 9.1. Архитектура локального пайплайна

```
┌──────────────────────────────────────────────────────────────┐
│                  Local CI/CD Pipeline                         │
│                                                               │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐               │
│  │  Stage   │    │  Stage   │    │  Stage   │               │
│  │   1      │───▶│   2      │───▶│   3      │               │
│  │  Build   │    │  Test    │    │ Package  │               │
│  └──────────┘    └──────────┘    └──────────┘               │
│       │                  │                  │                │
│       ▼                  ▼                  ▼                │
│  Maven/Gradle        JUnit 5            Docker              │
│  checkstyle          Testcontainers     ARM64 images        │
│  compile             Embedded Kafka     multi-platform      │
│  javadoc             Integration tests │                    │
│                                               ┌──────────┐  │
│                                               │  Stage   │  │
│                                               │   4      │  │
│                                               │ Deploy   │  │
│                                               └──────────┘  │
│                                                    │         │
│                                                    ▼         │
│                                            ┌──────────┐       │
│                                            │ Local:   │       │
│                                            │ docker-  │       │
│                                            │ compose  │       │
│                                            │          │       │
│                                            │ K8s:     │       │
│                                            │ (future) │       │
│                                            └──────────┘       │
└──────────────────────────────────────────────────────────────┘
```

## 9.2. Makefile — единая точка входа

```makefile
# Makefile
.PHONY: help build test test-unit test-integration package deploy clean all

# Переменные
JAVA_VERSION := 21
MAVEN_CMD := mvn
DOCKER_COMPOSE := docker compose
PROJECTS := call-processor fraud-detector transcription-analyzer reporting-nps

help: ## Показать справку
	@echo "Доступные команды:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Собрать все микросервисы
	@echo "=== Building all services ==="
	@for project in $(PROJECTS); do \
		echo "Building $$project..."; \
		cd $$project && $(MAVEN_CMD) clean compile -q && cd ..; \
	done
	@echo "=== Build complete ==="

checkstyle: ## Запустить checkstyle
	@echo "=== Running checkstyle ==="
	@for project in $(PROJECTS); do \
		echo "Checking $$project..."; \
		cd $$project && $(MAVEN_CMD) checkstyle:check -q && cd ..; \
	done
	@echo "=== Checkstyle passed ==="

test-unit: ## Запустить unit-тесты
	@echo "=== Running unit tests ==="
	@for project in $(PROJECTS); do \
		echo "Testing $$project (unit)..."; \
		cd $$project && $(MAVEN_CMD) test -Dtest='*Test' -q && cd ..; \
	done
	@echo "=== Unit tests passed ==="

test-integration: ## Запустить интеграционные тесты
	@echo "=== Running integration tests ==="
	@for project in $(PROJECTS); do \
		echo "Testing $$project (integration)..."; \
		cd $$project && $(MAVEN_CMD) verify -Dtest='*IntegrationTest' -q && cd ..; \
	done
	@echo "=== Integration tests passed ==="

test: test-unit test-integration ## Запустить все тесты
	@echo "=== All tests passed ==="

package: ## Собрать Docker-образы
	@echo "=== Building Docker images ==="
	@for project in $(PROJECTS); do \
		echo "Building image for $$project..."; \
		docker buildx build --platform linux/arm64 -t $$project:latest --load ./$$project; \
	done
	@echo "=== Docker images built ==="

deploy: ## Запустить все сервисы через docker-compose
	@echo "=== Deploying with Docker Compose ==="
	$(DOCKER_COMPOSE) up -d
	@echo "=== Deployment complete ==="
	@echo "Services running:"
	$(DOCKER_COMPOSE) ps

deploy-core: ## Запустить только core-сервисы (без мониторинга)
	@echo "=== Deploying core services ==="
	$(DOCKER_COMPOSE) --profile core up -d
	@echo "=== Core deployment complete ==="

deploy-monitoring: ## Запустить только мониторинг
	@echo "=== Deploying monitoring ==="
	$(DOCKER_COMPOSE) --profile monitoring up -d
	@echo "=== Monitoring deployment complete ==="

clean: ## Очистить артефакты сборки
	@echo "=== Cleaning ==="
	@for project in $(PROJECTS); do \
		echo "Cleaning $$project..."; \
		cd $$project && $(MAVEN_CMD) clean && cd ..; \
	done
	@echo "=== Clean complete ==="

down: ## Остановить все сервисы
	@echo "=== Stopping services ==="
	$(DOCKER_COMPOSE) down
	@echo "=== Services stopped ==="

logs: ## Показать логи всех сервисов
	$(DOCKER_COMPOSE) logs -f

logs-service: ## Показать логи конкретного сервиса (make logs-service SERVICE=call-processor)
	$(DOCKER_COMPOSE) logs -f $(SERVICE)

restart: ## Перезапустить все сервисы
	$(DOCKER_COMPOSE) restart

health: ## Проверить здоровье всех сервисов
	@echo "=== Checking service health ==="
	@for project in $(PROJECTS); do \
		echo -n "Checking $$project... "; \
		curl -sf http://localhost:$$($(shell echo $$project | tr '[:lower:]' '[:upper:]' | tr '-' '_')_PORT):$$($(shell echo $$project | tr '[:lower:]' '[:upper:]' | tr '-' '_')_PORT)/actuator/health > /dev/null 2>&1 && echo "OK" || echo "FAIL"; \
	done

all: clean build checkstyle test package deploy ## Полный пайплайн
```

## 9.3. Скрипты для отдельных этапов

### 9.3.1. scripts/build.sh — Сборка

```bash
#!/bin/bash
set -e

PROJECTS=("call-processor" "fraud-detector" "transcription-analyzer" "reporting-nps")

echo "=== Building all services ==="

for project in "${PROJECTS[@]}"; do
    echo "Building $project..."
    cd "$project" || exit 1
    mvn clean compile -q
    cd ..
done

echo "=== Build complete ==="
```

### 9.3.2. scripts/test.sh — Тестирование

```bash
#!/bin/bash
set -e

PROJECTS=("call-processor" "fraud-detector" "transcription-analyzer" "reporting-nps")
TEST_TYPE="${1:-all}"  # all, unit, integration

echo "=== Running $TEST_TYPE tests ==="

for project in "${PROJECTS[@]}"; do
    echo "Testing $project ($TEST_TYPE)..."
    cd "$project" || exit 1

    case "$TEST_TYPE" in
        unit)
            mvn test -Dtest='*Test' -q
            ;;
        integration)
            mvn verify -Dtest='*IntegrationTest' -q
            ;;
        all)
            mvn test -q
            ;;
    esac

    cd ..
done

echo "=== All tests passed ==="
```

### 9.3.3. scripts/package.sh — Сборка Docker-образов

```bash
#!/bin/bash
set -e

PROJECTS=("call-processor" "fraud-detector" "transcription-analyzer" "reporting-nps")

echo "=== Building Docker images (ARM64) ==="

for project in "${PROJECTS[@]}"; do
    echo "Building image for $project..."
    docker buildx build \
        --platform linux/arm64 \
        -t "$project:latest" \
        --load \
        "./$project"
done

echo "=== Docker images built ==="
```

### 9.3.4. scripts/deploy.sh — Развёртывание

```bash
#!/bin/bash
set -e

PROFILE="${1:-full}"  # full, core, monitoring

echo "=== Deploying with profile: $PROFILE ==="

case "$PROFILE" in
    core)
        docker compose --profile core up -d
        ;;
    monitoring)
        docker compose --profile monitoring up -d
        ;;
    full)
        docker compose up -d
        ;;
    *)
        echo "Unknown profile: $PROFILE"
        echo "Usage: $0 [full|core|monitoring]"
        exit 1
        ;;
esac

echo "=== Deployment complete ==="
echo "Services running:"
docker compose ps
```

### 9.3.5. scripts/health-check.sh — Проверка здоровья

```bash
#!/bin/bash
set -e

echo "=== Checking service health ==="

SERVICES=(
    "call-processor:8081"
    "fraud-detector:8082"
    "transcription-analyzer:8083"
    "reporting-nps:8084"
)

ALL_OK=true

for service in "${SERVICES[@]}"; do
    name="${service%%:*}"
    port="${service##*:}"
    echo -n "Checking $name... "

    if curl -sf "http://localhost:$port/actuator/health" > /dev/null 2>&1; then
        echo "OK"
    else
        echo "FAIL"
        ALL_OK=false
    fi
done

if [ "$ALL_OK" = true ]; then
    echo "=== All services healthy ==="
else
    echo "=== Some services are unhealthy ==="
    exit 1
fi
```

### 9.3.6. scripts/cleanup.sh — Очистка

```bash
#!/bin/bash
set -e

echo "=== Cleaning up ==="

# Остановка сервисов
docker compose down

# Очистка артефактов сборки
for project in call-processor fraud-detector transcription-analyzer reporting-nps; do
    echo "Cleaning $project..."
    cd "$project" && mvn clean && cd ..
done

echo "=== Cleanup complete ==="
```

## 9.4. Полный локальный пайплайн

### 9.4.1. scripts/run-pipeline.sh — Полный цикл

```bash
#!/bin/bash
set -e

echo "============================================"
echo "  Local CI/CD Pipeline"
echo "============================================"
echo ""

# Stage 1: Build
echo ">>> Stage 1: Build"
echo "============================================"
./scripts/build.sh
echo ""

# Stage 2: Checkstyle
echo ">>> Stage 2: Checkstyle"
echo "============================================"
for project in call-processor fraud-detector transcription-analyzer reporting-nps; do
    echo "Checking $project..."
    cd "$project" && mvn checkstyle:check -q && cd ..
done
echo ""

# Stage 3: Test
echo ">>> Stage 3: Test"
echo "============================================"
./scripts/test.sh all
echo ""

# Stage 4: Package
echo ">>> Stage 4: Package"
echo "============================================"
./scripts/package.sh
echo ""

# Stage 5: Deploy
echo ">>> Stage 5: Deploy"
echo "============================================"
./scripts/deploy.sh full
echo ""

# Stage 6: Health Check
echo ">>> Stage 6: Health Check"
echo "============================================"
./scripts/health-check.sh
echo ""

echo "============================================"
echo "  Pipeline completed successfully!"
echo "============================================"
```

### 9.4.2. Makefile targets — Полный пайплайн

```makefile
# Полные пайплайны

build-test: build checkstyle test ## Собрать, проверить и протестировать

package-deploy: package deploy ## Собрать образы и развернуть

all: clean build-test package deploy health ## Полный пайплайн
```

## 9.5. Quality Gates

| Gate | Порог | Описание |
|------|-------|----------|
| Unit test coverage | > 70% | Покрытие unit-тестов (Jacoco) |
| Integration test coverage | > 50% | Покрытие интеграционных тестов |
| No critical vulnerabilities | 0 | Проверка зависимостей (OWASP check) |
| Checkstyle violations | 0 | Стилевые проверки Maven |
| Build success | 100% | Успешная сборка всех модулей |
| Docker image scan | PASS | Сканирование образов (опционально) |
| Health check | 100% | Все сервисы здоровы |

## 9.6. Dockerfile (пример)

```dockerfile
# call-processor/Dockerfile
FROM eclipse-temurin:21-jre-alpine AS builder
WORKDIR /app
COPY target/*.jar app.jar
RUN java -Djarmode=layertools -jar app.jar extract

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /app/dependencies/ ./
COPY --from=builder /app/spring-boot-loader/ ./
COPY --from=builder /app/snapshot-dependencies/ ./
COPY --from=builder /app/application/ ./

EXPOSE 8081

ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
```

## 9.7. Image tagging strategy

| Сценарий | Tag | Описание |
|----------|-----|----------|
| Локальная разработка | `latest` | Последняя собранная версия |
| Конкретная версия | `v1.0.0` | Семантическое версионирование |
| Git commit | `git-sha` | Для отладки конкретных коммитов |

## 9.8. Deployment Model

### 9.8.1. Local deployment (docker-compose)

```bash
# Полный стек
make deploy

# Только core-сервисы
make deploy-core

# Только мониторинг
make deploy-monitoring

# Перезапуск
make restart

# Остановка
make down
```

### 9.8.2. Production deployment (K8s, future)

```yaml
# k8s/deployment.yaml (future)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: call-processor
spec:
  replicas: 3
  selector:
    matchLabels:
      app: call-processor
  template:
    metadata:
      labels:
        app: call-processor
    spec:
      containers:
        - name: call-processor
          image: call-processor:latest
          imagePullPolicy: Always
          ports:
            - containerPort: 8081
          env:
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "kafka-1:9092,kafka-2:9092,kafka-3:9092"
            - name: SPRING_PROFILES_ACTIVE
              value: "prod"
          resources:
            requests:
              memory: "1Gi"
              cpu: "1"
            limits:
              memory: "2Gi"
              cpu: "2"
          readinessProbe:
            httpGet:
              path: /actuator/health/readiness
              port: 8081
            initialDelaySeconds: 30
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /actuator/health/liveness
              port: 8081
            initialDelaySeconds: 60
            periodSeconds: 15
```
