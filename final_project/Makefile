.PHONY: deploy health logs stop clean restart deploy-core deploy-monitoring deploy-load-test

# Default profile
PROFILE ?= full

# Deploy all services
deploy:
	@echo "Deploying infrastructure with profile: $(PROFILE)"
	docker compose --profile $(PROFILE) up -d
	@echo ""
	@echo "Services starting... Waiting for health checks."
	@echo "Run 'make health' to check status."

# Deploy only core services (Kafka, Schema Registry, PostgreSQL)
deploy-core:
	@echo "Deploying core services only..."
	docker compose --profile core up -d

# Deploy only monitoring services
deploy-monitoring:
	@echo "Deploying monitoring stack only..."
	docker compose --profile monitoring up -d

# Deploy for load testing
deploy-load-test:
	@echo "Deploying load-test profile..."
	docker compose --profile load-test up -d

# Check health of all services
health:
	@echo "=== Service Health Status ==="
	@echo ""
	@echo "--- Docker Compose Status ---"
	@docker compose ps
	@echo ""
	@echo "--- Kafka Health ---"
	@for i in 1 2 3; do \
		echo -n "kafka-$$i: "; \
		docker exec kafka-$$i /usr/bin/kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1 && echo "OK" || echo "FAIL"; \
	done
	@echo ""
	@echo "--- PostgreSQL Health ---"
	@docker exec postgres pg_isready -U postgres > /dev/null 2>&1 && echo "postgres: OK" || echo "postgres: FAIL"
	@echo ""
	@echo "--- Schema Registry Health ---"
	@curl -sf http://localhost:8085/ > /dev/null 2>&1 && echo "schema-registry: OK" || echo "schema-registry: FAIL"
	@echo ""
	@echo "--- Prometheus Health ---"
	@curl -sf http://localhost:9090/-/healthy > /dev/null 2>&1 && echo "prometheus: OK" || echo "prometheus: FAIL"
	@echo ""
	@echo "--- Grafana Health ---"
	@curl -sf http://localhost:3000/api/health > /dev/null 2>&1 && echo "grafana: OK" || echo "grafana: FAIL"
	@echo ""
	@echo "--- Kafdrop Health ---"
	@curl -sf http://localhost:9000/ > /dev/null 2>&1 && echo "kafdrop: OK" || echo "kafdrop: FAIL"

# Stream logs
logs:
	docker compose --profile $(PROFILE) logs -f

# Stop all services
stop:
	@echo "Stopping all services..."
	docker compose --profile $(PROFILE) down

# Clean up (remove volumes)
clean:
	@echo "Removing all services and volumes..."
	docker compose --profile $(PROFILE) down -v
	@echo "Cleanup complete."

# Restart all services
restart: stop deploy

# Initialize Kafka topics (runs inside kafka-1 container)
init-topics:
	@echo "Initializing Kafka topics..."
	@docker exec kafka-1 bash /etc/kafka/init-topics.sh "kafka-1:9092,kafka-2:9092,kafka-3:9092" 30 5

# List Kafka topics
topics-list:
	@echo "=== Kafka Topics ==="
	@docker exec kafka-1 /usr/bin/kafka-topics --bootstrap-server kafka-1:9092 --list 2>/dev/null || echo "Kafka not ready"

# Full setup (deploy + init topics)
setup: deploy
	@echo ""
	@echo "Waiting for services to be healthy..."
	@sleep 30
	@make health
	@echo ""
	@echo "Initializing Kafka topics..."
	@make init-topics

# Show status
status:
	@echo "=== Infrastructure Status ==="
	@docker compose ps
	@echo ""
	@echo "=== Kafka Topics ==="
	@docker exec kafka-1 /usr/bin/kafka-topics --bootstrap-server kafka-1:9092 --list 2>/dev/null || echo "Kafka not ready yet"
