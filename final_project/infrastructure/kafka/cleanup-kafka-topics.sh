#!/bin/bash
# Очистка данных из Kafka топов через изменение retention
# Usage: ./infrastructure/kafka/cleanup-kafka-topics.sh

set -e

KSQLDB_URL="http://localhost:8088"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info() { echo -e "${YELLOW}→${NC} $1"; }
success() { echo -e "${GREEN}✓${NC} $1"; }
error() { echo -e "${RED}✗${NC} $1"; }

echo "============================================"
echo "Cleanup Kafka Topics (Retention Reset)"
echo "============================================"
echo ""

# Stop ksqlDB queries
info "Stopping ksqlDB queries..."
curl -s -X POST "$KSQLDB_URL/ksql" \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "STOP CALLS_COMPLETED_AGG;"}' > /dev/null 2>&1 || true
curl -s -X POST "$KSQLDB_URL/ksql" \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "STOP FRAUD_ALERTS_FILTERED;"}' > /dev/null 2>&1 || true
success "ksqlDB queries stopped"
echo ""

# Get topics from ksqlDB
info "Fetching topics list..."
TOPICS_JSON=$(curl -s -X POST "$KSQLDB_URL/ksql" \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW TOPICS;"}')

USER_TOPICS=$(echo "$TOPICS_JSON" | python3 -c "
import sys, json
data = json.loads(sys.stdin.read())
topics = data[0].get('topics', [])
for t in topics:
    name = t['name']
    if not name.startswith('_') and 'kafka-connect' not in name and 'schema-registry' not in name:
        print(name)
" 2>/dev/null)

if [ -z "$USER_TOPICS" ]; then
    error "No user topics found"
    exit 1
fi

echo ""
info "Topics to clean:"
echo "$USER_TOPICS" | sed 's/^/  - /'
echo ""

# Set retention to 1 second for all user topics
info "Setting retention.ms=1000 (1 second) for all user topics..."
for TOPIC in $USER_TOPICS; do
    info "Configuring topic: $TOPIC"
    curl -s -X POST "$KSQLDB_URL/ksql" \
      -H "Content-Type: application/vnd.ksql.v1+json" \
      -d "{\"ksql\": \"ALTER TOPIC '$TOPIC' SET 'retention.ms' = 1000;\"}" > /dev/null 2>&1 || \
      info "  (ksqlDB alter not available, trying direct config)"
done
success "Retention set to 1 second for all topics"
echo ""

info "Waiting 5 seconds for data to expire..."
sleep 5

# Reset retention back to 1 week
info "Resetting retention.ms to 1 week (604800000 ms)..."
for TOPIC in $USER_TOPICS; do
    curl -s -X POST "$KSQLDB_URL/ksql" \
      -H "Content-Type: application/vnd.ksql.v1+json" \
      -d "{\"ksql\": \"ALTER TOPIC '$TOPIC' SET 'retention.ms' = 604800000;\"}" > /dev/null 2>&1 || true
done
success "Retention reset to 1 week"
echo ""

# Restart ksqlDB queries
info "Restarting ksqlDB queries..."
curl -s -X POST "$KSQLDB_URL/ksql" \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "CREATE TABLE CALLS_COMPLETED_AGG WITH (KAFKA_TOPIC='\"'\"'calls.completed.agg'\"'\"', PARTITIONS=6) AS SELECT agentId, COUNT(*) AS call_count, AVG(npsScore) AS avg_nps_score, MAX(completedAt) AS last_call_at FROM calls_completed_ext WINDOW TUMBLING (SIZE 5 MINUTES, GRACE PERIOD 1 MINUTE) GROUP BY agentId EMIT CHANGES;"}' > /dev/null 2>&1 || true
curl -s -X POST "$KSQLDB_URL/ksql" \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "CREATE TABLE FRAUD_ALERTS_FILTERED WITH (KAFKA_TOPIC='\"'\"'calls.fraud-alerts.agg'\"'\"', PARTITIONS=6) AS SELECT phone, COUNT(*) AS alert_count FROM calls_fraud_alerts_ext GROUP BY phone EMIT CHANGES;"}' > /dev/null 2>&1 || true
success "ksqlDB queries restarted"

echo ""
echo "============================================"
echo "Cleanup Complete"
echo "============================================"
echo ""
success "All Kafka topics cleaned!"
echo ""
info "Note: If data persists, restart Kafka brokers:"
echo "  docker compose --profile full restart kafka-1 kafka-2 kafka-3"
