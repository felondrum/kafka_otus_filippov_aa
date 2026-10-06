#!/bin/bash
# Очистка всех данных из PostgreSQL и Kafka
# Usage: ./infrastructure/kafka/cleanup-all-data.sh

set -e

POSTGRES_HOST="postgres"
POSTGRES_PORT="5432"
POSTGRES_DB="call_platform"
POSTGRES_USER="postgres"
POSTGRES_PASS="postgres-secret"

KSQLDB_URL="http://localhost:8088"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info() { echo -e "${YELLOW}→${NC} $1"; }
success() { echo -e "${GREEN}✓${NC} $1"; }
error() { echo -e "${RED}✗${NC} $1"; }

echo "============================================"
echo "Cleanup All Data"
echo "============================================"
echo ""

# ==================== PostgreSQL ====================
echo "============================================"
echo "Step 1: Clearing PostgreSQL tables"
echo "============================================"

TABLES=$(docker exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -t -c "SELECT tablename FROM pg_tables WHERE schemaname='public';" 2>/dev/null | tr -d ' ' | grep -v '^$')

if [ -z "$TABLES" ]; then
    info "No tables found in PostgreSQL"
else
    echo ""
    info "Tables to clear:"
    echo "$TABLES"
    echo ""
    
    for TABLE in $TABLES; do
        if [[ "$TABLE" == "*_hibernate_seq"* ]] || [[ "$TABLE" == "flyway_schema_history"* ]]; then
            info "Skipping migration table: $TABLE"
            continue
        fi
        
        info "Clearing table: $TABLE"
        docker exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -c "DELETE FROM \"$TABLE\";" 2>/dev/null && \
            success "Cleared $TABLE" || \
            error "Failed to clear $TABLE"
    done
fi

echo ""
info "PostgreSQL tables after cleanup:"
docker exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT tablename, (SELECT COUNT(*) FROM pg_tables WHERE schemaname='public' AND tablename=t.tablename) as row_count FROM pg_tables t WHERE schemaname='public';" 2>/dev/null
echo ""

# ==================== ksqlDB Queries ====================
echo "============================================"
echo "Step 2: Stopping ksqlDB queries"
echo "============================================"

# Get running queries
info "Getting running ksqlDB queries..."
QUERIES=$(curl -s -X POST "$KSQLDB_URL/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json" \
    -d '{"ksql": "SHOW QUERIES;"}')

echo "$QUERIES" | python3 -c "
import sys, json
data = json.loads(sys.stdin.read())
queries = data[0].get('queries', [])
if queries:
    for q in queries:
        print(f'  - {q[\"id\"]}')
else:
    print('  No running queries')
" 2>/dev/null || echo "  Could not fetch queries"

echo ""
info "Stopping all persistent queries..."
# Stop each query
curl -s -X POST "$KSQLDB_URL/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json" \
    -d '{"ksql": "STOP CALLS_COMPLETED_AGG;"}' > /dev/null 2>&1 || true
curl -s -X POST "$KSQLDB_URL/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json" \
    -d '{"ksql": "STOP FRAUD_ALERTS_FILTERED;"}' > /dev/null 2>&1 || true
success "Stopped ksqlDB queries"
echo ""

# ==================== Kafka Topics (via ksqlDB) ====================
echo "============================================"
echo "Step 3: Clearing Kafka topics via ksqlDB"
echo "============================================"

# Get topics from ksqlDB
info "Fetching topics from ksqlDB..."
TOPICS=$(curl -s -X POST "$KSQLDB_URL/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json" \
    -d '{"ksql": "SHOW TOPICS;"}')

echo "$TOPICS" | python3 -c "
import sys, json
data = json.loads(sys.stdin.read())
topics = data[0].get('topics', [])
user_topics = [t['name'] for t in topics if not t['name'].startswith('_') and 'kafka-connect' not in t['name'] and 'schema-registry' not in t['name']]
if user_topics:
    print('User topics:')
    for t in user_topics:
        print(f'  - {t}')
else:
    print('No user topics found')
" 2>/dev/null || echo "  Could not fetch topics"

echo ""
info "Note: Kafka topics will be recreated automatically when services restart."
info "To manually delete topics, use:"
echo "  docker exec kafka-1 bash -c '/usr/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic <topic-name>'"
echo ""

# ==================== Summary ====================
echo "============================================"
echo "Cleanup Complete"
echo "============================================"
echo ""
success "PostgreSQL: All tables cleared"
success "ksqlDB: All queries stopped"
info "Kafka topics: Will be recreated on service restart"
echo ""
info "Next steps:"
echo "  1. Restart services: docker compose --profile full restart"
echo "  2. Run init scripts: docker compose --profile full up -d ksqldb-init"
echo "  3. Verify: ./infrastructure/kafka/ksqldb-checks.sh all"
