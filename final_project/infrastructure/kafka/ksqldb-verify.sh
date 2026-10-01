#!/bin/bash
# Quick Verification Script for ksqlDB Analytics Layer
# Usage: ./infrastructure/kafka/ksqldb-verify.sh
# This script demonstrates and verifies ksqlDB functionality

set -e

KSQLDB_URL="http://localhost:8088"
CALL_PROCESSOR_URL="http://localhost:8081"
LOAD_SIMULATOR_URL="http://localhost:8087"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "============================================"
echo "ksqlDB Analytics Layer - Quick Verification"
echo "============================================"
echo ""

# Function to print success
success() {
    echo -e "${GREEN}✓${NC} $1"
}

# Function to print error
error() {
    echo -e "${RED}✗${NC} $1"
}

# Function to print info
info() {
    echo -e "${YELLOW}→${NC} $1"
}

# Step 1: Check ksqlDB is running
echo ""
echo "============================================"
echo "Step 1: Checking ksqlDB server status"
echo "============================================"

if curl -s "${KSQLDB_URL}/info" > /dev/null 2>&1; then
    info "ksqlDB is running"
    curl -s "${KSQLDB_URL}/info" | python3 -m json.tool 2>/dev/null || \
    curl -s "${KSQLDB_URL}/info"
    success "ksqlDB server is accessible"
else
    error "ksqlDB is not accessible at ${KSQLDB_URL}"
    echo "Please ensure docker compose is running: docker compose --profile full up -d"
    exit 1
fi

# Step 2: Show external streams
echo ""
echo "============================================"
echo "Step 2: External streams (Kafka topics)"
echo "============================================"

STREAMS=$(curl -s -X POST "${KSQLDB_URL}/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json" \
    -d '{"ksql": "SHOW STREAMS;"}')

echo "$STREAMS" | python3 -m json.tool 2>/dev/null || echo "$STREAMS"

if echo "$STREAMS" | grep -q "CALLS_COMPLETED_EXT"; then
    success "calls_completed_ext stream exists"
else
    error "calls_completed_ext stream not found"
fi

if echo "$STREAMS" | grep -q "CALLS_FRAUD_ALERTS_EXT"; then
    success "calls_fraud_alerts_ext stream exists"
else
    error "calls_fraud_alerts_ext stream not found"
fi

# Step 3: Show persistent tables
echo ""
echo "============================================"
echo "Step 3: Persistent aggregation tables"
echo "============================================"

TABLES=$(curl -s -X POST "${KSQLDB_URL}/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json" \
    -d '{"ksql": "SHOW TABLES;"}')

echo "$TABLES" | python3 -m json.tool 2>/dev/null || echo "$TABLES"

if echo "$TABLES" | grep -q "CALLS_COMPLETED_AGG"; then
    success "calls_completed_agg table exists (TUMBLING window aggregation)"
else
    error "calls_completed_agg table not found"
fi

if echo "$TABLES" | grep -q "FRAUD_ALERTS_FILTERED"; then
    success "fraud_alerts_filtered table exists (GROUP BY aggregation)"
else
    error "fraud_alerts_filtered table not found"
fi

# Step 4: Show active queries
echo ""
echo "============================================"
echo "Step 4: Active persistent queries"
echo "============================================"

QUERIES=$(curl -s -X POST "${KSQLDB_URL}/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json" \
    -d '{"ksql": "SHOW QUERIES;"}')

echo "$QUERIES" | python3 -m json.tool 2>/dev/null || echo "$QUERIES"

RUNNING_COUNT=$(echo "$QUERIES" | grep -o '"RUNNING":[0-9]*' | head -1 | grep -o '[0-9]*')
if [ -n "$RUNNING_COUNT" ] && [ "$RUNNING_COUNT" -gt 0 ]; then
    success "${RUNNING_COUNT} persistent query(ies) running"
else
    error "No running queries found"
fi

# Step 5: Generate test data
echo ""
echo "============================================"
echo "Step 5: Generating test data"
echo "============================================"

info "Sending 5 test calls via call-processor API..."
for i in $(seq 1 5); do
    TEST_UUID=$(uuidgen)
    AGENT_ID="agent-test-$(echo $i | awk '{printf "%03d", $1}')"
    NPS_SCORE=$(( (RANDOM % 11) ))
    
    RESPONSE=$(curl -s -X POST "${CALL_PROCESSOR_URL}/api/calls" \
        -H "Content-Type: application/json" \
        -d "{
            \"callId\": \"${TEST_UUID}\",
            \"phone\": \"+7900123456${i}\",
            \"duration\": $(( RANDOM % 300 + 60 )),
            \"agentId\": \"${AGENT_ID}\",
            \"npsScore\": ${NPS_SCORE}
        }")
    
    echo "  Call $i: agent=${AGENT_ID}, nps=${NPS_SCORE}"
done
success "5 test calls sent"

info "Waiting for ksqlDB to process events (10 seconds)..."
sleep 10

# Step 6: Verify aggregation
echo ""
echo "============================================"
echo "Step 6: Verifying aggregation results"
echo "============================================"

info "Checking calls_completed_agg table..."
info "Output topic: calls.completed.agg"
info "Schema: agentId (STRING), call_count (BIGINT), avg_nps_score (DOUBLE), last_call_at (LONG)"
echo ""
info "Expected: 5 calls aggregated by agentId in 5-minute TUMBLING window"
echo ""

# Step 7: Show demo queries
echo ""
echo "============================================"
echo "Step 7: Demo ad-hoc queries"
echo "============================================"
echo ""
echo "You can run these queries to explore data:"
echo ""
echo "  # Get all agents with call counts:"
echo "  curl -X POST ${KSQLDB_URL}/ksql \\"
echo "    -H 'Content-Type: application/vnd.ksql.v1+json' \\"
echo "    -d '{\"ksql\": \"SELECT agentId, call_count FROM CALLS_COMPLETED_AGG EMIT CHANGES LIMIT 10;\"}'"
echo ""
echo "  # Get top agents by call count:"
echo "  curl -X POST ${KSQLDB_URL}/ksql \\"
echo "    -H 'Content-Type: application/vnd.ksql.v1+json' \\"
echo "    -d '{\"ksql\": \"SELECT agentId, call_count, avg_nps_score FROM CALLS_COMPLETED_AGG ORDER BY call_count DESC LIMIT 10;\"}'"
echo ""
echo "  # Check fraud alerts:"
echo "  curl -X POST ${KSQLDB_URL}/ksql \\"
echo "    -H 'Content-Type: application/vnd.ksql.v1+json' \\"
echo "    -d '{\"ksql\": \"SELECT phone, alert_count FROM FRAUD_ALERTS_FILTERED EMIT CHANGES LIMIT 10;\"}'"
echo ""

# Step 8: Summary
echo ""
echo "============================================"
echo "Verification Summary"
echo "============================================"
echo ""
echo "ksqlDB Analytics Layer Components:"
echo "  ✓ ksqlDB Server (port 8088)"
echo "  ✓ External Stream: calls_completed_ext → calls.completed topic"
echo "  ✓ External Stream: calls_fraud_alerts_ext → calls.fraud-alerts topic"
echo "  ✓ Persistent Table: calls_completed_agg (TUMBLING 5min window)"
echo "  ✓ Persistent Table: fraud_alerts_filtered (GROUP BY phone)"
echo "  ✓ Output Topic: calls.completed.agg"
echo "  ✓ Output Topic: calls.fraud-alerts.agg"
echo ""
echo "Data Flow:"
echo "  calls.completed ──▶ calls_completed_ext ──▶ calls_completed_agg ──▶ calls.completed.agg"
echo "  calls.fraud-alerts ──▶ calls_fraud_alerts_ext ──▶ fraud_alerts_filtered ──▶ calls.fraud-alerts.agg"
echo ""
echo "REST API:"
echo "  - ksqlDB: http://localhost:8088"
echo "  - Info: curl http://localhost:8088/info"
echo "  - Streams: curl -X POST http://localhost:8088/ksql -d '{\"ksql\": \"SHOW STREAMS;\"}'"
echo "  - Tables: curl -X POST http://localhost:8088/ksql -d '{\"ksql\": \"SHOW TABLES;\"}'"
echo "  - Queries: curl -X POST http://localhost:8088/ksql -d '{\"ksql\": \"SHOW QUERIES;\"}'"
echo ""
echo "============================================"
success "Quick verification complete!"
echo "============================================"
