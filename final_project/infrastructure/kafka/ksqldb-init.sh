#!/bin/bash
# ksqlDB Initialization Script
# Creates external streams and persistent tables for analytics
# This script runs automatically when docker compose starts

set -e

KSQLDB_URL="${KSQLDB_URL:-http://ksqldb-server:8088}"
KSQLDB_WAIT_RETRIES="${KSQLDB_WAIT_RETRIES:-30}"
KSQLDB_WAIT_INTERVAL="${KSQLDB_WAIT_INTERVAL:-5}"

echo "============================================"
echo "ksqlDB Initialization Script"
echo "============================================"
echo "ksqlDB URL: $KSQLDB_URL"

# Wait for ksqlDB to be ready
echo ""
echo "Waiting for ksqlDB to be ready..."
for i in $(seq 1 $KSQLDB_WAIT_RETRIES); do
    if curl -s "${KSQLDB_URL}/info" > /dev/null 2>&1; then
        echo "ksqlDB is ready!"
        break
    fi
    if [ $i -eq $KSQLDB_WAIT_RETRIES ]; then
        echo "ERROR: ksqlDB not ready after ${KSQLDB_WAIT_RETRIES} attempts"
        exit 1
    fi
    echo "  Attempt $i/$KSQLDB_WAIT_RETRIES - waiting ${KSQLDB_WAIT_INTERVAL}s..."
    sleep $KSQLDB_WAIT_INTERVAL
done

# Function to execute ksqlDB statement
execute_ksql() {
    local statement="$1"
    local description="$2"
    
    echo ""
    echo ">>> $description"
    echo "SQL: $statement"
    
    local response
    response=$(curl -s -X POST "${KSQLDB_URL}/ksql" \
        -H "Content-Type: application/vnd.ksql.v1+json" \
        -d "{\"ksql\": \"${statement}\"}" 2>&1)
    
    # Check for errors (ignore "already exists" errors)
    if echo "$response" | grep -q '"statement_error"'; then
        if echo "$response" | grep -qi 'already exists\|already registered'; then
            echo "  ✓ Already exists, skipping"
        else
            echo "  ERROR: $response"
            return 1
        fi
    elif echo "$response" | grep -q '"commandStatus"'; then
        echo "  ✓ Success"
    else
        echo "  ✓ Response: $response"
    fi
}

echo ""
echo "============================================"
echo "Step 1: Creating external streams"
echo "============================================"

# Create external stream for calls.completed
execute_ksql \
    "CREATE STREAM calls_completed_ext (callId VARCHAR, phone VARCHAR, duration BIGINT, agentId VARCHAR, npsScore INT, completedAt BIGINT) WITH (KAFKA_TOPIC='calls.completed', VALUE_FORMAT='JSON');" \
    "Creating external stream for calls.completed topic"

# Create external stream for calls.fraud-alerts
execute_ksql \
    "CREATE STREAM calls_fraud_alerts_ext (phone VARCHAR, pattern VARCHAR, severity VARCHAR, count BIGINT) WITH (KAFKA_TOPIC='calls.fraud-alerts', VALUE_FORMAT='JSON');" \
    "Creating external stream for calls.fraud-alerts topic"

echo ""
echo "============================================"
echo "Step 2: Creating persistent aggregation tables"
echo "============================================"

# Create calls_completed_agg table (TUMBLING window)
execute_ksql \
    "CREATE TABLE calls_completed_agg WITH (KAFKA_TOPIC='calls.completed.agg', PARTITIONS=6) AS SELECT agentId, COUNT(*) AS call_count, AVG(npsScore) AS avg_nps_score, MAX(completedAt) AS last_call_at FROM calls_completed_ext WINDOW TUMBLING (SIZE 5 MINUTES, GRACE PERIOD 1 MINUTE) GROUP BY agentId EMIT CHANGES;" \
    "Creating persistent aggregation table: calls_completed_agg (TUMBLING 5min window)"

# Create fraud_alerts_filtered table (simple GROUP BY, no window - aggregation by phone)
# Note: HOPPING window with composite key requires websocket endpoint, using simple aggregation
execute_ksql \
    "CREATE TABLE fraud_alerts_filtered WITH (KAFKA_TOPIC='calls.fraud-alerts.agg', PARTITIONS=6) AS SELECT phone, COUNT(*) AS alert_count FROM calls_fraud_alerts_ext GROUP BY phone EMIT CHANGES;" \
    "Creating persistent aggregation table: fraud_alerts_filtered (GROUP BY phone)"

echo ""
echo "============================================"
echo "Step 3: Verifying queries"
echo "============================================"

# Show all queries
echo ""
echo "Active ksqlDB queries:"
curl -s -X POST "${KSQLDB_URL}/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json" \
    -d '{"ksql": "SHOW QUERIES;"}' | python3 -m json.tool 2>/dev/null || \
    curl -s -X POST "${KSQLDB_URL}/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json" \
    -d '{"ksql": "SHOW QUERIES;"}'

echo ""
echo "============================================"
echo "ksqlDB initialization complete!"
echo "============================================"
echo ""
echo "Available endpoints:"
echo "  - ksqlDB REST API: http://localhost:8088"
echo "  - ksqlDB info: curl http://localhost:8088/info"
echo "  - Show streams: curl -X POST http://localhost:8088/ksql -H 'Content-Type: application/vnd.ksql.v1+json' -d '{\"ksql\": \"SHOW STREAMS;\"}'"
echo "  - Show tables: curl -X POST http://localhost:8088/ksql -H 'Content-Type: application/vnd.ksql.v1+json' -d '{\"ksql\": \"SHOW TABLES;\"}'"
echo "  - Show queries: curl -X POST http://localhost:8088/ksql -H 'Content-Type: application/vnd.ksql.v1+json' -d '{\"ksql\": \"SHOW QUERIES;\"}'"
echo ""
echo "Output topics:"
echo "  - calls.completed.agg (agentId, call_count, avg_nps_score, last_call_at)"
echo "  - calls.fraud-alerts.agg (phone, alert_count)"
echo ""
