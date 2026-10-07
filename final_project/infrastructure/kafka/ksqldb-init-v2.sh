#!/bin/bash
# ksqldb-init.sh — Initialize ksqlDB streams and tables
# Usage: docker exec ksqldb-init bash /tmp/ksqldb-init.sh

set -e

KSQLDB_URL="${KSQLDB_URL:-http://ksqldb-server:8088}"
RETRIES="${KSQLDB_WAIT_RETRIES:-30}"
WAIT_INTERVAL="${KSQLDB_WAIT_INTERVAL:-5}"

echo "============================================"
echo "ksqlDB Initialization"
echo "============================================"
echo "ksqlDB URL: $KSQLDB_URL"
echo ""

# Function to execute ksql
execute_ksql() {
    local ksql=$1
    echo "Executing: $ksql"
    
    local response=$(curl -s -X POST "$KSQLDB_URL/ksql" \
        -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
        -d "{\"ksql\": \"$ksql\"}")
    
    echo "Response: $response"
    echo ""
}

# Wait for ksqlDB to be ready
echo "Waiting for ksqlDB to be ready..."
for i in $(seq 1 $RETRIES); do
    if curl -sf "$KSQLDB_URL/info" > /dev/null 2>&1; then
        echo "ksqlDB is ready!"
        break
    fi
    if [ "$i" -eq "$RETRIES" ]; then
        echo "ERROR: ksqlDB not ready after $RETRIES retries"
        exit 1
    fi
    echo "Attempt $i/$RETRIES - ksqlDB not ready, waiting ${WAIT_INTERVAL}s..."
    sleep $WAIT_INTERVAL
done

# Step 1: Drop existing persistent queries (idempotent)
echo "============================================"
echo "Step 1: Dropping existing persistent queries"
echo "============================================"

execute_ksql "DROP QUERY IF EXISTS CTAS_CALLS_COMPLETED_AGG_3;"
execute_ksql "DROP QUERY IF EXISTS CTAS_FRAUD_ALERTS_FILTERED_5;"

# Step 2: Drop existing tables (will also drop output topics)
echo "============================================"
echo "Step 2: Dropping existing tables"
echo "============================================"

execute_ksql "DROP TABLE IF EXISTS CALLS_COMPLETED_AGG;"
execute_ksql "DROP TABLE IF EXISTS FRAUD_ALERTS_FILTERED;"

# Step 3: Create external streams
echo "============================================"
echo "Step 3: Creating external streams"
echo "============================================"

execute_ksql "CREATE STREAM CALLS_COMPLETED_EXT (
    CALLID VARCHAR,
    PHONE VARCHAR,
    DURATION BIGINT,
    AGENTID VARCHAR,
    NPSSCORE INT,
    COMPLETEDAT BIGINT
) WITH (KAFKA_TOPIC='calls.completed', VALUE_FORMAT='JSON', PARTITIONS=6);"

execute_ksql "CREATE STREAM CALLS_FRAUD_ALERTS_EXT (
    PHONE VARCHAR,
    PATTERN VARCHAR,
    SEVERITY VARCHAR,
    COUNT BIGINT
) WITH (KAFKA_TOPIC='calls.fraud-alerts', VALUE_FORMAT='JSON', PARTITIONS=6);"

# Step 4: Create persistent aggregation tables
echo "============================================"
echo "Step 4: Creating persistent aggregation tables"
echo "============================================"

execute_ksql "CREATE TABLE CALLS_COMPLETED_AGG WITH (KAFKA_TOPIC='calls.completed.agg', PARTITIONS=6, VALUE_FORMAT='JSON') AS
SELECT
    AGENTID,
    COUNT(*) AS CALL_COUNT,
    AVG(NPSSCORE) AS AVG_NPS_SCORE,
    MAX(COMPLETEDAT) AS LAST_CALL_AT
FROM CALLS_COMPLETED_EXT
WINDOW TUMBLING (SIZE 5 MINUTES, GRACE PERIOD 1 MINUTES)
GROUP BY AGENTID
EMIT CHANGES;"

execute_ksql "CREATE TABLE FRAUD_ALERTS_FILTERED WITH (KAFKA_TOPIC='calls.fraud-alerts.agg', PARTITIONS=6, VALUE_FORMAT='JSON') AS
SELECT
    PHONE,
    COUNT(*) AS ALERT_COUNT
FROM CALLS_FRAUD_ALERTS_EXT
GROUP BY PHONE
EMIT CHANGES;"

# Step 5: Verify
echo "============================================"
echo "Step 5: Verification"
echo "============================================"

echo "Streams:"
curl -s -X POST "$KSQLDB_URL/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
    -d '{"ksql": "SHOW STREAMS;"}' | python3 -m json.tool 2>/dev/null || \
    curl -s -X POST "$KSQLDB_URL/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
    -d '{"ksql": "SHOW STREAMS;"}'

echo ""
echo "Tables:"
curl -s -X POST "$KSQLDB_URL/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
    -d '{"ksql": "SHOW TABLES;"}' | python3 -m json.tool 2>/dev/null || \
    curl -s -X POST "$KSQLDB_URL/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
    -d '{"ksql": "SHOW TABLES;"}'

echo ""
echo "Queries:"
curl -s -X POST "$KSQLDB_URL/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
    -d '{"ksql": "SHOW QUERIES;"}' | python3 -m json.tool 2>/dev/null || \
    curl -s -X POST "$KSQLDB_URL/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
    -d '{"ksql": "SHOW QUERIES;"}'

echo ""
echo "============================================"
echo "ksqlDB Initialization Complete"
echo "============================================"
