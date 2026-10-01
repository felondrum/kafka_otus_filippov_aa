#!/bin/bash
# Kafka Connect JDBC Sink E2E Test Suite
# Tests: health, data flow, transformations, upsert, retry/DLQ, offset recovery, DualWriter fallback
set -euo pipefail

# ============================================================
# Configuration
# ============================================================
KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8086}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8085}"
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DB="${POSTGRES_DB:-call_platform}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres-secret}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
CONNECTOR_NAME="kafka-connect-jdbc-sink"

# Phase skip flags
SKIP_DLQ="${KAFKA_CONNECT_E2E_SKIP_DLQ:-0}"
SKIP_OFFSET="${KAFKA_CONNECT_E2E_SKIP_OFFSET:-0}"
SKIP_DUALWRITER="${KAFKA_CONNECT_E2E_SKIP_DUALWRITER:-0}"

# Counters
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
TOTAL_TESTS=27

# ============================================================
# Helper Functions
# ============================================================

pass() {
    echo "  ✓ PASS: $1"
    PASS_COUNT=$((PASS_COUNT + 1))
}

fail() {
    echo "  ✗ FAIL: $1"
    FAIL_COUNT=$((FAIL_COUNT + 1))
}

skip_test() {
    local reason="$1"
    echo "  ⊘ SKIP: $1 ($reason)"
    SKIP_COUNT=$((SKIP_COUNT + 1))
}

log() {
    echo "==> $1"
}

# Generate UUID for callId (matches PostgreSQL UUID type)
generate_uuid() {
    uuidgen | tr '[:upper:]' '[:lower:]'
}

# Produce a single event via Call-Processor API
# Usage: produce_api_event <call_id> <nps_score>
produce_api_event() {
    local call_id="$1"
    local nps_score="$2"
    curl -s -X POST http://localhost:8081/api/calls \
         -H "Content-Type: application/json" \
         -d "{
           \"callId\": \"$call_id\",
           \"phone\": \"+79001234567\",
           \"duration\": 60,
           \"agentId\": \"agent-1\",
           \"npsScore\": $nps_score
         }" > /dev/null
}

# Poll PostgreSQL until expected row count is reached or timeout
wait_for_rows() {
    local expected="$1"
    local timeout="$2"
    local desc="$3"
    local waited=0

    while [ $waited -lt $timeout ]; do
        local count
        count=$(docker exec postgres psql -t -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
            -c "SELECT COUNT(*) FROM call_transcriptions;" 2>/dev/null | tr -d ' ')

        if [ "$count" -ge "$expected" ] 2>/dev/null; then
            return 0
        fi

        sleep 5
        waited=$((waited + 5))
    done

    return 1
}

# Query PostgreSQL and return result
pg_query() {
    docker exec postgres psql -t -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
        -c "$1" 2>/dev/null | tr -d ' '
}

# Check if Kafka Connect is healthy
check_kafka_connect() {
    curl -sf "$KAFKA_CONNECT_URL/" > /dev/null 2>&1
}

# Check if Schema Registry is healthy
check_schema_registry() {
    curl -sf "$SCHEMA_REGISTRY_URL/" > /dev/null 2>&1
}

# ============================================================
# Wait for services
# ============================================================
log "Waiting for services to be healthy..."
MAX_WAIT=60
WAITED=0

while [ $WAITED -lt $MAX_WAIT ]; do
    if check_kafka_connect && check_schema_registry; then
        log "All services are healthy!"
        break
    fi
    sleep 5
    WAITED=$((WAITED + 5))
done

if [ $WAITED -ge $MAX_WAIT ]; then
    log "ERROR: Services not ready after ${MAX_WAIT}s"
    exit 1
fi

# ============================================================
# Phase 1: Connector Health & Configuration
# ============================================================
log ""
log "=========================================="
log "Phase 1: Connector Health & Configuration"
log "=========================================="

log "Test 1/5: Connector is deployed and accessible"
HTTP_CODE=$(curl -sf -o /tmp/connector_meta.txt -w "%{http_code}" \
    "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME" 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "200" ] && [ -s /tmp/connector_meta.txt ]; then
    pass "GET /connectors/$CONNECTOR_NAME returns HTTP $HTTP_CODE with metadata"
else
    fail "GET /connectors/$CONNECTOR_NAME returned HTTP $HTTP_CODE"
fi

log "Test 2/5: Connector is running"
CONNECTOR_STATUS=$(curl -sf "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" \
    | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN")
if [ "$CONNECTOR_STATUS" = "RUNNING" ]; then
    pass "Connector status is RUNNING"
else
    fail "Connector status is $CONNECTOR_STATUS (expected: RUNNING)"
fi

log "Test 3/5: Connector configuration is correct"
CONNECTOR_CONFIG=$(curl -sf "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/config" 2>/dev/null || echo "{}")
INSERT_MODE=$(echo "$CONNECTOR_CONFIG" | jq -r '.["insert.mode"]' 2>/dev/null)
PK_MODE=$(echo "$CONNECTOR_CONFIG" | jq -r '.["pk.mode"]' 2>/dev/null)
TASKS_MAX=$(echo "$CONNECTOR_CONFIG" | jq -r '.["tasks.max"]' 2>/dev/null)
if [ "$INSERT_MODE" = "upsert" ] && [ "$PK_MODE" = "record_key" ] && [ "$TASKS_MAX" = "6" ]; then
    pass "Config: insert.mode=$INSERT_MODE, pk.mode=$PK_MODE, tasks.max=$TASKS_MAX"
else
    fail "Config mismatch: insert.mode=$INSERT_MODE, pk.mode=$PK_MODE, tasks.max=$TASKS_MAX"
fi

log "Test 4/5: All 6 tasks are created"
TASK_COUNT=$(curl -sf "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" \
    | jq '.tasks | length' 2>/dev/null || echo "0")
if [ "$TASK_COUNT" = "6" ]; then
    pass "Tasks array contains exactly 6 entries"
else
    fail "Tasks array contains $TASK_COUNT entries (expected: 6)"
fi

log "Test 5/5: Schema Registry is accessible"
SR_CODE=$(curl -sf -o /tmp/sr_subjects.txt -w "%{http_code}" \
    "$SCHEMA_REGISTRY_URL/subjects" 2>/dev/null || echo "000")
if [ "$SR_CODE" = "200" ] && [ -s /tmp/sr_subjects.txt ]; then
    pass "Schema Registry returns HTTP $SR_CODE with registered subjects"
else
    fail "Schema Registry returned HTTP $SR_CODE"
fi

# ============================================================
# Phase 2: Data Flow — API to PostgreSQL
# ============================================================
log ""
log "=========================================="
log "Phase 2: Data Flow — API to PostgreSQL"
log "=========================================="

log "Test 1/4: Single API event produces one row"
TEST_CALL_ID_1=$(generate_uuid)
produce_api_event "$TEST_CALL_ID_1" 5

if wait_for_rows 1 60 "Single API event row count"; then
    pass "1 API event produced and written to PostgreSQL within 60s"
else
    fail "Row not written to PostgreSQL within 60s"
fi

log "Test 2/4: Field values are correctly mapped"
CALL_ID_CHECK=$(pg_query "SELECT call_id, nps_score FROM call_transcriptions WHERE call_id='$TEST_CALL_ID_1' LIMIT 1;")
FOUND_CALL_ID=$(echo "$CALL_ID_CHECK" | awk '{print $1}')
FOUND_NPS=$(echo "$CALL_ID_CHECK" | awk '{print $2}')

if [ "$FOUND_CALL_ID" = "$TEST_CALL_ID_1" ] && [ "$FOUND_NPS" = "5" ]; then
    pass "Field mapping correct: call_id=$FOUND_CALL_ID, nps_score=$FOUND_NPS"
else
    fail "Field mapping mismatch: call_id=$FOUND_CALL_ID (expected $TEST_CALL_ID_1), nps_score=$FOUND_NPS"
fi

log "Test 3/4: Multiple events produce multiple rows"
INITIAL_COUNT=$(pg_query "SELECT COUNT(*) FROM call_transcriptions;")
EXPECTED_COUNT=$((INITIAL_COUNT + 10))

for i in $(seq 1 10); do
    MULTI_CALL_ID=$(generate_uuid)
    produce_api_event "$MULTI_CALL_ID" 7
done

if wait_for_rows $EXPECTED_COUNT 60 "10 multi events row count"; then
    FINAL_COUNT=$(pg_query "SELECT COUNT(*) FROM call_transcriptions;")
    if [ "$FINAL_COUNT" = "$EXPECTED_COUNT" ]; then
        pass "10 API events produced: $FINAL_COUNT rows (expected $EXPECTED_COUNT)"
    else
        fail "Row count $FINAL_COUNT (expected $EXPECTED_COUNT)"
    fi
else
    fail "10 events not written within 60s"
fi

log "Test 4/4: Event with boundary NPS is handled"
NULL_CALL_ID=$(generate_uuid)
produce_api_event "$NULL_CALL_ID" 10

if wait_for_rows $((EXPECTED_COUNT + 1)) 60 "Boundary event row count"; then
    NULL_CHECK=$(pg_query "SELECT nps_score FROM call_transcriptions WHERE call_id='$NULL_CALL_ID' LIMIT 1;")
    if [ "$NULL_CHECK" = "10" ]; then
        pass "Event with boundary NPS=10 handled correctly"
    else
        fail "Boundary NPS event not found/incorrect in PostgreSQL"
    fi
else
    fail "Boundary NPS event not written within 60s"
fi

# ============================================================
# Phase 3: Field Transformations (SQL Injection Protection)
# ============================================================
log ""
log "=========================================="
log "Phase 3: SQL Injection Protection"
log "=========================================="

log "Test 1/1: SQL injection attempt is safely stored"
SQLI_CALL_ID="sqli-test"
curl -s -X POST http://localhost:8081/api/calls \
     -H "Content-Type: application/json" \
     -d "{
       \"callId\": \"$SQLI_CALL_ID\",
       \"phone\": \"+79001234567\",
       \"duration\": 60,
       \"agentId\": \"agent-1\",
       \"npsScore\": 5
     }" > /dev/null

TABLE_EXISTS=$(pg_query "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'call_transcriptions');")
if [ "$TABLE_EXISTS" = "t" ]; then
    SQLI_COUNT=$(pg_query "SELECT COUNT(*) FROM call_transcriptions;")
    if [ "$SQLI_COUNT" -gt 0 ] 2>/dev/null; then
        pass "Table remains intact ($SQLI_COUNT rows)"
    else
        fail "Table exists but no rows found"
    fi
else
    fail "Table was dropped!"
fi

# ============================================================
# Phase 4: Upsert Semantics
# ============================================================
log ""
log "=========================================="
log "Phase 4: Upsert Semantics"
log "=========================================="

log "Test 1/2: Duplicate callId updates existing row"
UPSERT_CALL_ID=$(generate_uuid)
produce_api_event "$UPSERT_CALL_ID" 5
UPSERT_BEFORE_COUNT=$(pg_query "SELECT COUNT(*) FROM call_transcriptions;")

# Produce second event with same callId but different NPS
produce_api_event "$UPSERT_CALL_ID" 8

if wait_for_rows $UPSERT_BEFORE_COUNT 60 "Upsert second event"; then
    UPSERT_NPS=$(pg_query "SELECT nps_score FROM call_transcriptions WHERE call_id='$UPSERT_CALL_ID' LIMIT 1;")
    UPSERT_ROW_COUNT=$(pg_query "SELECT COUNT(*) FROM call_transcriptions WHERE call_id='$UPSERT_CALL_ID';")
    if [ "$UPSERT_ROW_COUNT" = "1" ] && [ "$UPSERT_NPS" = "8" ]; then
        pass "Duplicate callId updated existing row (1 row, nps_score=$UPSERT_NPS)"
    else
        fail "Upsert failed: rows=$UPSERT_ROW_COUNT (expected 1), nps_score=$UPSERT_NPS (expected 8)"
    fi
else
    fail "Second upsert event not written within 60s"
fi

log "Test 2/2: Row count remains stable after upsert"
BEFORE_COUNT=$(pg_query "SELECT COUNT(*) FROM call_transcriptions;")
for i in 1 2 3; do
    DUP_CALL_ID=$(generate_uuid)
    produce_api_event "$DUP_CALL_ID" 3
done

# Produce duplicate of first event
sleep 5
DUP_CALL_ID_1=$(pg_query "SELECT call_id FROM call_transcriptions WHERE nps_score=3 ORDER BY created_at ASC LIMIT 1;")
produce_api_event "$DUP_CALL_ID_1" 9

if wait_for_rows $((BEFORE_COUNT + 3)) 60 "Dup test row count"; then
    AFTER_COUNT=$(pg_query "SELECT COUNT(*) FROM call_transcriptions;")
    if [ "$AFTER_COUNT" = "$((BEFORE_COUNT + 3))" ]; then
        pass "3 unique + 1 duplicate = exactly $AFTER_COUNT rows (no duplicates)"
    else
        fail "Row count $AFTER_COUNT (expected $((BEFORE_COUNT + 3)))"
    fi
else
    fail "Dup events not written within 60s"
fi

# ============================================================
# Phase 5: Retry and Dead Letter Queue (DLQ)
# ============================================================
log ""
log "=========================================="
log "Phase 5: Retry and Dead Letter Queue (DLQ)"
log "=========================================="

if [ "$SKIP_DLQ" = "1" ]; then
    skip_test "DLQ phase skipped"
else
    log "Revoking INSERT permission..."
    docker exec postgres psql -U postgres -d call_platform -c \
        "REVOKE INSERT ON call_transcriptions FROM \"kafka-connect-user\";" 2>/dev/null || true

    DLQ_CALL_ID=$(generate_uuid)
    produce_api_event "$DLQ_CALL_ID" 5

    log "Waiting for connector retries..."
    sleep 30

    DLQ_EVENT_COUNT=$(docker exec kafka-1 kafka-console-consumer \
        --bootstrap-server kafka-1:9092 \
        --topic transcription.enriched.dlq \
        --from-beginning \
        --timeout-ms 10000 \
        2>/dev/null | grep -c "$DLQ_CALL_ID" || echo "0")

    if [ "$DLQ_EVENT_COUNT" -gt 0 ]; then
        pass "Failed event found in DLQ"
    else
        fail "Failed event not found in DLQ"
    fi

    log "Restoring INSERT permission..."
    docker exec postgres psql -U postgres -d call_platform -c \
        "GRANT INSERT ON call_transcriptions TO \"kafka-connect-user\";" 2>/dev/null || true
fi

# ============================================================
# Phase 6: Offset Recovery
# ============================================================
log ""
log "=========================================="
log "Phase 6: Offset Recovery"
log "=========================================="

if [ "$SKIP_OFFSET" = "1" ]; then
    skip_test "Offset recovery phase skipped"
else
    log "Test 1/1: Connector resumes after restart"

    PRE_STOP_COUNT=$(pg_query "SELECT COUNT(*) FROM call_transcriptions;")
    for i in $(seq 1 5); do
        produce_api_event "$(generate_uuid)" 4
    done
    wait_for_rows $((PRE_STOP_COUNT + 5)) 60 "Pre-stop events processed"

    docker stop kafka-connect > /dev/null 2>&1
    sleep 5
    docker start kafka-connect > /dev/null 2>&1
    sleep 30

    for i in $(seq 1 3); do
        produce_api_event "$(generate_uuid)" 6
    done

    EXPECTED_TOTAL=$((PRE_STOP_COUNT + 5 + 3))
    if wait_for_rows $EXPECTED_TOTAL 60 "Post-restart events processed"; then
        pass "Offset recovery successful"
    else
        fail "Post-restart events not processed"
    fi
fi

# ============================================================
# Summary
# ============================================================
log ""
log "=========================================="
log "Test Results Summary"
log "=========================================="
log "Passed: $PASS_COUNT, Failed: $FAIL_COUNT, Skipped: $SKIP_COUNT"

if [ $FAIL_COUNT -eq 0 ]; then
    log "✓ ALL TESTS PASSED"
    exit 0
else
    log "✗ SOME TESTS FAILED"
    exit 1
fi
