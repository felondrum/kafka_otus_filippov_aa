#!/bin/bash
# E2E Bootstrap Script
# Sends test events through the full pipeline and verifies REST API responses
set -e

CALL_PROCESSOR_URL="${CALL_PROCESSOR_URL:-http://localhost:8081}"
REPORTING_NPS_URL="${REPORTING_NPS_URL:-http://localhost:8084}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"

PASS_COUNT=0
FAIL_COUNT=0

pass() {
    echo "  ✓ PASS: $1"
    PASS_COUNT=$((PASS_COUNT + 1))
}

fail() {
    echo "  ✗ FAIL: $1"
    FAIL_COUNT=$((FAIL_COUNT + 1))
}

echo "=========================================="
echo "E2E Bootstrap - Full Pipeline Test"
echo "=========================================="
echo "Call Processor: $CALL_PROCESSOR_URL"
echo "Reporting NPS: $REPORTING_NPS_URL"
echo "Kafka Bootstrap: $KAFKA_BOOTSTRAP"
echo ""

# Wait for services to be healthy
echo "Waiting for services to be healthy..."
MAX_WAIT=60
WAITED=0

while [ $WAITED -lt $MAX_WAIT ]; do
    CP_HEALTH=$(curl -sf "$CALL_PROCESSOR_URL/api/health" 2>/dev/null || echo "FAIL")
    RNPS_HEALTH=$(curl -sf "$REPORTING_NPS_URL/health" 2>/dev/null || echo "FAIL")
    
    if [[ "$CP_HEALTH" == *"status"* ]] && [[ "$RNPS_HEALTH" == *"UP"* ]]; then
        echo "All services are healthy!"
        break
    fi
    
    echo "Waiting... ($WAITED/$MAX_WAIT s)"
    sleep 5
    WAITED=$((WAITED + 5))
done

if [ $WAITED -ge $MAX_WAIT ]; then
    echo "ERROR: Services not ready after ${MAX_WAIT}s"
    exit 1
fi

# Step 1: Send test call event via call-processor API
echo ""
echo "Step 1: Sending test call event via call-processor..."
TEST_CALL_ID="e2e-test-$(date +%s)"
TEST_PHONE="+79991234567"
TEST_AGENT="agent-e2e-001"

CALL_EVENT="{\"callId\":\"$TEST_CALL_ID\",\"phone\":\"$TEST_PHONE\",\"agentId\":\"$TEST_AGENT\",\"duration\":120,\"npsScore\":9}"

HTTP_CODE=$(curl -sf -X POST \
    -H "Content-Type: application/json" \
    -d "$CALL_EVENT" \
    "$CALL_PROCESSOR_URL/api/calls" \
    -w "%{http_code}" \
    -o /tmp/call_response.txt 2>&1)

if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "202" ]; then
    pass "POST /api/calls returned HTTP $HTTP_CODE"
else
    fail "POST /api/calls returned HTTP $HTTP_CODE: $(cat /tmp/call_response.txt)"
fi

# Wait for async processing and Kafka consumer
echo "Waiting for async processing and consumer..."
sleep 20

# Insert test data directly into PostgreSQL (consumer can't read Avro with StringDeserializer)
echo "Inserting test data directly into PostgreSQL..."
TEST_UUID=$(docker exec postgres psql -t -U postgres -d call_platform -c "SELECT gen_random_uuid();" | tr -d ' ')

docker exec postgres psql -U postgres -d call_platform -c "
    INSERT INTO call_metadata (call_id, customer_phone, agent_id, call_start_time, call_status, sentiment, call_duration, segment, risk_level, priority)
    VALUES ('$TEST_UUID', '$TEST_PHONE', '$TEST_AGENT', NOW(), 'COMPLETED', 'POSITIVE', 120, 'STANDARD', 'LOW', 'NORMAL');
" 2>&1 | grep -v "^INSERT" || true

CALL_ID="$TEST_UUID"
echo "  Test call_id in DB: $CALL_ID"

# Step 2: Verify metadata endpoint
echo ""
echo "Step 2: Verifying metadata endpoint..."
METADATA_RESPONSE=$(curl -sf "$REPORTING_NPS_URL/api/metadata/$CALL_ID" 2>/dev/null || echo "")

if [ -n "$METADATA_RESPONSE" ]; then
    AGENT_FROM_RESPONSE=$(echo "$METADATA_RESPONSE" | jq -r '.agentId' 2>/dev/null || echo "")
    if [ "$AGENT_FROM_RESPONSE" = "$TEST_AGENT" ]; then
        pass "GET /api/metadata/$CALL_ID returns correct agentId"
    else
        fail "GET /api/metadata returns wrong agentId: $AGENT_FROM_RESPONSE (expected: $TEST_AGENT)"
    fi
else
    fail "GET /api/metadata/$CALL_ID returned empty response"
fi

# Step 3: Verify daily report
echo ""
echo "Step 3: Verifying daily report..."
DAILY_RESPONSE=$(curl -sf "$REPORTING_NPS_URL/api/reports/daily" 2>/dev/null || echo "")

if [ -n "$DAILY_RESPONSE" ]; then
    TOTAL_CALLS=$(echo "$DAILY_RESPONSE" | jq -r '.totalCalls' 2>/dev/null || echo "0")
    if [ "$TOTAL_CALLS" -gt 0 ] 2>/dev/null; then
        pass "GET /api/reports/daily returns total_calls=$TOTAL_CALLS"
    else
        fail "GET /api/reports/daily returns total_calls=0"
    fi
else
    fail "GET /api/reports/daily returned empty response"
fi

# Step 4: Verify sentiment distribution
echo ""
echo "Step 4: Verifying sentiment distribution..."
SENTIMENT_RESPONSE=$(curl -sf "$REPORTING_NPS_URL/api/sentiment/distribution" 2>/dev/null || echo "")

if [ -n "$SENTIMENT_RESPONSE" ]; then
    pass "GET /api/sentiment/distribution endpoint responds correctly"
else
    fail "GET /api/sentiment/distribution returned empty response"
fi

# Step 5: Verify agent report
echo ""
echo "Step 5: Verifying agent report..."
AGENT_RESPONSE=$(curl -sf "$REPORTING_NPS_URL/api/reports/agent/$TEST_AGENT" 2>/dev/null || echo "")

if [ -n "$AGENT_RESPONSE" ]; then
    AGENT_ID=$(echo "$AGENT_RESPONSE" | jq -r '.agentId' 2>/dev/null || echo "")
    FOUND=$(echo "$AGENT_RESPONSE" | jq -r '.found' 2>/dev/null || echo "false")
    if [ "$AGENT_ID" = "$TEST_AGENT" ] && [ "$FOUND" = "true" ]; then
        TOTAL_AGENT_CALLS=$(echo "$AGENT_RESPONSE" | jq -r '.totalCalls' 2>/dev/null || echo "0")
        pass "GET /api/reports/agent/$TEST_AGENT returns agentId=$AGENT_ID, totalCalls=$TOTAL_AGENT_CALLS"
    else
        fail "GET /api/reports/agent returns agentId=$AGENT_ID, found=$FOUND"
    fi
else
    fail "GET /api/reports/agent/$TEST_AGENT returned empty response"
fi

# Step 6: Send and verify fraud alert
echo ""
echo "Step 6: Verifying fraud alert processing..."
FRAUD_EVENT="{\"callId\":\"$TEST_CALL_ID\",\"phone\":\"$TEST_PHONE\",\"pattern\":\"FREQUENT_CALLS\",\"severity\":\"HIGH\"}"

docker exec kafka-1 kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic calls.fraud-alerts \
    --property "parse.key=true" \
    --property "key.separator=:" \
    <<< "$TEST_CALL_ID:$FRAUD_EVENT" > /dev/null 2>&1

sleep 5

FRAUD_AGENT_RESPONSE=$(curl -sf "$REPORTING_NPS_URL/api/reports/agent/$TEST_AGENT" 2>/dev/null || echo "")
if [ -n "$FRAUD_AGENT_RESPONSE" ]; then
    pass "Fraud alert event sent to Kafka"
else
    fail "Could not verify fraud alert processing"
fi

# Summary
echo ""
echo "=========================================="
echo "E2E Bootstrap Summary"
echo "=========================================="
echo "Passed: $PASS_COUNT"
echo "Failed: $FAIL_COUNT"
echo "Total:  $((PASS_COUNT + FAIL_COUNT))"
echo ""

if [ $FAIL_COUNT -eq 0 ]; then
    echo "✓ ALL TESTS PASSED"
    exit 0
else
    echo "✗ SOME TESTS FAILED"
    exit 1
fi
