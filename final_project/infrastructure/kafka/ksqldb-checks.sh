#!/bin/bash
# ksqlDB Quick Checks — набор curl-команд для быстрой проверки
# Usage: ./ksqldb-checks.sh [command]
#   Без аргументов — показывает все доступные команды
#   С аргументом — выполняет конкретную команду

KSQLDB_URL="${KSQLDB_URL:-http://localhost:8088}"
CONTENT_TYPE="Content-Type: application/vnd.ksql.v1+json"

show_help() {
    echo "============================================"
    echo "ksqlDB Quick Checks"
    echo "============================================"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Available commands:"
    echo ""
    echo "  Info & Status:"
    echo "    info              — ksqlDB server info (version, cluster)"
    echo "    status            — server health status"
    echo ""
    echo "  Streams & Tables:"
    echo "    streams           — list all external streams"
    echo "    tables            — list all persistent tables"
    echo "    topics            — list Kafka topics visible to ksqlDB"
    echo ""
    echo "  Queries:"
    echo "    queries           — show all persistent queries + status"
    echo "    queries-running   — count of RUNNING queries"
    echo ""
    echo "  Aggregation Data:"
    echo "    agg-calls         — show calls_completed_agg (latest records)"
    echo "    agg-fraud         — show fraud_alerts_filtered (latest records)"
    echo "    agg-calls-count   — total agents with calls"
    echo "    agg-fraud-count   — total phones with alerts"
    echo ""
    echo "  Output Topics:"
    echo "    topic-schema      — show output topics schemas"
    echo ""
    echo "  Metrics:"
    echo "    metrics           — ksqlDB internal metrics"
    echo ""
    echo "  Examples:"
    echo "    $0 info"
    echo "    $0 queries"
    echo "    $0 agg-calls"
    echo "    $0 agg-fraud"
    echo ""
    echo "============================================"
}

# Helper function to execute ksqlDB query
query_ksql() {
    local sql="$1"
    local description="$2"
    
    if [ -n "$description" ]; then
        echo ""
        echo ">>> $description"
    fi
    
    curl -s -X POST "${KSQLDB_URL}/ksql" \
        -H "$CONTENT_TYPE" \
        -d "{\"ksql\": \"${sql}\"}" | python3 -m json.tool 2>/dev/null || \
        curl -s -X POST "${KSQLDB_URL}/ksql" \
        -H "$CONTENT_TYPE" \
        -d "{\"ksql\": \"${sql}\"}"
    
    echo ""
}

case "${1}" in
    # ==================== Info & Status ====================
    info)
        echo "============================================"
        echo "ksqlDB Server Info"
        echo "============================================"
        curl -s "${KSQLDB_URL}/info" | python3 -m json.tool
        echo ""
        ;;
    
    status)
        echo "============================================"
        echo "ksqlDB Server Status"
        echo "============================================"
        curl -s "${KSQLDB_URL}/health/live" 2>&1 || echo "Live check: N/A"
        echo ""
        curl -s "${KSQLDB_URL}/health/ready" 2>&1 || echo "Ready check: N/A"
        echo ""
        ;;
    
    # ==================== Streams & Tables ====================
    streams)
        echo "============================================"
        echo "External Streams (Kafka topic readers)"
        echo "============================================"
        query_ksql "SHOW STREAMS;" "Available external streams"
        
        echo ""
        echo "Streams details:"
        query_ksql "SHOW STREAMS EXTENDED;" "Extended stream info"
        ;;
    
    tables)
        echo "============================================"
        echo "Persistent Tables (aggregation results)"
        echo "============================================"
        query_ksql "SHOW TABLES;" "Available persistent tables"
        
        echo ""
        echo "Tables details:"
        query_ksql "SHOW TABLES EXTENDED;" "Extended table info"
        ;;
    
    topics)
        echo "============================================"
        echo "Kafka Topics (visible to ksqlDB)"
        echo "============================================"
        query_ksql "SHOW TOPICS;" "All Kafka topics"
        ;;
    
    # ==================== Queries ====================
    queries)
        echo "============================================"
        echo "Persistent Queries"
        echo "============================================"
        query_ksql "SHOW QUERIES;" "All persistent queries with status"
        
        echo ""
        echo "Query details:"
        query_ksql "SHOW QUERIES EXTENDED;" "Extended query info"
        ;;
    
    queries-running)
        echo "============================================"
        echo "Running Queries Count"
        echo "============================================"
        RESULT=$(curl -s -X POST "${KSQLDB_URL}/ksql" \
            -H "$CONTENT_TYPE" \
            -d '{"ksql": "SHOW QUERIES;"}')
        
        COUNT=$(echo "$RESULT" | python3 -c "
import sys, json
data = json.load(sys.stdin)
queries = data[0].get('queries', [])
running = sum(q.get('statusCount', {}).get('RUNNING', 0) for q in queries)
print(running)
" 2>/dev/null)
        
        echo "Running queries: ${COUNT:-0}"
        echo ""
        if [ "${COUNT:-0}" -ge 2 ]; then
            echo "✓ All expected queries are running"
        else
            echo "⚠ Expected 2 running queries (calls_completed_agg, fraud_alerts_filtered)"
        fi
        echo ""
        ;;
    
    # ==================== Aggregation Data ====================
    agg-calls)
        echo "============================================"
        echo "Calls Completed Aggregation (calls_completed_agg)"
        echo "============================================"
        echo ""
        echo "Schema: agentId (STRING), call_count (BIGINT), avg_nps_score (DOUBLE), last_call_at (LONG)"
        echo ""
        echo "Output topic: calls.completed.agg"
        echo ""
        echo "Query status and offsets (shows processing progress):"
        echo ""
        
        curl -s -X POST "${KSQLDB_URL}/ksql" \
            -H "$CONTENT_TYPE" \
            -d '{"ksql": "SHOW QUERIES EXTENDED;"}' | python3 -m json.tool | grep -A 50 "CALLS_COMPLETED" | head -40
        
        echo ""
        echo "Note: PRINT/SELECT queries require websocket endpoint (/query)"
        echo "To view aggregation data, consume from Kafka topic: calls.completed.agg"
        echo ""
        ;;
    
    agg-fraud)
        echo "============================================"
        echo "Fraud Alerts Aggregation (fraud_alerts_filtered)"
        echo "============================================"
        echo ""
        echo "Schema: phone (STRING), alert_count (BIGINT)"
        echo ""
        echo "Output topic: calls.fraud-alerts.agg"
        echo ""
        echo "Query status and offsets (shows processing progress):"
        echo ""
        
        curl -s -X POST "${KSQLDB_URL}/ksql" \
            -H "$CONTENT_TYPE" \
            -d '{"ksql": "SHOW QUERIES EXTENDED;"}' | python3 -m json.tool | grep -A 30 "FRAUD_ALERTS" | head -30
        
        echo ""
        echo "Note: PRINT/SELECT queries require websocket endpoint (/query)"
        echo "To view aggregation data, consume from Kafka topic: calls.fraud-alerts.agg"
        echo ""
        ;;
    
    agg-calls-count)
        echo "============================================"
        echo "Calls Aggregation — Agent Count"
        echo "============================================"
        echo ""
        echo "Checking query processing status..."
        echo ""
        
        curl -s -X POST "${KSQLDB_URL}/ksql" \
            -H "$CONTENT_TYPE" \
            -d '{"ksql": "SHOW QUERIES EXTENDED;"}' | python3 -c "
import sys, json
data = json.load(sys.stdin)
queries = data[0].get('queryDescriptions', [])
for q in queries:
    if 'CALLS_COMPLETED' in q.get('id', ''):
        tasks = q.get('tasksMetadata', [])
        total_committed = 0
        for t in tasks:
            for o in t.get('topicOffsets', []):
                tp = o.get('topicPartitionEntity', {})
                if 'calls.completed' in tp.get('topic', ''):
                    total_committed += o.get('committedOffset', 0)
        print(f'  Total records processed: {total_committed}')
" 2>/dev/null
        echo ""
        ;;
    
    agg-fraud-count)
        echo "============================================"
        echo "Fraud Aggregation — Phone Count"
        echo "============================================"
        echo ""
        echo "Checking query processing status..."
        echo ""
        
        curl -s -X POST "${KSQLDB_URL}/ksql" \
            -H "$CONTENT_TYPE" \
            -d '{"ksql": "SHOW QUERIES EXTENDED;"}' | python3 -c "
import sys, json
data = json.load(sys.stdin)
queries = data[0].get('queryDescriptions', [])
for q in queries:
    if 'FRAUD_ALERTS' in q.get('id', ''):
        tasks = q.get('tasksMetadata', [])
        total_committed = 0
        for t in tasks:
            for o in t.get('topicOffsets', []):
                tp = o.get('topicPartitionEntity', {})
                if 'calls.fraud-alerts' in tp.get('topic', ''):
                    total_committed += o.get('committedOffset', 0)
        print(f'  Total records processed: {total_committed}')
" 2>/dev/null
        echo ""
        ;;
    
    # ==================== Output Topics ====================
    topic-schema)
        echo "============================================"
        echo "Output Topics Schemas"
        echo "============================================"
        echo ""
        
        echo "=== calls.completed.agg ==="
        echo "Key: agentId (STRING)"
        echo "Value:"
        echo "  - call_count: BIGINT"
        echo "  - avg_nps_score: DOUBLE"
        echo "  - last_call_at: LONG"
        echo ""
        
        echo "=== calls.fraud-alerts.agg ==="
        echo "Key: phone (STRING)"
        echo "Value:"
        echo "  - alert_count: BIGINT"
        echo ""
        
        echo "Topic details from ksqlDB:"
        query_ksql "DESCRIBE calls.completed.agg;" "calls.completed.agg schema"
        query_ksql "DESCRIBE calls.fraud-alerts.agg;" "calls.fraud-alerts.agg schema"
        ;;
    
    # ==================== Metrics ====================
    metrics)
        echo "============================================"
        echo "ksqlDB Internal Metrics"
        echo "============================================"
        echo ""
        
        echo "Query metrics:"
        query_ksql "SHOW QUERIES;" "Query status and metrics"
        
        echo ""
        echo "Thread saturation (should be < 1.0):"
        echo "  Check ksqldb-server logs for 'Reporting query saturation' entries"
        echo ""
        docker logs ksqldb-server 2>&1 | grep "saturation" | tail -5
        ;;
    
    # ==================== All Checks ====================
    all)
        echo "============================================"
        echo "ksqlDB — Full Status Check"
        echo "============================================"
        echo ""
        
        echo "1. Server Info:"
        curl -s "${KSQLDB_URL}/info" | python3 -c "
import sys, json
info = json.load(sys.stdin)
ksql = info.get('KsqlServerInfo', {})
print(f\"   Version: {ksql.get('version', 'N/A')}\")
print(f\"   Cluster: {ksql.get('kafkaClusterId', 'N/A')[:16]}...\")
print(f\"   Service: {ksql.get('ksqlServiceId', 'N/A')}\")
print(f\"   Status:  {ksql.get('serverStatus', 'N/A')}\")
"
        echo ""
        
        echo "2. Streams:"
        query_ksql "SHOW STREAMS;" "" | python3 -c "
import sys, json
data = json.load(sys.stdin)
streams = data[0].get('streams', [])
for s in streams:
    print(f\"   ✓ {s['name']} → {s['topic']} ({s['valueFormat']})\")
if not streams:
    print('   ⚠ No streams found')
"
        echo ""
        
        echo "3. Tables:"
        query_ksql "SHOW TABLES;" "" | python3 -c "
import sys, json
data = json.load(sys.stdin)
tables = data[0].get('tables', [])
for t in tables:
    windowed = ' [windowed]' if t.get('isWindowed') else ''
    print(f\"   ✓ {t['name']}{windowed} → {t['topic']}\")
if not tables:
    print('   ⚠ No tables found')
"
        echo ""
        
        echo "4. Queries:"
        query_ksql "SHOW QUERIES;" "" | python3 -c "
import sys, json
data = json.load(sys.stdin)
queries = data[0].get('queries', [])
running = sum(q.get('statusCount', {}).get('RUNNING', 0) for q in queries)
print(f\"   Total: {len(queries)}\")
print(f\"   Running: {running}\")
for q in queries:
    status = '● RUNNING' if q.get('statusCount', {}).get('RUNNING', 0) > 0 else '○ STOPPED'
    print(f\"   {status} {q['id']}\")
"
        echo ""
        
        echo "5. Output Topics:"
        echo "   ✓ calls.completed.agg (agentId aggregation)"
        echo "   ✓ calls.fraud-alerts.agg (phone aggregation)"
        echo ""
        
        echo "============================================"
        echo "Quick commands:"
        echo "   $0 agg-calls     — show call aggregation"
        echo "   $0 agg-fraud     — show fraud aggregation"
        echo "   $0 queries       — show query status"
        echo "============================================"
        ;;
    
    # ==================== Default ====================
    *)
        show_help
        ;;
esac
