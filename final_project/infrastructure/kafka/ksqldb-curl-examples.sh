#!/bin/bash
# ksqlDB — готовые curl-команды для копирования в терминал
# Копируй строку и вставляй в терминал!
#
# Все команды используют: curl -X POST http://localhost:8088/ksql
# Header: Content-Type: application/vnd.ksql.v1+json

KSQLDB="http://localhost:8088"
CT="Content-Type: application/vnd.ksql.v1+json"

cat << 'EOF'
╔══════════════════════════════════════════════════════════════╗
║  ksqlDB — CURL COMMANDS (copy & paste)                      ║
║  Note: PRINT/SELECT require websocket. Use SHOW commands    ║
╚══════════════════════════════════════════════════════════════╝

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  INFO & STATUS (work via REST API)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Server info
curl ${KSQLDB}/info

# Health check
curl ${KSQLDB}/health/live
curl ${KSQLDB}/health/ready

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  STREAMS & TABLES (work via REST API)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# List external streams
curl -s -X POST ${KSQLDB}/ksql \
  -H '${CT}' \
  -d '{"ksql": "SHOW STREAMS;"}' | python3 -m json.tool

# List persistent tables
curl -s -X POST ${KSQLDB}/ksql \
  -H '${CT}' \
  -d '{"ksql": "SHOW TABLES;"}' | python3 -m json.tool

# List Kafka topics
curl -s -X POST ${KSQLDB}/ksql \
  -H '${CT}' \
  -d '{"ksql": "SHOW TOPICS;"}' | python3 -m json.tool

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  QUERIES (work via REST API)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Show all queries + status
curl -s -X POST ${KSQLDB}/ksql \
  -H '${CT}' \
  -d '{"ksql": "SHOW QUERIES;"}' | python3 -m json.tool

# Extended query info (topology, execution plan, offsets)
curl -s -X POST ${KSQLDB}/ksql \
  -H '${CT}' \
  -d '{"ksql": "SHOW QUERIES EXTENDED;"}' | python3 -m json.tool

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  AGGREGATION STATUS (work via REST API)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Check calls_completed_agg processing (shows offsets)
curl -s -X POST ${KSQLDB}/ksql \
  -H '${CT}' \
  -d '{"ksql": "SHOW QUERIES EXTENDED;"}' | python3 -c "
import sys, json
data = json.load(sys.stdin)
for q in data[0].get('queryDescriptions', []):
    if 'CALLS_COMPLETED' in q.get('id', ''):
        print(f\"Query: {q['id']}\")
        for t in q.get('tasksMetadata', [])[:2]:
            for o in t.get('topicOffsets', []):
                tp = o.get('topicPartitionEntity', {})
                if 'calls.completed' in tp.get('topic', ''):
                    print(f\"  Partition {tp['partition']}: {o['committedOffset']}/{o['endOffset']}\")
"

# Check fraud_alerts_filtered processing (shows offsets)
curl -s -X POST ${KSQLDB}/ksql \
  -H '${CT}' \
  -d '{"ksql": "SHOW QUERIES EXTENDED;"}' | python3 -c "
import sys, json
data = json.load(sys.stdin)
for q in data[0].get('queryDescriptions', []):
    if 'FRAUD_ALERTS' in q.get('id', ''):
        print(f\"Query: {q['id']}\")
        for t in q.get('tasksMetadata', [])[:2]:
            for o in t.get('topicOffsets', []):
                tp = o.get('topicPartitionEntity', {})
                if 'calls.fraud-alerts' in tp.get('topic', ''):
                    print(f\"  Partition {tp['partition']}: {o['committedOffset']}/{o['endOffset']}\")
"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ONE-LINERS (quick copy)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Quick status (no formatting)
curl -s ${KSQLDB}/info

# Streams count
curl -s -X POST ${KSQLDB}/ksql -H '${CT}' -d '{"ksql": "SHOW STREAMS;"}' | grep -o '"name":"[^"]*"'

# Tables count
curl -s -X POST ${KSQLDB}/ksql -H '${CT}' -d '{"ksql": "SHOW TABLES;"}' | grep -o '"name":"[^"]*"'

# Running queries count
curl -s -X POST ${KSQLDB}/ksql -H '${CT}' -d '{"ksql": "SHOW QUERIES;"}' | grep -o '"RUNNING":[0-9]*'

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  NOTE: PRINT/SELECT queries
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# PRINT and SELECT queries require WebSocket endpoint:
#   ws://localhost:8088/query
#
# Example using wscat (npm install -g wscat):
#   wscat -c ws://localhost:8088/query
#   {"ksql": "SELECT * FROM CALLS_COMPLETED_AGG;"}
#
# Or use the convenience script for full verification:
#   ./infrastructure/kafka/ksqldb-checks.sh all
#   ./infrastructure/kafka/ksqldb-checks.sh agg-calls
#   ./infrastructure/kafka/ksqldb-checks.sh agg-fraud

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  USEFUL TIPS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Use alias for quick access
alias ksql="curl -s -X POST http://localhost:8088/ksql -H 'Content-Type: application/vnd.ksql.v1+json'"

# Then use:
# ksql -d '{"ksql": "SHOW STREAMS;"}' | python3 -m json.tool
# ksql -d '{"ksql": "SHOW TABLES;"}' | python3 -m json.tool
# ksql -d '{"ksql": "SHOW QUERIES;"}' | python3 -m json.tool

# Or use the convenience script:
# ./infrastructure/kafka/ksqldb-checks.sh all
# ./infrastructure/kafka/ksqldb-checks.sh agg-calls
# ./infrastructure/kafka/ksqldb-checks.sh agg-fraud

EOF
