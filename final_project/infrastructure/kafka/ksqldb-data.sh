#!/bin/bash
# ksqldb-data.sh — Быстрое получение данных из ksqlDB output topics
# Usage: ./ksqldb-data.sh [calls|fraud|all]

set -e

KAFKA_BROKER="kafka-1:9092"
KAFKA_TOPIC_CMD="/usr/bin/kafka-console-consumer"

show_calls_agg() {
    echo "============================================"
    echo "CALLS_COMPLETED_AGG (Топ агентов по вызовам)"
    echo "============================================"
    echo ""
    
    docker exec kafka-1 $KAFKA_TOPIC_CMD \
        --bootstrap-server $KAFKA_BROKER \
        --topic calls.completed.agg \
        --from-beginning \
        --max-messages 20 \
        --property print.key=true \
        --property key.separator="|" \
        --timeout-ms 10000 2>&1 | \
    python3 -c "
import sys, json

records = []
for line in sys.stdin:
    line = line.strip()
    if '|' in line:
        key, value = line.split('|', 1)
        try:
            data = json.loads(value)
            records.append({'key': key, 'value': data})
        except:
            pass

records.sort(key=lambda x: x['value'].get('CALL_COUNT', 0), reverse=True)

print(f'{\"Agent\":<20} {\"Calls\":>10} {\"Avg NPS\":>10} {\"Last Call\":>20}')
print('-' * 65)
for i, r in enumerate(records[:20], 1):
    v = r['value']
    print(f'{r[\"key\"]:<20} {v[\"CALL_COUNT\"]:>10} {v[\"AVG_NPS_SCORE\"]:>10.2f} {v.get(\"LAST_CALL_AT\", 0):>20}')
"
}

show_fraud_agg() {
    echo "============================================"
    echo "FRAUD_ALERTS_FILTERED (Топ номеров с alerts)"
    echo "============================================"
    echo ""
    
    docker exec kafka-1 $KAFKA_TOPIC_CMD \
        --bootstrap-server $KAFKA_BROKER \
        --topic calls.fraud-alerts.agg \
        --from-beginning \
        --max-messages 20 \
        --property print.key=true \
        --property key.separator="|" \
        --timeout-ms 10000 2>&1 | \
    python3 -c "
import sys, json

records = []
for line in sys.stdin:
    line = line.strip()
    if '|' in line:
        key, value = line.split('|', 1)
        try:
            data = json.loads(value)
            records.append({'key': key, 'value': data})
        except:
            pass

records.sort(key=lambda x: x['value'].get('ALERT_COUNT', 0), reverse=True)

print(f'{\"Phone\":<25} {\"Alerts\":>10}')
print('-' * 38)
for i, r in enumerate(records[:20], 1):
    print(f'{r[\"key\"]:<25} {r[\"value\"][\"ALERT_COUNT\"]:>10}')
"
}

case "${1:-all}" in
    calls)
        show_calls_agg
        ;;
    fraud)
        show_fraud_agg
        ;;
    all)
        show_calls_agg
        echo ""
        show_fraud_agg
        ;;
    *)
        echo "Usage: $0 [calls|fraud|all]"
        echo ""
        echo "Options:"
        echo "  calls  - Show calls.completed.agg data"
        echo "  fraud  - Show calls.fraud-alerts.agg data"
        echo "  all    - Show both (default)"
        exit 1
        ;;
esac
