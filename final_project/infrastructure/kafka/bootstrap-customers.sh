#!/bin/bash
# Bootstrap customers.profile topic with sample customer data
# This script runs inside a Kafka broker container to use kafka-console-producer
set -e

CSV_FILE="/tmp/customers.csv"
KAFKA_BROKER="${KAFKA_BOOTSTRAP_SERVERS:-kafka-1:9092,kafka-2:9092,kafka-3:9092}"
TOPIC="customers.profile"
KAFKA_CONSOLE_PRODUCER="/usr/bin/kafka-console-producer"

echo "=========================================="
echo "Bootstrapping customers.profile topic"
echo "=========================================="
echo "Kafka brokers: $KAFKA_BROKER"
echo "Topic: $TOPIC"

# Wait for Kafka to be ready
echo ""
echo "Waiting for Kafka to be ready..."
for i in $(seq 1 30); do
    if $KAFKA_CONSOLE_PRODUCER \
        --broker-list "$KAFKA_BROKER" \
        --topic "$TOPIC" \
        --property "parse.key=true" \
        --dry-run 2>/dev/null; then
        echo "Kafka is ready!"
        break
    fi
    if [ "$i" -eq "30" ]; then
        echo "ERROR: Kafka not ready after 30 retries"
        exit 1
    fi
    echo "Attempt $i/30 - Kafka not ready, waiting 5s..."
    sleep 5
done

# Create CSV file with sample customer data
echo ""
echo "Creating sample customer data..."
cat > "$CSV_FILE" << 'CSVEOF'
phone,segment,riskLevel
+71234567890,PREMIUM,HIGH
+71234567891,PREMIUM,MEDIUM
+71234567892,PREMIUM,LOW
+71234567893,STANDARD,HIGH
+71234567894,STANDARD,MEDIUM
+71234567895,STANDARD,LOW
+71234567896,CORPORATE,HIGH
+71234567897,CORPORATE,MEDIUM
+71234567898,CORPORATE,LOW
+71234567899,STANDARD,MEDIUM
CSVEOF

# Produce to Kafka (skip header, format as key:value)
echo ""
echo "Producing customer data to Kafka..."
tail -n +2 "$CSV_FILE" | while IFS=',' read -r phone segment riskLevel; do
    echo "${phone}:{\"phone\":\"${phone}\",\"segment\":\"${segment}\",\"riskLevel\":\"${riskLevel}\"}"
done | $KAFKA_CONSOLE_PRODUCER \
    --broker-list "$KAFKA_BROKER" \
    --topic "$TOPIC" \
    --property "parse.key=true" \
    --property "key.separator=:" 2>&1

echo ""
echo "Bootstrap complete!"
echo "Customer data has been loaded into $TOPIC topic"
echo ""
echo "To verify, check the topic in Kafdrop or with:"
echo "  kafka-console-consumer.sh --bootstrap-server $KAFKA_BROKER --topic $TOPIC --from-beginning --timeout-ms 5000"
