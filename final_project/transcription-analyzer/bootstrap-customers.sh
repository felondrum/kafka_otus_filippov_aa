#!/bin/bash
# Bootstrap customers.profile topic with sample customer data
# Usage: ./bootstrap-customers.sh [csv_file] [kafka-broker]

set -e

CSV_FILE="${1:-/app/data/customers.csv}"
KAFKA_BROKER="${2:-kafka:9092}"
TOPIC="customers.profile"

echo "Bootstrapping customers.profile topic..."
echo "CSV file: $CSV_FILE"
echo "Kafka broker: $KAFKA_BROKER"
echo "Topic: $TOPIC"

# Check if CSV file exists
if [ ! -f "$CSV_FILE" ]; then
    echo "CSV file not found: $CSV_FILE"
    echo "Creating sample CSV file..."
    mkdir -p /app/data
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
    echo "Sample CSV file created: $CSV_FILE"
fi

# Read CSV and produce to Kafka (skip header)
tail -n +2 "$CSV_FILE" | while IFS=',' read -r phone segment riskLevel; do
    # Format as JSON for kafka-console-producer
    echo "${phone}:{\"phone\":\"${phone}\",\"segment\":\"${segment}\",\"riskLevel\":\"${riskLevel}\"}"
done | kafka-console-producer.sh \
    --broker-list "$KAFKA_BROKER" \
    --topic "$TOPIC" \
    --property "parse.key=true" \
    --property "key.separator=:" \
    --property "key.formatter=org.apache.kafka.common.serialization.StringSerializer" \
    --property "value.formatter=org.apache.kafka.common.serialization.StringSerializer" 2>/dev/null || {
    echo "Warning: Could not connect to Kafka at $KAFKA_BROKER"
    echo "This is expected if Kafka is not running."
    echo "To bootstrap manually:"
    echo "  cat $CSV_FILE | tail -n +2 | while IFS=',' read -r phone segment riskLevel; do"
    echo "    echo \"\${phone}:{\\\"phone\\\":\\\"\\\${phone}\\\",\\\"segment\\\":\\\"\\\${segment}\\\",\\\"riskLevel\\\":\\\"\\\${riskLevel}\\\"}\""
    echo "  done | kafka-console-producer --broker-list $KAFKA_BROKER --topic $TOPIC --property parse.key=true"
}

echo "Bootstrap complete!"
echo "To verify, check the $TOPIC topic in Kafdrop or with:"
echo "  kafka-console-consumer.sh --bootstrap-server $KAFKA_BROKER --topic $TOPIC --from-beginning --timeout-ms 5000"
