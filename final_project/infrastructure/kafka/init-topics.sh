#!/bin/bash
# Topic initialization script for Kafka
# Creates all required topics with correct configuration
# Usage: ./init-topics.sh [BOOTSTRAP_SERVERS]
#
# NOTE: This script MUST run inside a Kafka broker container
#       (has kafka-topics, kafka-broker-api-versions binaries)
#       From host: docker exec kafka-1 bash /tmp/init-topics.sh
#       Or via Makefile: make init-topics (which runs docker exec)

set -e

BOOTSTRAP_SERVERS="${1:-kafka-1:9092,kafka-2:9092,kafka-3:9092}"
RETRIES="${2:-30}"
RETRY_INTERVAL="${3:-5}"

# Use Confluent image binary paths (no .sh extension)
KAFKA_TOPICS="/usr/bin/kafka-topics"

echo "Initializing Kafka topics..."
echo "Bootstrap servers: $BOOTSTRAP_SERVERS"
echo "Retries: $RETRIES"

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
for i in $(seq 1 $RETRIES); do
    if $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVERS" --list > /dev/null 2>&1; then
        echo "Kafka is ready!"
        break
    fi
    if [ "$i" -eq "$RETRIES" ]; then
        echo "ERROR: Kafka not ready after $RETRIES retries"
        exit 1
    fi
    echo "Attempt $i/$RETRIES - Kafka not ready, waiting ${RETRY_INTERVAL}s..."
    sleep $RETRY_INTERVAL
done

# Function to create a topic
create_topic() {
    local topic=$1
    local partitions=$2
    local replication=$3
    local config_args=$4
    
    echo "Creating topic: $topic (partitions=$partitions, replication=$replication)"
    
    if $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --create \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor "$replication" \
        --config $config_args 2>&1; then
        echo "  Topic '$topic' created successfully"
    else
        # Topic might already exist, that's OK
        echo "  Topic '$topic' already exists (skipping)"
    fi
}

echo ""
echo "=== Creating Stream Topics (Avro format) ==="
create_topic "calls.completed" 6 3
create_topic "calls.fraud-alerts" 6 3
create_topic "transcription.raw" 6 3
create_topic "transcription.summary" 6 3
create_topic "transcription.enriched" 6 3

echo ""
echo "=== Creating Stream Topic (JSON format) ==="
create_topic "calls.dlq" 6 3

echo ""
echo "=== Creating Compacted Topics (Avro format) ==="
create_topic "calls.metadata" 6 3 "cleanup.policy=compact"
create_topic "customers.profile" 6 3 "cleanup.policy=compact"

echo ""
echo "=== Creating ksqlDB Output Topics (Avro format) ==="
create_topic "calls.completed.agg" 6 3
create_topic "calls.fraud-alerts.agg" 6 3

echo ""
echo "=== Creating DLQ Topic (JSON format) ==="
create_topic "transcription.enriched.dlq" 3 3

echo ""
echo "=== Verifying Topics ==="
$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVERS" --list

echo ""
echo "Topic initialization complete!"
