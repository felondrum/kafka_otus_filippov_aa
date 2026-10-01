#!/bin/bash
# Kafka Connect JDBC Sink Connector Deployment Script
# Deploys the JDBC Sink connector for transcription.enriched → PostgreSQL
set -e

KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://kafka-connect:8083}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8085}"
CONNECTOR_NAME="kafka-connect-jdbc-sink"

echo "=========================================="
echo "Deploying Kafka Connect JDBC Sink Connector"
echo "=========================================="
echo "Kafka Connect: $KAFKA_CONNECT_URL"
echo "Connector: $CONNECTOR_NAME"
echo "Schema Registry: $SCHEMA_REGISTRY_URL"

# Wait for Kafka Connect to be ready
echo ""
echo "Waiting for Kafka Connect to be ready..."
for i in $(seq 1 60); do
    if curl -sf "$KAFKA_CONNECT_URL/" > /dev/null 2>&1; then
        echo "Kafka Connect is ready!"
        break
    fi
    if [ "$i" -eq "60" ]; then
        echo "ERROR: Kafka Connect not ready after 60 retries"
        exit 1
    fi
    echo "Attempt $i/60 - waiting 5s..."
    sleep 5
done

# Wait for Schema Registry to be ready
echo ""
echo "Waiting for Schema Registry to be ready..."
for i in $(seq 1 60); do
    if curl -sf "$SCHEMA_REGISTRY_URL/" > /dev/null 2>&1; then
        echo "Schema Registry is ready!"
        break
    fi
    if [ "$i" -eq "60" ]; then
        echo "ERROR: Schema Registry not ready after 60 retries"
        exit 1
    fi
    echo "Attempt $i/60 - waiting 5s..."
    sleep 5
done

# Check if connector already exists
echo ""
echo "Checking if connector '$CONNECTOR_NAME' already exists..."
if curl -sf "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null 2>&1; then
    echo "Connector exists. Updating configuration..."
    HTTP_METHOD="PUT"
else
    echo "Connector does not exist. Creating new connector..."
    HTTP_METHOD="POST"
fi

# Deploy/Update connector configuration
echo ""
echo "Deploying JDBC Sink connector via $HTTP_METHOD..."

CONNECTOR_CONFIG='{
  "name": "kafka-connect-jdbc-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "3",
    "connection.url": "jdbc:postgresql://postgres:5432/call_platform?sslmode=disable",
    "connection.user": "kafka-connect-user",
    "connection.password": "kafka-connect-secret",
    "topics": "transcription.enriched",
    "table.name.format": "call_transcriptions",
    "pk.mode": "none",
    "auto.create": "false",
    "auto.evolve": "false",
    "insert.mode": "insert",
    "batch.size": 100,
    "flush.max.records": 500,
    "retry.backoff.ms": 1000,
    "max.retries": 3,
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "transcription.enriched.dlq",
    "errors.deadletterqueue.topic.replication.factor": 3,
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true"
  }
}'

RESPONSE=$(curl -sf -X "$HTTP_METHOD" \
    -H "Content-Type: application/json" \
    -d "$CONNECTOR_CONFIG" \
    "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME" \
    -w "\n%{http_code}" 2>&1)

HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)

echo "HTTP Status: $HTTP_CODE"

if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ]; then
    echo "Connector deployed successfully!"
else
    echo "Failed to deploy connector (HTTP $HTTP_CODE)"
    echo "Response: $BODY"
    exit 1
fi

# Wait for connector to start
echo ""
echo "Waiting for connector to become RUNNING..."
for i in $(seq 1 60); do
    STATUS=$(curl -sf "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq -r '.connector.status' 2>/dev/null)
    if [ "$STATUS" = "RUNNING" ]; then
        echo "Connector is RUNNING!"
        break
    fi
    if [ "$i" -eq "60" ]; then
        echo "WARNING: Connector did not reach RUNNING state"
        curl -sf "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq '.' 2>/dev/null
    else
        echo "Attempt $i/60 - Status: $STATUS, waiting 5s..."
        sleep 5
    fi
done

# Final verification
echo ""
echo "=========================================="
echo "Deployment Summary"
echo "=========================================="
echo "Connector: $CONNECTOR_NAME"
curl -sf "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq '.' 2>/dev/null || echo "Could not get status"
echo ""
echo "Done!"
