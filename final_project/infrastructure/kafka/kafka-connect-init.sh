#!/bin/bash
# Kafka Connect JDBC Sink Connector Deployment Script
# Deploys the JDBC Sink connector for transcription.enriched → PostgreSQL
set -e

KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://kafka-connect:8083}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"

echo "=========================================="
echo "Deploying Kafka Connect JDBC Sink Connector"
echo "=========================================="
echo "Kafka Connect: $KAFKA_CONNECT_URL"
echo "Schema Registry: $SCHEMA_REGISTRY_URL"

# Wait for Kafka Connect to be ready
echo ""
echo "Waiting for Kafka Connect to be ready..."
for i in $(seq 1 30); do
    if curl -sf "$KAFKA_CONNECT_URL/" > /dev/null 2>&1; then
        echo "Kafka Connect is ready!"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: Kafka Connect not ready after 30 retries"
        exit 1
    fi
    echo "Attempt $i/30 - Kafka Connect not ready, waiting 5s..."
    sleep 5
done

# Wait for Schema Registry to be ready
echo ""
echo "Waiting for Schema Registry to be ready..."
for i in $(seq 1 30); do
    if curl -sf "$SCHEMA_REGISTRY_URL/" > /dev/null 2>&1; then
        echo "Schema Registry is ready!"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: Schema Registry not ready after 30 retries"
        exit 1
    fi
    echo "Attempt $i/30 - Schema Registry not ready, waiting 5s..."
    sleep 5
done

# Check if connector already exists
CONNECTOR_NAME="kafka-connect-jdbc-sink"
echo ""
echo "Checking if connector '$CONNECTOR_NAME' already exists..."
if curl -sf "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null 2>&1; then
    echo "Connector '$CONNECTOR_NAME' already exists. Updating configuration..."
    
    # Get existing config
    EXISTING_CONFIG=$(curl -sf "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/config")
    echo "Existing config retrieved."
else
    EXISTING_CONFIG=""
    echo "Connector '$CONNECTOR_NAME' does not exist. Creating new connector..."
fi

# Deploy/Update connector configuration
echo ""
echo "Deploying JDBC Sink connector..."

CONNECTOR_CONFIG='{
  "name": "'$CONNECTOR_NAME'",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "6",
    "connection.url": "jdbc:postgresql://postgres:5432/call_platform",
    "connection.user": "kafka-connect-user",
    "connection.password": "kafka-connect-secret",
    "topics": "transcription.enriched",
    "table.name.format": "call_transcriptions",
    "pk.mode": "RecordKey",
    "pk.fields": "call_id",
    "auto.create": "false",
    "insert.mode": "upsert",
    "delete.captured.records": "false",
    "transforms": "extract,replace",
    "transforms.extract.type": "org.apache.kafka.connect.transforms.ExtractField$Value",
    "transforms.extract.field": "value",
    "transforms.replace.type": "org.apache.kafka.connect.transforms.ReplaceString$Value",
    "schema.registry.url": "'$SCHEMA_REGISTRY_URL'",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schemas.allow": "false",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "'$SCHEMA_REGISTRY_URL'"
  }
}'

HTTP_CODE=$(curl -sf -X PUT \
    -H "Content-Type: application/json" \
    -d "$CONNECTOR_CONFIG" \
    "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/config" \
    -w "%{http_code}" \
    -o /tmp/connector_response.txt 2>&1)

if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ]; then
    echo "Connector '$CONNECTOR_NAME' deployed successfully (HTTP $HTTP_CODE)"
else
    echo "Failed to deploy connector (HTTP $HTTP_CODE)"
    echo "Response: $(cat /tmp/connector_response.txt)"
    exit 1
fi

# Wait for connector to start
echo ""
echo "Waiting for connector to start..."
for i in $(seq 1 30); do
    CONNECTOR_STATUS=$(curl -sf "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq -r '.connector.status' 2>/dev/null)
    if [ "$CONNECTOR_STATUS" = "RUNNING" ]; then
        echo "Connector is RUNNING!"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "WARNING: Connector did not reach RUNNING state after 30 retries"
        echo "Connector status: $CONNECTOR_STATUS"
        echo "Task statuses:"
        curl -sf "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq '.tasks' 2>/dev/null || echo "Could not get task status"
    else
        echo "Attempt $i/30 - Connector status: $CONNECTOR_STATUS, waiting 5s..."
        sleep 5
    fi
done

# Verify connector
echo ""
echo "=========================================="
echo "Connector Deployment Summary"
echo "=========================================="
echo "Connector: $CONNECTOR_NAME"
echo "Status: $(curl -sf "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq -r '.connector.status' 2>/dev/null || echo 'UNKNOWN')"
echo ""
echo "Connector config:"
curl -sf "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/config" | jq '.' 2>/dev/null || echo "Could not get config"
echo ""
echo "Connector tasks:"
curl -sf "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq '.tasks' 2>/dev/null || echo "Could not get tasks"
echo ""
echo "JDBC Sink connector deployment complete!"
