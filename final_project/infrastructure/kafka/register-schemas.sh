#!/bin/bash
# Register JSON schemas in Schema Registry
# This script registers all required schemas for the Kafka topics
set -e

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8085}"
SCHEMA_DIR="${SCHEMA_DIR:-/tmp/schemas}"

echo "=========================================="
echo "Registering JSON schemas in Schema Registry"
echo "=========================================="
echo "Schema Registry: $SCHEMA_REGISTRY_URL"
echo "Schema directory: $SCHEMA_DIR"

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

# Function to register a schema
register_schema() {
    local topic_name=$1
    local schema_file=$2
    local subject="$topic_name-value"
    
    echo ""
    echo "Registering schema for: $topic_name"
    echo "  Subject: $subject"
    echo "  File: $schema_file"
    
    if [ ! -f "$schema_file" ]; then
        echo "  ERROR: Schema file not found: $schema_file"
        return 1
    fi
    
    # Check if subject already exists
    if curl -sf "$SCHEMA_REGISTRY_URL/subjects/$subject" > /dev/null 2>&1; then
        echo "  Subject '$subject' already exists, checking compatibility..."
        
        # Get existing schema
        local existing_schema
        existing_schema=$(curl -sf "$SCHEMA_REGISTRY_URL/subjects/$subject/versions/latest" | jq -r '.schema')
        
        # Get new schema
        local new_schema
        new_schema=$(cat "$schema_file" | jq -c '.')
        
        if [ "$existing_schema" = "$new_schema" ]; then
            echo "  Schema is identical, skipping registration"
            return 0
        fi
        
        # Try to check compatibility
        local compat
        compat=$(curl -sf -X PUT \
            -H "Content-Type: application/vnd.schemaregistry.v1+json" \
            --data "{\"compatibility\": \"BACKWARD\"}" \
            "$SCHEMA_REGISTRY_URL/subjects/$subject/config" 2>&1 || echo "Could not check compatibility")
        
        echo "  Schema exists but differs, compatibility: $compat"
        return 0
    fi
    
    # Register new schema
    local http_code
    http_code=$(curl -sf -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data "{
            \"subject\": \"$subject\",
            \"version\": 1,
            \"schema\": $(cat "$schema_file" | jq -c '.')
        }" \
        -w "%{http_code}" \
        "$SCHEMA_REGISTRY_URL/subjects" \
        -o /tmp/schema_response.txt 2>&1)
    
    if [ "$http_code" = "200" ]; then
        echo "  Schema registered successfully (HTTP $http_code)"
        return 0
    else
        echo "  ERROR: Failed to register schema (HTTP $http_code)"
        echo "  Response: $(cat /tmp/schema_response.txt)"
        return 1
    fi
}

# Register all schemas
echo ""
echo "=========================================="
echo "Registering transcription-analyzer schemas"
echo "=========================================="

register_schema "transcription.raw" "$SCHEMA_DIR/RawTranscription.json"
register_schema "transcription.summary" "$SCHEMA_DIR/TranscriptionSummary.json"
register_schema "transcription.enriched" "$SCHEMA_DIR/EnrichedTranscription.json"
register_schema "calls.metadata" "$SCHEMA_DIR/CallMetadata.json"

echo ""
echo "=========================================="
echo "Registering call-processor schemas"
echo "=========================================="

register_schema "calls.completed" "$SCHEMA_DIR/CallEvent.json"

echo ""
echo "=========================================="
echo "Registering fraud-detector schemas"
echo "=========================================="

register_schema "calls.fraud-alerts" "$SCHEMA_DIR/FraudAlert.json"

echo ""
echo "=========================================="
echo "Registering transcription-analyzer schemas"
echo "=========================================="

register_schema "customers.profile" "$SCHEMA_DIR/CustomerProfile.json"

echo ""
echo "=========================================="
echo "Schema registration complete!"
echo "=========================================="
echo ""
echo "Registered subjects:"
curl -sf "$SCHEMA_REGISTRY_URL/subjects" | jq -r '.[]' 2>/dev/null || echo "Could not list subjects"
echo ""
echo "To verify, visit:"
echo "  $SCHEMA_REGISTRY_URL/subjects"
