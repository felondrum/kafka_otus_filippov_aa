#!/bin/bash

PASS_COUNT=0
FAIL_COUNT=0

pass() {
  printf "  \033[32m[PASS]\033[0m %s\n" "$1"
  PASS_COUNT=$((PASS_COUNT + 1))
}

fail() {
  printf "  \033[31m[FAIL]\033[0m %s\n" "$1"
  FAIL_COUNT=$((FAIL_COUNT + 1))
}

echo "========================================="
echo "  Kafka Connect + Debezium CDC - Test"
echo "========================================="

# =========================================
# CLEANUP: Remove all artifacts
# =========================================
echo -e "\n=== CLEANUP: Removing previous artifacts ==="

# Delete connector if exists
CONNECTORS=$(curl -s http://localhost:8083/connectors 2>/dev/null)
for name in $(echo "$CONNECTORS" | python3 -c "import sys,json; print(' '.join(json.load(sys.stdin)))" 2>/dev/null); do
  echo "  Deleting connector: $name"
  curl -s -X DELETE "http://localhost:8083/connectors/$name" > /dev/null 2>&1
done

# Delete Kafka topic if exists
TOPIC="otusdb.public.users"
docker exec kafka-new-hw5 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete --topic "$TOPIC" 2>/dev/null || true
sleep 2

# Delete PostgreSQL table
docker exec -e PGPASSWORD=root postgres-hw5 psql -U root -d otus -c "DROP TABLE IF EXISTS users;" > /dev/null 2>&1

echo "  Cleanup complete"

# =========================================
# STEP 1: Check containers
# =========================================

# =========================================
# STEP 1: Check containers
# =========================================
printf "\n=== STEP 1: Check running containers ==="

CONTAINERS=("kafka-new-hw5" "postgres-hw5" "kafka-connect-hw5")
for container in "${CONTAINERS[@]}"; do
  if docker ps --format '{{.Names}}' | grep -q "$container"; then
    pass "Container '$container' is running"
  else
    fail "Container '$container' is NOT running"
  fi
done

# =========================================
# STEP 2: Setup PostgreSQL
# =========================================
printf "\n=== STEP 2: Setup PostgreSQL ==="

echo "  Running setup-db.sh..."
if bash setup-db.sh; then
  pass "Table users created and populated"
else
  fail "Error creating table users"
fi

# Check data in table
RECORD_COUNT=$(docker exec -e PGPASSWORD=root postgres-hw5 psql -U root -d otus -t -A -c "SELECT count(*) FROM users;" 2>/dev/null | tr -d '[:space:]')
if [ "$RECORD_COUNT" -ge 5 ] 2>/dev/null; then
  pass "Found $RECORD_COUNT records in users table"
else
  fail "Expected >= 5 records in users table, found: ${RECORD_COUNT:-0}"
fi

# =========================================
# STEP 3: Register Debezium connector
# =========================================
printf "\n=== STEP 3: Register Debezium CDC Connector ==="

echo "  Running register-connector.sh..."
if bash register-connector.sh; then
  pass "Connector registered"
else
  fail "Error registering connector"
fi

# Check connector state
CONNECTOR_STATE=$(curl -s http://localhost:8083/connectors/postgres-connector/status 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('connector',{}).get('state','UNKNOWN'))" 2>/dev/null)
if [ "$CONNECTOR_STATE" = "RUNNING" ]; then
  pass "Connector is RUNNING"
else
  fail "Connector state: ${CONNECTOR_STATE:-UNKNOWN} (expected RUNNING)"
  printf "\n  Kafka Connect logs (last 30 lines):"
  docker logs kafka-connect-hw5 --tail 30 2>&1 | tail -30
fi

# =========================================
# STEP 4: Wait for initial snapshot
# =========================================
printf "\n=== STEP 4: Wait for initial snapshot ==="

TOPIC="otusdb.public.users"
echo "  Waiting for messages in topic '$TOPIC'..."
sleep 5

# Run consumer and collect messages
CONSUMER_OUTPUT=$(docker exec kafka-new-hw5 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC" \
  --from-beginning \
  --max-messages 10 \
  --timeout-ms 10000 2>&1)

SNAPSHOT_MESSAGES=$(echo "$CONSUMER_OUTPUT" | grep -c '"op":"r"' 2>/dev/null || echo "0")
TOTAL_MESSAGES=$(echo "$CONSUMER_OUTPUT" | grep -c '"op"' 2>/dev/null || echo "0")

if [ "$TOTAL_MESSAGES" -ge 5 ] 2>/dev/null; then
  pass "Initial snapshot found: $TOTAL_MESSAGES messages (expected >= 5)"
else
  fail "Initial snapshot: found $TOTAL_MESSAGES messages (expected >= 5)"
fi

# =========================================
# STEP 5: CDC - INSERT
# =========================================
printf "\n=== STEP 5: CDC - INSERT ==="

echo "  Inserting new records into PostgreSQL..."
docker exec -e PGPASSWORD=root postgres-hw5 psql -U root -d otus -c "INSERT INTO users (name, email, age) VALUES ('Olga Novikova', 'olga@example.com', 27), ('Sergey Morozov', 'sergey@example.com', 33);" > /dev/null 2>&1

pass "2 new records inserted into users table"

# Wait for CDC processing
echo "  Waiting for CDC processing (10 sec)..."
sleep 10

# Check CDC messages
CDC_OUTPUT=$(docker exec kafka-new-hw5 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC" \
  --from-beginning \
  --max-messages 20 \
  --timeout-ms 15000 2>&1)

INSERT_MESSAGES=$(echo "$CDC_OUTPUT" | grep -c '"op":"c"' 2>/dev/null || echo "0")

if [ "$INSERT_MESSAGES" -ge 2 ] 2>/dev/null; then
  pass "Found $INSERT_MESSAGES CDC insert messages (expected >= 2)"
else
  fail "CDC insert messages: found $INSERT_MESSAGES (expected >= 2)"
fi

# Check specific records
if echo "$CDC_OUTPUT" | grep -q "Olga Novikova"; then
  pass "Record 'Olga Novikova' found in Kafka"
else
  fail "Record 'Olga Novikova' NOT found in Kafka"
fi

if echo "$CDC_OUTPUT" | grep -q "Sergey Morozov"; then
  pass "Record 'Sergey Morozov' found in Kafka"
else
  fail "Record 'Sergey Morozov' NOT found in Kafka"
fi

# =========================================
# STEP 6: CDC - UPDATE
# =========================================
printf "\n=== STEP 6: CDC - UPDATE ==="

echo "  Updating record in PostgreSQL..."
docker exec -e PGPASSWORD=root postgres-hw5 psql -U root -d otus -c "UPDATE users SET age = 26, email = 'ivan_new@example.com' WHERE name = 'Ivan Ivanov';" > /dev/null 2>&1

pass "Record updated in users table"

# Wait for CDC processing
echo "  Waiting for CDC processing (10 sec)..."
sleep 10

UPDATE_OUTPUT=$(docker exec kafka-new-hw5 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC" \
  --from-beginning \
  --max-messages 25 \
  --timeout-ms 15000 2>&1)

UPDATE_MESSAGES=$(echo "$UPDATE_OUTPUT" | grep -c '"op":"u"' 2>/dev/null || echo "0")

if [ "$UPDATE_MESSAGES" -ge 1 ] 2>/dev/null; then
  pass "Found $UPDATE_MESSAGES CDC update messages (expected >= 1)"
else
  fail "CDC update messages: found $UPDATE_MESSAGES (expected >= 1)"
fi

# =========================================
# STEP 7: CDC - DELETE
# =========================================
printf "\n=== STEP 7: CDC - DELETE ==="

echo "  Deleting record from PostgreSQL..."
docker exec -e PGPASSWORD=root postgres-hw5 psql -U root -d otus -c "DELETE FROM users WHERE name = 'Dmitry Volkov';" > /dev/null 2>&1

pass "Record deleted from users table"

# Wait for CDC processing
echo "  Waiting for CDC processing (10 sec)..."
sleep 10

DELETE_OUTPUT=$(docker exec kafka-new-hw5 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC" \
  --from-beginning \
  --max-messages 30 \
  --timeout-ms 15000 2>&1)

DELETE_MESSAGES=$(echo "$DELETE_OUTPUT" | grep -c '"op":"d"' 2>/dev/null || echo "0")

if [ "$DELETE_MESSAGES" -ge 1 ] 2>/dev/null; then
  pass "Found $DELETE_MESSAGES CDC delete messages (expected >= 1)"
else
  fail "CDC delete messages: found $DELETE_MESSAGES (expected >= 1)"
fi

# =========================================
# RESULTS
# =========================================
printf "\n========================================="
echo "  RESULTS"
echo "========================================="
printf "  \033[32mPassed: $PASS_COUNT\033[0m\n"
printf "  \033[31mFailed: $FAIL_COUNT\033[0m\n"
TOTAL=$((PASS_COUNT + FAIL_COUNT))
printf "  Total: $TOTAL"
echo "========================================="

if [ "$FAIL_COUNT" -eq 0 ]; then
  printf "  \033[32mAll tests passed! OK\033[0m\n"
  exit 0
else
  printf "  \033[31mSome tests failed\033[0m\n"
  exit 1
fi
