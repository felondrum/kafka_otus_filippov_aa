#!/bin/bash
set -e

CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="postgres-connector"

echo "========================================="
echo "Регистрация Debezium PostgreSQL CDC Connector"
echo "========================================="

# Проверка доступности Kafka Connect
printf "\n[1] Проверка Kafka Connect API..."
for i in {1..30}; do
  if curl -s "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
    echo "    Kafka Connect доступен (попытка $i)"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "    ERROR: Kafka Connect недоступен после 30 попыток"
    exit 1
  fi
  echo -n "."
  sleep 1
done

# Проверка, не зарегистрирован ли уже connector
printf "\n[2] Проверка существующих connector'ов..."
EXISTING=$(curl -s "${CONNECT_URL}/connectors" 2>/dev/null)
echo "    Ответ API: ${EXISTING}"
if echo "$EXISTING" | grep -q "$CONNECTOR_NAME"; then
  echo "    Connector '${CONNECTOR_NAME}' уже зарегистрирован"
  echo "    Перезапуск..."
  curl -s -X PUT "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/restart?force=true" > /dev/null
  echo "    Connector перезапущен"
else
  # Регистрация connector
  printf "\n[3] Регистрация connector'а..."
  echo "    Конфиг: $(cat connect-config.json)"
  CONFIG_FILE="$(cd "$(dirname "$0")" && pwd)/connect-config.json"
  echo "    Файл конфига: $CONFIG_FILE"
  HTTP_CODE=$(curl -s -o /tmp/curl_response.txt -w "%{http_code}" -X POST "${CONNECT_URL}/connectors" \
    -H "Content-Type: application/json" \
    -d @"$CONFIG_FILE")
  BODY=$(cat /tmp/curl_response.txt)
  echo "    HTTP код: $HTTP_CODE"
  echo "    Ответ: $BODY"
  if [ "$HTTP_CODE" != "200" ] && [ "$HTTP_CODE" != "201" ]; then
    echo "    ERROR: Не удалось зарегистрировать connector (HTTP $HTTP_CODE)"
    echo "    Логи Kafka Connect:"
    docker logs kafka-connect-hw5 --tail 50 2>&1
    exit 1
  fi
  echo "    Connector '${CONNECTOR_NAME}' зарегистрирован"
fi

# Ожидание готовности connector
printf "\n[4] Ожидание готовности connector'а..."
for i in {1..60}; do
  STATUS=$(curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null)
  STATE=$(echo "$STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin).get('connector_status',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")
  echo "    Попытка $i: state=$STATE"
  if [ "$STATE" = "RUNNING" ]; then
    echo "    Connector в состоянии RUNNING"
    break
  fi
  if [ "$STATE" = "FAILED" ]; then
    echo "    ERROR: Connector перешел в состояние FAILED"
    echo "    Логи connector'а:"
    curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/log" 2>/dev/null
    echo ""
    echo "    Логи Kafka Connect:"
    docker logs kafka-connect-hw5 --tail 50 2>&1
    exit 1
  fi
  if [ "$i" -eq 60 ]; then
    echo "    WARNING: Connector не запустился за 60 секунд, текущее состояние: ${STATE}"
  fi
  echo -n "."
  sleep 1
done

# Вывод статуса
printf "\n[5] Статус connector'а:"
curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | python3 -m json.tool 2>/dev/null || \
curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status"

printf "\n========================================="
echo "Connector '${CONNECTOR_NAME}' готов к работе"
echo "========================================="
