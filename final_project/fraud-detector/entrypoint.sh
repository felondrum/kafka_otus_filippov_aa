#!/bin/sh
set -e
# Use /tmp as base for state directory, it is world-writable
export STATE_DIR="/tmp/fraud-detector-state-${HOSTNAME:-default}"
# Kafka Streams will create this directory if it doesn't exist.
# If it fails, it means we don't have permission to write in /tmp.
# But /tmp should be writable.
if ! java -DAPP_ID="fraud-detector-${HOSTNAME:-default}" -Dstate.dir="$STATE_DIR" -jar app.jar; then
  echo "Application failed. Sleeping for investigation..."
  sleep infinity
fi
