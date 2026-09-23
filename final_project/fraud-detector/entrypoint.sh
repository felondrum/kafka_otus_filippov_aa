#!/bin/sh
set -e
mkdir -p /tmp/fraud-detector-state
exec java -jar app.jar
