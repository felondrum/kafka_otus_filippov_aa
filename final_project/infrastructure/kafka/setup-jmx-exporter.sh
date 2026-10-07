#!/bin/bash
# Download JMX Prometheus exporter agent for Kafka brokers
# This enables Prometheus to scrape Kafka JMX metrics

JMX_EXPORTER_VERSION="0.20.0"
JMX_EXPORTER_JAR="/opt/jmx_exporter/jmx_prometheus_javaagent.jar"

echo "Downloading JMX Prometheus exporter v${JMX_EXPORTER_VERSION}..."
curl -L -o "${JMX_EXPORTER_JAR}" "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar"

if [ $? -eq 0 ]; then
    echo "✓ Successfully downloaded JMX exporter to ${JMX_EXPORTER_JAR}"
    ls -lh "${JMX_EXPORTER_JAR}"
else
    echo "✗ Failed to download JMX exporter"
    exit 1
fi
