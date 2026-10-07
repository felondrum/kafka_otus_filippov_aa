#!/bin/bash
# Kafka startup script with SASL/PLAIN configuration
# This script configures SASL/PLAIN before starting Kafka

set -e

# Create SASL users file
cat > /etc/kafka/secrets/kafka_users.properties << EOF
# SASL/PLAIN users for Kafka
# Format: username=plaintext_password
admin=${KAFKA_CLIENTS_PASSWORD:-admin-secret}
controls=${KAFKA_CONTROLS_PASSWORD:-controls-secret}
connect=${KAFKA_CONNECTS_PASSWORD:-connect-secret}
EOF

# Create JAAS configuration for broker
export KAFKA_OPTS="-Djava.security.auth.login.config=/etc/kafka/jaas.conf"

# Create JAAS config file
cat > /etc/kafka/jaas.conf << EOF
KafkaServer {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    user_admin="${KAFKA_CLIENTS_PASSWORD:-admin-secret}"
    user_controls="${KAFKA_CONTROLS_PASSWORD:-controls-secret}"
    user_connect="${KAFKA_CONNECTS_PASSWORD:-connect-secret}";
};

KafkaClient {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="admin"
    password="${KAFKA_CLIENTS_PASSWORD:-admin-secret}";
};
EOF

# Add JMX exporter agent if enabled
if [ "${JMX_EXPORTER_ENABLED:-false}" = "true" ]; then
    JMX_EXPORTER_PORT=${JMX_EXPORTER_PORT:-5556}
    JAVA_OPTS="$JAVA_OPTS -javaagent:/opt/jmx_exporter/jmx_prometheus_httpserver.jar=${JMX_EXPORTER_PORT}:/opt/jmx_exporter/config.yml"
    export JAVA_OPTS
fi

# Start Kafka with default entrypoint
exec /etc/confluent/docker/run
