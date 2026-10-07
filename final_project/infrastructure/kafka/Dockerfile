FROM confluentinc/cp-kafka:7.6.1

# Install JMX exporter for Prometheus metrics
RUN apt-get update && apt-get install -y wget curl && \
    mkdir -p /opt/jmx_exporter && \
    wget -qO /opt/jmx_exporter/jmx_prometheus_httpserver.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_httpserver/0.20.0/jmx_prometheus_httpserver-0.20.0.jar && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy SASL configuration
COPY sasl.properties /etc/kafka/sasl.properties
COPY server.properties /etc/kafka/server.properties
COPY kafka-start.sh /etc/kafka/kafka-start.sh
RUN chmod +x /etc/kafka/kafka-start.sh

# Configure JMX exporter
COPY jmx-exporter-config.yml /opt/jmx_exporter/config.yml

# Expose JMX exporter port
EXPOSE 5556

ENTRYPOINT ["/etc/kafka/kafka-start.sh"]
