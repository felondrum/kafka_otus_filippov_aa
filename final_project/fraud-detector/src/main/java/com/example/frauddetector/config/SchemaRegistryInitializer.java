package com.example.frauddetector.config;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import jakarta.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Registers FraudAlert Avro schema in Schema Registry on startup.
 * Idempotent: skips registration if schema already exists.
 */
@Configuration
public class SchemaRegistryInitializer {

    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryInitializer.class);

    private static final String SCHEMA_SUBJECT = "calls.fraud-alerts-value";
    private static final AtomicBoolean registered = new AtomicBoolean(false);

    @Value("${SCHEMA_REGISTRY_URL:http://schema-registry:8081}")
    private String schemaRegistryUrl;

    @PostConstruct
    public void registerSchema() {
        if (registered.compareAndSet(false, true)) {
            try {
                String schemaString = loadSchemaFromResources();
                SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl, 1);

                // Check if schema already exists
                try {
                    int subjectId = schemaRegistry.getSubjectSchemaMetadata(SCHEMA_SUBJECT).getId();
                    log.info("FraudAlert schema already registered in Schema Registry (subject: {}, id: {})", SCHEMA_SUBJECT, subjectId);
                    return;
                } catch (Exception e) {
                    // Schema not found, register it
                    log.info("FraudAlert schema not found in Schema Registry, registering...");
                }

                // Register the schema
                int schemaId = schemaRegistry.register(SCHEMA_SUBJECT, schemaString);
                log.info("FraudAlert schema registered in Schema Registry (subject: {}, id: {})", SCHEMA_SUBJECT, schemaId);
            } catch (Exception e) {
                log.error("Failed to register FraudAlert schema in Schema Registry", e);
                throw new RuntimeException("Failed to register FraudAlert schema", e);
            }
        }
    }

    private String loadSchemaFromResources() throws Exception {
        // Try to load from avro directory in resources
        ClassPathResource resource = new ClassPathResource("avro/FraudAlert.avsc");
        if (!resource.exists()) {
            // Fallback: load from source directory (for development)
            resource = new ClassPathResource("avro/FraudAlert.avsc");
        }
        
        InputStream is = resource.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line).append("\n");
        }
        return sb.toString().trim();
    }
}
