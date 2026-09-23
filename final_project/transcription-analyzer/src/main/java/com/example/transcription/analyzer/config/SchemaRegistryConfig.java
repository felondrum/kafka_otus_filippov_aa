package com.example.transcription.analyzer.config;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.CodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PostConstruct;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Map;

@Configuration
public class SchemaRegistryConfig {

    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryConfig.class);

    @Value("${schema-registry.url}")
    private String schemaRegistryUrl;

    @PostConstruct
    public void init() {
        log.info("Initializing Schema Registry client with URL: {}", schemaRegistryUrl);
        registerSchemas();
    }

    @Bean
    public CachedSchemaRegistryClient schemaRegistryClient() {
        return new CachedSchemaRegistryClient(schemaRegistryUrl, 10);
    }

    private void registerSchemas() {
        try {
            CachedSchemaRegistryClient client = schemaRegistryClient();

            // Set global BACKWARD compatibility
            client.setCompatibility(CompatibilityLevel.BACKWARD.name());
            log.info("Set BACKWARD compatibility mode");

            // Register RawTranscription schema
            registerSchema("RawTranscription-value", RAW_TRANSCRIPTION_SCHEMA);

            // Register TranscriptionSummary schema
            registerSchema("TranscriptionSummary-value", TRANSCRIPTION_SUMMARY_SCHEMA);

            // Register EnrichedTranscription schema
            registerSchema("EnrichedTranscription-value", ENRICHED_TRANSCRIPTION_SCHEMA);

            // Register CustomerProfile schema
            registerSchema("CustomerProfile-value", CUSTOMER_PROFILE_SCHEMA);

            log.info("All Avro schemas registered with Schema Registry");
        } catch (Exception e) {
            log.warn("Schema Registry not available at startup, schemas will be registered on first produce: {}", e.getMessage());
        }
    }

    private void registerSchema(String subject, String schemaString) {
        try {
            int version = registerIfNotExists(subject, schemaString);
            log.info("Schema registered: subject={}, version={}", subject, version);
        } catch (Exception e) {
            log.error("Failed to register schema {}: {}", subject, e.getMessage());
        }
    }

    private int registerIfNotExists(String subject, String schemaString) throws Exception {
        try {
            // Check if schema already exists
            int latestVersion = getLatestVersion(subject);
            log.info("Schema already registered: subject={}, version={}", subject, latestVersion);
            return latestVersion;
        } catch (Exception e) {
            // Schema not found, register it
            Schema schema = new Schema.Parser().parse(schemaString);
            int version = registerSchema(subject, schema);
            return version;
        }
    }

    private int registerSchema(String subject, Schema schema) throws Exception {
        CachedSchemaRegistryClient client = schemaRegistryClient();
        int version = client.register(subject, schema);
        log.info("Registered new schema: subject={}, version={}", subject, version);
        return version;
    }

    private int getLatestVersion(String subject) throws Exception {
        CachedSchemaRegistryClient client = schemaRegistryClient();
        return client.getLatestSchemaVersion(subject);
    }

    // RawTranscription schema: callId, text, quality, language
    private static final String RAW_TRANSCRIPTION_SCHEMA = """
            {
              "namespace": "com.example.transcription.avro",
              "type": "record",
              "name": "RawTranscription",
              "fields": [
                {"name": "callId", "type": "string"},
                {"name": "text", "type": "string"},
                {"name": "quality", "type": "float"},
                {"name": "language", "type": "string"}
              ]
            }
            """;

    // TranscriptionSummary schema: callId, problem, solution, sentiment, urgency, confidence
    private static final String TRANSCRIPTION_SUMMARY_SCHEMA = """
            {
              "namespace": "com.example.transcription.avro",
              "type": "record",
              "name": "TranscriptionSummary",
              "fields": [
                {"name": "callId", "type": "string"},
                {"name": "problem", "type": "string"},
                {"name": "solution", "type": "string"},
                {"name": "sentiment", "type": "string"},
                {"name": "urgency", "type": "string"},
                {"name": "confidence", "type": "float"}
              ]
            }
            """;

    // EnrichedTranscription schema: callId, transcriptionText, sentiment, urgency, problem, solution, confidence, segment, riskLevel, priority
    private static final String ENRICHED_TRANSCRIPTION_SCHEMA = """
            {
              "namespace": "com.example.transcription.avro",
              "type": "record",
              "name": "EnrichedTranscription",
              "fields": [
                {"name": "callId", "type": "string"},
                {"name": "transcriptionText", "type": "string"},
                {"name": "sentiment", "type": "string"},
                {"name": "urgency", "type": "string"},
                {"name": "problem", "type": "string"},
                {"name": "solution", "type": "string"},
                {"name": "confidence", "type": "float"},
                {"name": "segment", "type": "string"},
                {"name": "riskLevel", "type": "string"},
                {"name": "priority", "type": "string"}
              ]
            }
            """;

    // CustomerProfile schema: phone (key), segment, riskLevel
    private static final String CUSTOMER_PROFILE_SCHEMA = """
            {
              "namespace": "com.example.transcription.avro",
              "type": "record",
              "name": "CustomerProfile",
              "fields": [
                {"name": "phone", "type": "string"},
                {"name": "segment", "type": {"type": "enum", "name": "Segment", "symbols": ["PREMIUM", "STANDARD", "CORPORATE"]}},
                {"name": "riskLevel", "type": {"type": "enum", "name": "RiskLevel", "symbols": ["LOW", "MEDIUM", "HIGH"]}}
              ]
            }
            """;
}
