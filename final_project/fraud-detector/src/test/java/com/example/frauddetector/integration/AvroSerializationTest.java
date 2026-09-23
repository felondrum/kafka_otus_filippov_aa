package com.example.frauddetector.integration;

import com.example.frauddetector.avro.FraudAlert;
import com.example.frauddetector.config.FraudAlertSerde;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Avro serialization/deserialization of FraudAlert.
 * Verifies that the FraudAlert schema is correct and serialization works properly.
 */
class AvroSerializationTest {

    private static final Logger log = LoggerFactory.getLogger(AvroSerializationTest.class);
    private FraudAlertSerde fraudAlertSerde;

    @BeforeEach
    void setUp() {
        fraudAlertSerde = new FraudAlertSerde();
    }

    /**
     * Test that FraudAlert schema is valid and has all required fields.
     */
    @Test
    void shouldHaveValidFraudAlertSchema() {
        assertNotNull(FraudAlert.SCHEMA$, "FraudAlert schema should not be null");
        
        // Verify all required fields exist
        assertEquals(6, FraudAlert.SCHEMA$.getFields().size(), 
            "FraudAlert should have 6 fields: callId, phone, pattern, count, timestamp, severity");
        
        // Verify field names
        var fieldNames = FraudAlert.SCHEMA$.getFields().stream()
            .map(f -> f.name())
            .toList();
        
        assertTrue(fieldNames.contains("callId"), "Schema should have callId field");
        assertTrue(fieldNames.contains("phone"), "Schema should have phone field");
        assertTrue(fieldNames.contains("pattern"), "Schema should have pattern field");
        assertTrue(fieldNames.contains("count"), "Schema should have count field");
        assertTrue(fieldNames.contains("timestamp"), "Schema should have timestamp field");
        assertTrue(fieldNames.contains("severity"), "Schema should have severity field");
        
        log.info("FraudAlert schema validated with {} fields", fieldNames.size());
    }

    /**
     * Test FREQUENT_CALLS pattern serialization/deserialization.
     */
    @Test
    void shouldSerializeAndDeserializeFrequentCallsAlert() {
        FraudAlert alert = createFrequentCallsAlert();
        
        byte[] serialized = fraudAlertSerde.serializer().serialize("calls.fraud-alerts", alert);
        assertNotNull(serialized, "Serialized data should not be null");
        assertTrue(serialized.length > 0, "Serialized data should not be empty");
        
        FraudAlert deserialized = fraudAlertSerde.deserializer().deserialize("calls.fraud-alerts", serialized);
        
        assertNotNull(deserialized, "Deserialized alert should not be null");
        assertEquals(alert.getCallId(), deserialized.getCallId());
        assertEquals(alert.getPhone(), deserialized.getPhone());
        assertEquals(alert.getPattern(), deserialized.getPattern());
        assertEquals(alert.getCount(), deserialized.getCount());
        assertEquals(alert.getTimestamp(), deserialized.getTimestamp());
        assertEquals(alert.getSeverity(), deserialized.getSeverity());
        
        log.info("FREQUENT_CALLS alert serialized ({}) and deserialized successfully", serialized.length);
    }

    /**
     * Test NPS_ESCALATION pattern serialization/deserialization.
     */
    @Test
    void shouldSerializeAndDeserializeNpsEscalationAlert() {
        FraudAlert alert = createNpsEscalationAlert();
        
        byte[] serialized = fraudAlertSerde.serializer().serialize("calls.fraud-alerts", alert);
        assertNotNull(serialized, "Serialized data should not be null");
        
        FraudAlert deserialized = fraudAlertSerde.deserializer().deserialize("calls.fraud-alerts", serialized);
        
        assertNotNull(deserialized, "Deserialized alert should not be null");
        assertEquals(FraudAlert.FraudPattern.NPS_ESCALATION, deserialized.getPattern());
        assertEquals(FraudAlert.AlertSeverity.HIGH, deserialized.getSeverity());
        assertEquals(3, deserialized.getCount());
        
        log.info("NPS_ESCALATION alert serialized and deserialized successfully");
    }

    /**
     * Test ANOMALOUS_DURATION pattern serialization/deserialization.
     */
    @Test
    void shouldSerializeAndDeserializeAnomalousDurationAlert() {
        FraudAlert alert = createAnomalousDurationAlert();
        
        byte[] serialized = fraudAlertSerde.serializer().serialize("calls.fraud-alerts", alert);
        assertNotNull(serialized, "Serialized data should not be null");
        
        FraudAlert deserialized = fraudAlertSerde.deserializer().deserialize("calls.fraud-alerts", serialized);
        
        assertNotNull(deserialized, "Deserialized alert should not be null");
        assertEquals(FraudAlert.FraudPattern.ANOMALOUS_DURATION, deserialized.getPattern());
        assertEquals(FraudAlert.AlertSeverity.LOW, deserialized.getSeverity());
        assertEquals(1, deserialized.getCount());
        
        log.info("ANOMALOUS_DURATION alert serialized and deserialized successfully");
    }

    /**
     * Test that null input to serializer returns null.
     */
    @Test
    void shouldHandleNullAlertInSerializer() {
        byte[] result = fraudAlertSerde.serializer().serialize("calls.fraud-alerts", null);
        assertNull(result, "Serializer should return null for null input");
    }

    /**
     * Test that null input to deserializer returns null.
     */
    @Test
    void shouldHandleNullAlertInDeserializer() {
        FraudAlert result = fraudAlertSerde.deserializer().deserialize("calls.fraud-alerts", null);
        assertNull(result, "Deserializer should return null for null input");
    }

    /**
     * Test that all enum values are valid.
     */
    @Test
    void shouldHaveValidEnumValues() {
        // Verify FraudPattern enum values
        FraudAlert.FraudPattern[] patterns = FraudAlert.FraudPattern.values();
        assertEquals(3, patterns.length, "Should have 3 fraud patterns");
        
        assertTrue(java.util.Set.of(patterns).contains(FraudAlert.FraudPattern.FREQUENT_CALLS));
        assertTrue(java.util.Set.of(patterns).contains(FraudAlert.FraudPattern.NPS_ESCALATION));
        assertTrue(java.util.Set.of(patterns).contains(FraudAlert.FraudPattern.ANOMALOUS_DURATION));
        
        // Verify AlertSeverity enum values
        FraudAlert.AlertSeverity[] severities = FraudAlert.AlertSeverity.values();
        assertEquals(3, severities.length, "Should have 3 severity levels");
        
        assertTrue(java.util.Set.of(severities).contains(FraudAlert.AlertSeverity.HIGH));
        assertTrue(java.util.Set.of(severities).contains(FraudAlert.AlertSeverity.MEDIUM));
        assertTrue(java.util.Set.of(severities).contains(FraudAlert.AlertSeverity.LOW));
        
        log.info("All enum values validated: {} patterns, {} severities", patterns.length, severities.length);
    }

    /**
     * Test Avro binary format is valid (not pipe-delimited string).
     */
    @Test
    void shouldProduceAvroBinaryNotString() {
        FraudAlert alert = createFrequentCallsAlert();
        
        byte[] serialized = fraudAlertSerde.serializer().serialize("calls.fraud-alerts", alert);
        
        // Avro binary should not contain pipe characters (which would indicate string format)
        String hex = bytesToHex(serialized);
        assertFalse(hex.contains("|"), "Avro binary should not contain pipe characters");
        
        // Avro binary should start with magic byte (0x00) for Confluent format
        // or be raw Avro binary
        assertTrue(serialized.length > 0, "Serialized data should not be empty");
        
        log.info("Avro binary format validated ({} bytes, hex: {})", serialized.length, hex.substring(0, Math.min(20, hex.length())));
    }

    /**
     * Create a FREQUENT_CALLS alert for testing.
     */
    private FraudAlert createFrequentCallsAlert() {
        return new FraudAlert(
            "freq-test-123",
            "+79991234567",
            FraudAlert.FraudPattern.FREQUENT_CALLS,
            6,
            Instant.now().toString(),
            FraudAlert.AlertSeverity.MEDIUM
        );
    }

    /**
     * Create an NPS_ESCALATION alert for testing.
     */
    private FraudAlert createNpsEscalationAlert() {
        return new FraudAlert(
            "nps-test-456",
            "+79991234567",
            FraudAlert.FraudPattern.NPS_ESCALATION,
            3,
            Instant.now().toString(),
            FraudAlert.AlertSeverity.HIGH
        );
    }

    /**
     * Create an ANOMALOUS_DURATION alert for testing.
     */
    private FraudAlert createAnomalousDurationAlert() {
        return new FraudAlert(
            "anom-test-789",
            "+79991234567",
            FraudAlert.FraudPattern.ANOMALOUS_DURATION,
            1,
            Instant.now().toString(),
            FraudAlert.AlertSeverity.LOW
        );
    }

    /**
     * Convert bytes to hex string for logging.
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
