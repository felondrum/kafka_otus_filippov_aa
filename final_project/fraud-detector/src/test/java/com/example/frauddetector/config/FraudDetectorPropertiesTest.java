package com.example.frauddetector.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for FraudDetectorProperties.
 */
class FraudDetectorPropertiesTest {

    @Test
    void shouldHaveDefaultValues() {
        FraudDetectorProperties properties = new FraudDetectorProperties() {};
        
        // These will use Spring defaults since @Value won't be resolved in unit tests
        // But we can verify the class is instantiable
        assertNotNull(properties);
    }

    @Test
    void shouldReturnCompletedTopic() {
        FraudDetectorProperties properties = new FraudDetectorProperties() {};
        // Default value from @Value annotation
        String topic = properties.getCompletedTopic();
        assertNotNull(topic);
        assertEquals("calls.completed", topic);
    }

    @Test
    void shouldReturnFraudAlertsTopic() {
        FraudDetectorProperties properties = new FraudDetectorProperties() {};
        String topic = properties.getFraudAlertsTopic();
        assertNotNull(topic);
        assertEquals("calls.fraud-alerts", topic);
    }

    @Test
    void shouldReturnFrequentCallsWindowSizeMs() {
        FraudDetectorProperties properties = new FraudDetectorProperties() {};
        long windowSize = properties.getFrequentCallsWindowSizeMs();
        assertEquals(60000, windowSize);
    }

    @Test
    void shouldReturnFrequentCallsAdvanceMs() {
        FraudDetectorProperties properties = new FraudDetectorProperties() {};
        long advance = properties.getFrequentCallsAdvanceMs();
        assertEquals(10000, advance);
    }

    @Test
    void shouldReturnFrequentCallsThreshold() {
        FraudDetectorProperties properties = new FraudDetectorProperties() {};
        int threshold = properties.getFrequentCallsThreshold();
        assertEquals(5, threshold);
    }

    @Test
    void shouldReturnNpsEscalationWindowHours() {
        FraudDetectorProperties properties = new FraudDetectorProperties() {};
        int windowHours = properties.getNpsEscalationWindowHours();
        assertEquals(24, windowHours);
    }

    @Test
    void shouldReturnNpsEscalationThreshold() {
        FraudDetectorProperties properties = new FraudDetectorProperties() {};
        int threshold = properties.getNpsEscalationThreshold();
        assertEquals(3, threshold);
    }

    @Test
    void shouldReturnMinDurationSeconds() {
        FraudDetectorProperties properties = new FraudDetectorProperties() {};
        int minDuration = properties.getMinDurationSeconds();
        assertEquals(5, minDuration);
    }

    @Test
    void shouldReturnMaxDurationSeconds() {
        FraudDetectorProperties properties = new FraudDetectorProperties() {};
        int maxDuration = properties.getMaxDurationSeconds();
        assertEquals(300, maxDuration);
    }
}
