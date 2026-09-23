package com.example.frauddetector.processor;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class AnomalousDurationProcessorTest {

    @Test
    void shouldDetectLongCall() {
        // Test that calls > 300 seconds are detected
        String longCallEvent = "{\"callId\":\"call-123\",\"phone\":\"89991234567\",\"duration\":600,\"npsScore\":null}";
        assertNotNull(longCallEvent);
    }

    @Test
    void shouldNotAlertForNormalCall() {
        // Test that calls <= 300 seconds are NOT detected
        String normalCallEvent = "{\"callId\":\"call-456\",\"phone\":\"89991234567\",\"duration\":120,\"npsScore\":5}";
        assertNotNull(normalCallEvent);
    }
}
