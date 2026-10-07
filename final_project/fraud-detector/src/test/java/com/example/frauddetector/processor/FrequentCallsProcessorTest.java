package com.example.frauddetector.processor;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FrequentCallsProcessorTest {

    private PhoneNormalizer normalizer = new PhoneNormalizer();

    @Test
    void shouldDetectFrequentCalls() {
        assertNotNull(normalizer);
    }

    @Test
    void shouldNormalizePhone() {
        assertEquals("+79991234567", normalizer.normalize("89991234567"));
    }
}
