package com.example.transcription.analyzer.integration;

import com.example.transcription.analyzer.metadata.MetadataManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for metadata lifecycle:
 * all status transitions occur in correct order
 */
class MetadataLifecycleIntegrationTest {

    @Test
    void testMetadataLifecycleSequence() {
        // Test transition validation logic
        assertTrue(MetadataManager.isValidTransition(null, MetadataManager.STATUS_TRANSCRIBING));
        assertTrue(MetadataManager.isValidTransition(
                MetadataManager.STATUS_TRANSCRIBING,
                MetadataManager.STATUS_SUMMARIZING));
        assertTrue(MetadataManager.isValidTransition(
                MetadataManager.STATUS_SUMMARIZING,
                MetadataManager.STATUS_COMPLETED));

        assertTrue(MetadataManager.isValidTransition(null, "TRANSCRIBING"));
        assertTrue(MetadataManager.isValidTransition("TRANSCRIBING", "SUMMARIZING"));
        assertTrue(MetadataManager.isValidTransition("SUMMARIZING", "COMPLETED"));

        assertFalse(MetadataManager.isValidTransition("TRANSCRIBING", "COMPLETED"));
        assertFalse(MetadataManager.isValidTransition("COMPLETED", "TRANSCRIBING"));
    }

    @Test
    void testMultipleCallsIndependent() {
        // Verify transition logic works for multiple calls
        assertTrue(MetadataManager.isValidTransition(null, MetadataManager.STATUS_TRANSCRIBING));
        assertTrue(MetadataManager.isValidTransition("SUMMARIZING", MetadataManager.STATUS_COMPLETED));
    }
}
