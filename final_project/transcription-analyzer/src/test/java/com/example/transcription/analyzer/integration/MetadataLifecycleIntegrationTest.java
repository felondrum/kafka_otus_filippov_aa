package com.example.transcription.analyzer.integration;

import com.example.transcription.analyzer.IntegrationTest;
import com.example.transcription.analyzer.metadata.MetadataManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for metadata lifecycle:
 * all status transitions occur in correct order
 */
@IntegrationTest
class MetadataLifecycleIntegrationTest {

    @Autowired
    private MetadataManager metadataManager;

    @Test
    void testMetadataLifecycleSequence() {
        String callId = "lifecycle-test-1";

        // Step 1: TRANSCRIBING
        metadataManager.onTranscriptionProduced(callId);
        assertTrue(MetadataManager.isValidTransition(null, MetadataManager.STATUS_TRANSCRIBING));

        // Step 2: SUMMARIZING
        metadataManager.onSummaryStarted(callId);
        assertTrue(MetadataManager.isValidTransition(
                MetadataManager.STATUS_TRANSCRIBING,
                MetadataManager.STATUS_SUMMARIZING));

        // Step 3: COMPLETED
        metadataManager.onDualWriteComplete(callId);
        assertTrue(MetadataManager.isValidTransition(
                MetadataManager.STATUS_SUMMARIZING,
                MetadataManager.STATUS_COMPLETED));

        // Verify sequence is correct
        assertTrue(MetadataManager.isValidTransition(null, "TRANSCRIBING"));
        assertTrue(MetadataManager.isValidTransition("TRANSCRIBING", "SUMMARIZING"));
        assertTrue(MetadataManager.isValidTransition("SUMMARIZING", "COMPLETED"));

        // Verify invalid transitions
        assertFalse(MetadataManager.isValidTransition("TRANSCRIBING", "COMPLETED")); // Skip
        assertFalse(MetadataManager.isValidTransition("COMPLETED", "TRANSCRIBING")); // Backwards
    }

    @Test
    void testMultipleCallsIndependent() {
        String callId1 = "lifecycle-test-2a";
        String callId2 = "lifecycle-test-2b";

        // Process call 1
        metadataManager.onTranscriptionProduced(callId1);
        metadataManager.onSummaryStarted(callId1);
        metadataManager.onDualWriteComplete(callId1);

        // Process call 2 (independent)
        metadataManager.onTranscriptionProduced(callId2);
        metadataManager.onSummaryStarted(callId2);
        metadataManager.onDualWriteComplete(callId2);

        // Both should have valid transitions
        assertTrue(MetadataManager.isValidTransition(null, MetadataManager.STATUS_TRANSCRIBING));
        assertTrue(MetadataManager.isValidTransition("SUMMARIZING", MetadataManager.STATUS_COMPLETED));
    }
}
