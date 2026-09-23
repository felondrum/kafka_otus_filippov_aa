package com.example.transcription.analyzer.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MetadataManagerTest {

    @Test
    void testIsValidTransition() {
        // First transition (no current status) -> TRANSCRIBING is valid
        assertTrue(MetadataManager.isValidTransition(null, MetadataManager.STATUS_TRANSCRIBING));

        // TRANSCRIBING -> SUMMARIZING is valid
        assertTrue(MetadataManager.isValidTransition(MetadataManager.STATUS_TRANSCRIBING, MetadataManager.STATUS_SUMMARIZING));

        // SUMMARIZING -> COMPLETED is valid
        assertTrue(MetadataManager.isValidTransition(MetadataManager.STATUS_SUMMARIZING, MetadataManager.STATUS_COMPLETED));

        // TRANSCRIBING -> COMPLETED is NOT valid (skip)
        assertFalse(MetadataManager.isValidTransition(MetadataManager.STATUS_TRANSCRIBING, MetadataManager.STATUS_COMPLETED));

        // COMPLETED -> TRANSCRIBING is NOT valid (backwards)
        assertFalse(MetadataManager.isValidTransition(MetadataManager.STATUS_COMPLETED, MetadataManager.STATUS_TRANSCRIBING));
    }

    @Test
    void testStatusConstants() {
        assertEquals("TRANSCRIBING", MetadataManager.STATUS_TRANSCRIBING);
        assertEquals("SUMMARIZING", MetadataManager.STATUS_SUMMARIZING);
        assertEquals("COMPLETED", MetadataManager.STATUS_COMPLETED);
    }
}
