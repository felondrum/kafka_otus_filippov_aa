package com.example.transcription.analyzer.integration;

import com.example.transcription.analyzer.IntegrationTest;
import com.example.transcription.analyzer.enrichment.CustomerProfileManager;
import com.example.transcription.analyzer.enrichment.EnrichmentService;
import com.example.transcription.avro.EnrichedTranscription;
import com.example.transcription.avro.TranscriptionSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for missing customer profile:
 * call without profile → default values used
 */
@IntegrationTest
class MissingCustomerProfileIntegrationTest {

    @Autowired
    private CustomerProfileManager customerProfileManager;

    @Autowired
    private EnrichmentService enrichmentService;

    @BeforeEach
    void setUp() {
        customerProfileManager.clear();
    }

    @Test
    void testMissingProfileReturnsDefaults() {
        TranscriptionSummary summary = new TranscriptionSummary(
                "missing-profile-test", "other", "resolved_on_call",
                "neutral", "low", 0.0f);

        // Enrich with non-existent phone
        EnrichedTranscription enriched = enrichmentService.enrich(
                summary, "test text", "+99999999999");

        // Should use default values
        assertEquals("missing-profile-test", enriched.getCallId().toString());
        assertEquals("STANDARD", enriched.getSegment()); // Default
        assertEquals("LOW", enriched.getRiskLevel()); // Default
        assertEquals("normal", enriched.getPriority()); // standard + low = normal
    }

    @Test
    void testEmptyProfileManagerReturnsDefaults() {
        // Verify manager is empty
        assertEquals(0, customerProfileManager.getProfileCount());

        TranscriptionSummary summary = new TranscriptionSummary(
                "empty-manager-test", "card_loss", "card_blocked",
                "negative", "high", 0.7f);

        EnrichedTranscription enriched = enrichmentService.enrich(
                summary, "lost my card", "+00000000000");

        // Should still enrich with defaults
        assertEquals("STANDARD", enriched.getSegment());
        assertEquals("LOW", enriched.getRiskLevel());
        assertEquals("normal", enriched.getPriority());
    }
}
