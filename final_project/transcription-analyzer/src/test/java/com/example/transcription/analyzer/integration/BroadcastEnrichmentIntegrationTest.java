package com.example.transcription.analyzer.integration;

import com.example.transcription.analyzer.IntegrationTest;
import com.example.transcription.analyzer.enrichment.CustomerProfileManager;
import com.example.transcription.analyzer.enrichment.EnrichmentService;
import com.example.transcription.avro.CustomerProfile;
import com.example.transcription.avro.EnrichedTranscription;
import com.example.transcription.avro.TranscriptionSummary;
import com.example.transcription.avro.RiskLevel;
import com.example.transcription.avro.Segment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for broadcast enrichment: call with customer profile
 * → enriched event includes segment + riskLevel
 */
@IntegrationTest
class BroadcastEnrichmentIntegrationTest {

    @Autowired
    private CustomerProfileManager customerProfileManager;

    @Autowired
    private EnrichmentService enrichmentService;

    @BeforeEach
    void setUp() {
        customerProfileManager.clear();
    }

    @Test
    void testBroadcastEnrichmentWithCustomerProfile() {
        // Load customer profile
        CustomerProfile profile = new CustomerProfile();
        profile.setPhone("+71234567890");
        profile.setSegment(Segment.PREMIUM);
        profile.setRiskLevel(RiskLevel.HIGH);
        customerProfileManager.loadProfile(profile);

        // Create a summary
        TranscriptionSummary summary = new TranscriptionSummary(
                "enrich-test-1", "fraud_suspected", "card_blocked",
                "negative", "critical", 0.8f);

        // Enrich with customer profile
        EnrichedTranscription enriched = enrichmentService.enrich(
                summary, "test text", "+71234567890");

        // Verify enrichment includes customer data
        assertEquals("enrich-test-1", enriched.getCallId().toString());
        assertEquals("PREMIUM", enriched.getSegment());
        assertEquals("HIGH", enriched.getRiskLevel());
        assertEquals("critical", enriched.getPriority()); // premium + high = critical
    }

    @Test
    void testBroadcastEnrichmentPriorityMatrix() {
        TranscriptionSummary summary = new TranscriptionSummary(
                "priority-test", "other", "resolved_on_call",
                "neutral", "low", 0.0f);

        // Test premium + low = high
        CustomerProfile p1 = new CustomerProfile();
        p1.setPhone("+1"); p1.setSegment(Segment.PREMIUM);
        p1.setRiskLevel(RiskLevel.LOW);
        customerProfileManager.loadProfile(p1);
        EnrichedTranscription e1 = enrichmentService.enrich(summary, "", "+1");
        assertEquals("high", e1.getPriority());

        // Test standard + high = high
        customerProfileManager.clear();
        CustomerProfile p2 = new CustomerProfile();
        p2.setPhone("+2"); p2.setSegment(Segment.STANDARD);
        p2.setRiskLevel(RiskLevel.HIGH);
        customerProfileManager.loadProfile(p2);
        EnrichedTranscription e2 = enrichmentService.enrich(summary, "", "+2");
        assertEquals("high", e2.getPriority());

        // Test corporate + medium = high
        customerProfileManager.clear();
        CustomerProfile p3 = new CustomerProfile();
        p3.setPhone("+3"); p3.setSegment(Segment.CORPORATE);
        p3.setRiskLevel(RiskLevel.MEDIUM);
        customerProfileManager.loadProfile(p3);
        EnrichedTranscription e3 = enrichmentService.enrich(summary, "", "+3");
        assertEquals("high", e3.getPriority());
    }
}
