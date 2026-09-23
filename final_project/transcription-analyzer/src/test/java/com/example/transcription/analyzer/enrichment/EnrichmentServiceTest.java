package com.example.transcription.analyzer.enrichment;

import com.example.transcription.avro.CustomerProfile;
import com.example.transcription.avro.EnrichedTranscription;
import com.example.transcription.avro.TranscriptionSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EnrichmentServiceTest {

    private CustomerProfileManager profileManager;
    private EnrichmentService enrichmentService;

    @BeforeEach
    void setUp() {
        profileManager = new CustomerProfileManager();
        enrichmentService = new EnrichmentService(profileManager);
    }

    @Test
    void testEnrichWithCustomerProfile() {
        // Load a customer profile
        CustomerProfile profile = new CustomerProfile();
        profile.setPhone("+71234567890");
        profile.setSegment(CustomerProfile.Segment.PREMIUM);
        profile.setRiskLevel(CustomerProfile.RiskLevel.HIGH);
        profileManager.loadProfile(profile);

        // Create a summary
        TranscriptionSummary summary = new TranscriptionSummary("call-1", "fraud_suspected", "card_blocked",
                "negative", "critical", 0.8f);

        EnrichedTranscription enriched = enrichmentService.enrich(summary, "test text", "+71234567890");

        assertEquals("call-1", enriched.getCallId().toString());
        assertEquals("PREMIUM", enriched.getSegment());
        assertEquals("HIGH", enriched.getRiskLevel());
        assertEquals("critical", enriched.getPriority()); // premium + high = critical
    }

    @Test
    void testEnrichWithMissingProfile() {
        TranscriptionSummary summary = new TranscriptionSummary("call-2", "other", "resolved_on_call",
                "neutral", "low", 0.0f);

        EnrichedTranscription enriched = enrichmentService.enrich(summary, "test text", "+99999999999");

        assertEquals("call-2", enriched.getCallId().toString());
        assertEquals("STANDARD", enriched.getSegment()); // Default
        assertEquals("LOW", enriched.getRiskLevel()); // Default
        assertEquals("normal", enriched.getPriority()); // standard + low = normal
    }

    @Test
    void testPriorityMatrix() {
        TranscriptionSummary summary = new TranscriptionSummary("call-x", "other", "resolved_on_call",
                "neutral", "low", 0.0f);

        // premium + low = high
        profileManager.clear();
        CustomerProfile p1 = new CustomerProfile();
        p1.setPhone("+1"); p1.setSegment(CustomerProfile.Segment.PREMIUM);
        p1.setRiskLevel(CustomerProfile.RiskLevel.LOW);
        profileManager.loadProfile(p1);
        EnrichedTranscription e1 = enrichmentService.enrich(summary, "", "+1");
        assertEquals("high", e1.getPriority());

        // premium + medium = high
        profileManager.clear();
        CustomerProfile p2 = new CustomerProfile();
        p2.setPhone("+2"); p2.setSegment(CustomerProfile.Segment.PREMIUM);
        p2.setRiskLevel(CustomerProfile.RiskLevel.MEDIUM);
        profileManager.loadProfile(p2);
        EnrichedTranscription e2 = enrichmentService.enrich(summary, "", "+2");
        assertEquals("high", e2.getPriority());

        // premium + high = critical
        profileManager.clear();
        CustomerProfile p3 = new CustomerProfile();
        p3.setPhone("+3"); p3.setSegment(CustomerProfile.Segment.PREMIUM);
        p3.setRiskLevel(CustomerProfile.RiskLevel.HIGH);
        profileManager.loadProfile(p3);
        EnrichedTranscription e3 = enrichmentService.enrich(summary, "", "+3");
        assertEquals("critical", e3.getPriority());

        // standard + low = normal
        profileManager.clear();
        CustomerProfile p4 = new CustomerProfile();
        p4.setPhone("+4"); p4.setSegment(CustomerProfile.Segment.STANDARD);
        p4.setRiskLevel(CustomerProfile.RiskLevel.LOW);
        profileManager.loadProfile(p4);
        EnrichedTranscription e4 = enrichmentService.enrich(summary, "", "+4");
        assertEquals("normal", e4.getPriority());

        // standard + high = high
        profileManager.clear();
        CustomerProfile p5 = new CustomerProfile();
        p5.setPhone("+5"); p5.setSegment(CustomerProfile.Segment.STANDARD);
        p5.setRiskLevel(CustomerProfile.RiskLevel.HIGH);
        profileManager.loadProfile(p5);
        EnrichedTranscription e5 = enrichmentService.enrich(summary, "", "+5");
        assertEquals("high", e5.getPriority());

        // corporate + medium = high
        profileManager.clear();
        CustomerProfile p6 = new CustomerProfile();
        p6.setPhone("+6"); p6.setSegment(CustomerProfile.Segment.CORPORATE);
        p6.setRiskLevel(CustomerProfile.RiskLevel.MEDIUM);
        profileManager.loadProfile(p6);
        EnrichedTranscription e6 = enrichmentService.enrich(summary, "", "+6");
        assertEquals("high", e6.getPriority());

        // corporate + high = critical
        profileManager.clear();
        CustomerProfile p7 = new CustomerProfile();
        p7.setPhone("+7"); p7.setSegment(CustomerProfile.Segment.CORPORATE);
        p7.setRiskLevel(CustomerProfile.RiskLevel.HIGH);
        profileManager.loadProfile(p7);
        EnrichedTranscription e7 = enrichmentService.enrich(summary, "", "+7");
        assertEquals("critical", e7.getPriority());
    }
}
