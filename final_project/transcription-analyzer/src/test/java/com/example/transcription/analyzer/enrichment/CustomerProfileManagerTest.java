package com.example.transcription.analyzer.enrichment;

import com.example.transcription.avro.CustomerProfile;
import com.example.transcription.avro.EnrichedTranscription;
import com.example.transcription.avro.TranscriptionSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CustomerProfileManagerTest {

    private CustomerProfileManager manager;

    @BeforeEach
    void setUp() {
        manager = new CustomerProfileManager();
    }

    @Test
    void testLoadAndLookupProfile() {
        CustomerProfile profile = new CustomerProfile();
        profile.setPhone("+71234567890");
        profile.setSegment(CustomerProfile.Segment.PREMIUM);
        profile.setRiskLevel(CustomerProfile.RiskLevel.HIGH);

        manager.loadProfile(profile);
        assertEquals(1, manager.getProfileCount());

        CustomerProfile lookup = manager.lookup("+71234567890");
        assertNotNull(lookup);
        assertEquals(CustomerProfile.Segment.PREMIUM, lookup.getSegment());
        assertEquals(CustomerProfile.RiskLevel.HIGH, lookup.getRiskLevel());
    }

    @Test
    void testMissingProfileReturnsNull() {
        CustomerProfile lookup = manager.lookup("+99999999999");
        assertNull(lookup);
    }

    @Test
    void testClear() {
        CustomerProfile profile = new CustomerProfile();
        profile.setPhone("+71234567890");
        profile.setSegment(CustomerProfile.Segment.STANDARD);
        profile.setRiskLevel(CustomerProfile.RiskLevel.LOW);

        manager.loadProfile(profile);
        assertEquals(1, manager.getProfileCount());

        manager.clear();
        assertEquals(0, manager.getProfileCount());
    }

    @Test
    void testUpdateProfile() {
        CustomerProfile profile1 = new CustomerProfile();
        profile1.setPhone("+71234567890");
        profile1.setSegment(CustomerProfile.Segment.STANDARD);
        profile1.setRiskLevel(CustomerProfile.RiskLevel.LOW);
        manager.loadProfile(profile1);

        CustomerProfile profile2 = new CustomerProfile();
        profile2.setPhone("+71234567890");
        profile2.setSegment(CustomerProfile.Segment.PREMIUM);
        profile2.setRiskLevel(CustomerProfile.RiskLevel.HIGH);
        manager.loadProfile(profile2);

        CustomerProfile lookup = manager.lookup("+71234567890");
        assertEquals(CustomerProfile.Segment.PREMIUM, lookup.getSegment());
        assertEquals(1, manager.getProfileCount());
    }
}
