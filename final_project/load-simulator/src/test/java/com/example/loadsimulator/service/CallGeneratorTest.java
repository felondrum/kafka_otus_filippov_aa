package com.example.loadsimulator.service;

import com.example.loadsimulator.dto.CallEventRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class CallGeneratorTest {

    private PhoneGenerator phoneGenerator;
    private CallGenerator callGenerator;

    @BeforeEach
    void setUp() {
        phoneGenerator = new PhoneGenerator();
        callGenerator = new CallGenerator(phoneGenerator);
    }

    @Test
    void generateNormalCall_createsValidEvent() {
        CallEventRequest event = callGenerator.generateNormalCall();

        assertNotNull(event.getCallId());
        assertNotNull(event.getPhone());
        assertNotNull(event.getDuration());
        assertNotNull(event.getAgentId());
        assertNotNull(event.getNpsScore());
    }

    @Test
    void generateNormalCall_validatesCallIdIsUuid() {
        CallEventRequest event = callGenerator.generateNormalCall();

        assertTrue(event.getCallId().matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                "callId must be a valid UUID");
    }

    @Test
    void generateNormalCall_validatesPhoneE164() {
        CallEventRequest event = callGenerator.generateNormalCall();

        assertTrue(event.getPhone().matches("^\\+?[1-9]\\d{1,14}$"),
                "phone must be in E.164 format");
    }

    @Test
    void generateNormalCall_durationInRange() {
        CallEventRequest event = callGenerator.generateNormalCall();

        assertTrue(event.getDuration() >= 30 && event.getDuration() <= 300,
                "duration must be between 30 and 300");
    }

    @Test
    void generateNormalCall_npsScoreInRange() {
        CallEventRequest event = callGenerator.generateNormalCall();

        assertTrue(event.getNpsScore() >= 0 && event.getNpsScore() <= 10,
                "npsScore must be between 0 and 10");
    }

    @Test
    void generateBatch_createsCorrectCount() {
        List<CallEventRequest> batch = callGenerator.generateBatch(100);

        assertEquals(100, batch.size());
    }

    @Test
    void generateBatch_allEventsValid() {
        List<CallEventRequest> batch = callGenerator.generateBatch(1000);

        long validCalls = batch.stream()
                .filter(e -> e.getCallId() != null && e.getPhone() != null
                        && e.getDuration() != null && e.getAgentId() != null)
                .count();

        assertEquals(1000, validCalls, "All 1000 calls must be valid");
    }

    @Test
    void generateBatch_allCallIdsUnique() {
        List<CallEventRequest> batch = callGenerator.generateBatch(100);

        long uniqueIds = batch.stream()
                .map(CallEventRequest::getCallId)
                .distinct()
                .count();

        assertEquals(100, uniqueIds, "All callIds must be unique");
    }
}
