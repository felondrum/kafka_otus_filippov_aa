package com.example.loadsimulator.service;

import com.example.loadsimulator.dto.CallEventRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AnomalousDurationCallFactoryTest {

    private PhoneGenerator phoneGenerator;
    private AnomalousDurationCallFactory factory;

    @BeforeEach
    void setUp() {
        phoneGenerator = new PhoneGenerator();
        factory = new AnomalousDurationCallFactory(phoneGenerator);
    }

    @Test
    void generate_allCallsHaveAnomalousDuration() {
        for (int i = 0; i < 100; i++) {
            CallEventRequest event = factory.generate(i);
            assertTrue(event.getDuration() >= 301 && event.getDuration() <= 600,
                    "Duration must be between 301 and 600, got " + event.getDuration());
        }
    }

    @Test
    void generate_validatesAllFields() {
        CallEventRequest event = factory.generate(0);

        assertNotNull(event.getCallId());
        assertNotNull(event.getPhone());
        assertNotNull(event.getAgentId());
        assertNotNull(event.getNpsScore());
        assertTrue(event.getNpsScore() >= 0 && event.getNpsScore() <= 10);
    }
}
