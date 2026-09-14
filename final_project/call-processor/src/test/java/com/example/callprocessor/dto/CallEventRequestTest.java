package com.example.callprocessor.dto;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class CallEventRequestTest {

    private static Validator validator;

    @BeforeAll
    static void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    void shouldValidateValidRequest() {
        CallEventRequest request = new CallEventRequest();
        request.setCallId("call-123");
        request.setPhone("+71234567890");
        request.setDuration(300);
        request.setAgentId("agent-1");
        request.setNpsScore(8);

        Set<ConstraintViolation<CallEventRequest>> violations = validator.validate(request);
        assertTrue(violations.isEmpty());
    }

    @Test
    void shouldRejectNullCallId() {
        CallEventRequest request = new CallEventRequest();
        request.setPhone("+71234567890");
        request.setDuration(300);
        request.setAgentId("agent-1");
        request.setNpsScore(8);

        Set<ConstraintViolation<CallEventRequest>> violations = validator.validate(request);
        assertFalse(violations.isEmpty());
        assertTrue(violations.stream().anyMatch(v -> v.getPropertyPath().toString().equals("callId")));
    }

    @Test
    void shouldRejectInvalidPhoneFormat() {
        CallEventRequest request = new CallEventRequest();
        request.setCallId("call-123");
        request.setPhone("invalid-phone");
        request.setDuration(300);
        request.setAgentId("agent-1");
        request.setNpsScore(8);

        Set<ConstraintViolation<CallEventRequest>> violations = validator.validate(request);
        assertFalse(violations.isEmpty());
        assertTrue(violations.stream().anyMatch(v -> v.getPropertyPath().toString().equals("phone")));
    }

    @Test
    void shouldRejectZeroDuration() {
        CallEventRequest request = new CallEventRequest();
        request.setCallId("call-123");
        request.setPhone("+71234567890");
        request.setDuration(0);
        request.setAgentId("agent-1");
        request.setNpsScore(8);

        Set<ConstraintViolation<CallEventRequest>> violations = validator.validate(request);
        assertFalse(violations.isEmpty());
        assertTrue(violations.stream().anyMatch(v -> v.getPropertyPath().toString().equals("duration")));
    }

    @Test
    void shouldRejectNullAgentId() {
        CallEventRequest request = new CallEventRequest();
        request.setCallId("call-123");
        request.setPhone("+71234567890");
        request.setDuration(300);
        request.setNpsScore(8);

        Set<ConstraintViolation<CallEventRequest>> violations = validator.validate(request);
        assertFalse(violations.isEmpty());
        assertTrue(violations.stream().anyMatch(v -> v.getPropertyPath().toString().equals("agentId")));
    }

    @Test
    void shouldRejectNpsScoreAbove10() {
        CallEventRequest request = new CallEventRequest();
        request.setCallId("call-123");
        request.setPhone("+71234567890");
        request.setDuration(300);
        request.setAgentId("agent-1");
        request.setNpsScore(11);

        Set<ConstraintViolation<CallEventRequest>> violations = validator.validate(request);
        assertFalse(violations.isEmpty());
        assertTrue(violations.stream().anyMatch(v -> v.getPropertyPath().toString().equals("npsScore")));
    }

    @Test
    void shouldRejectNpsScoreBelow0() {
        CallEventRequest request = new CallEventRequest();
        request.setCallId("call-123");
        request.setPhone("+71234567890");
        request.setDuration(300);
        request.setAgentId("agent-1");
        request.setNpsScore(-1);

        Set<ConstraintViolation<CallEventRequest>> violations = validator.validate(request);
        assertFalse(violations.isEmpty());
        assertTrue(violations.stream().anyMatch(v -> v.getPropertyPath().toString().equals("npsScore")));
    }
}
