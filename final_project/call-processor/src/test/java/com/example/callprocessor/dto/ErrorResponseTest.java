package com.example.callprocessor.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ErrorResponseTest {

    @Test
    void shouldSerializeErrorResponse() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ErrorResponse response = new ErrorResponse("VALIDATION_ERROR", "callId is required");
        
        String json = mapper.writeValueAsString(response);
        
        assertNotNull(json);
        assertTrue(json.contains("VALIDATION_ERROR"));
        assertTrue(json.contains("callId is required"));
        assertTrue(json.contains("timestamp"));
    }
}
