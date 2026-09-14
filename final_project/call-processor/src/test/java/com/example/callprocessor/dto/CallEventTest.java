package com.example.callprocessor.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CallEventTest {

    @Test
    void shouldSerializeCallEvent() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        CallEvent event = new CallEvent("call-123", "+71234567890", 300, "agent-1", 8);
        
        String json = mapper.writeValueAsString(event);
        
        assertNotNull(json);
        assertTrue(json.contains("call-123"));
        assertTrue(json.contains("+71234567890"));
    }

    @Test
    void shouldDeserializeCallEvent() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String json = """
            {
                "callId": "call-123",
                "phone": "+71234567890",
                "duration": 300,
                "agentId": "agent-1",
                "npsScore": 8
            }
            """;
        
        CallEvent event = mapper.readValue(json, CallEvent.class);
        
        assertEquals("call-123", event.getCallId());
        assertEquals("+71234567890", event.getPhone());
        assertEquals(300, event.getDuration());
        assertEquals("agent-1", event.getAgentId());
        assertEquals(8, event.getNpsScore());
    }
}
