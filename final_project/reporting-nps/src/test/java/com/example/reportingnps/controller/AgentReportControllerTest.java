package com.example.reportingnps.controller;

import com.example.reportingnps.dto.AgentReportResponse;
import com.example.reportingnps.service.AgentReportService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(AgentReportController.class)
class AgentReportControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private AgentReportService agentReportService;

    @Test
    void shouldReturnAgentReport() throws Exception {
        // Given
        AgentReportResponse response = new AgentReportResponse(
                "agent-001", 100, 0.85, java.util.Map.of(), java.util.Map.of(), 5, true
        );

        when(agentReportService.getAgentReport("agent-001")).thenReturn(response);

        // When & Then
        mockMvc.perform(get("/api/reports/agent/agent-001"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.agentId").value("agent-001"))
                .andExpect(jsonPath("$.totalCalls").value(100))
                .andExpect(jsonPath("$.fraudAlerts").value(5));
    }

    @Test
    void shouldReturn404WhenAgentNotFound() throws Exception {
        // Given
        AgentReportResponse response = new AgentReportResponse(
                "agent-999", 0, 0.0, java.util.Map.of(), java.util.Map.of(), 0, false
        );

        when(agentReportService.getAgentReport("agent-999")).thenReturn(response);

        // When & Then
        mockMvc.perform(get("/api/reports/agent/agent-999"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.found").value(false));
    }
}
