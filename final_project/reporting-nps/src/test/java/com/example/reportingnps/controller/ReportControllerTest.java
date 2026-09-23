package com.example.reportingnps.controller;

import com.example.reportingnps.dto.DailyReportResponse;
import com.example.reportingnps.service.ReportService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.bean.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(ReportController.class)
class ReportControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private ReportService reportService;

    @Test
    void shouldReturnDailyReport() throws Exception {
        // Given
        DailyReportResponse response = new DailyReportResponse(
                100, 0.85,
                Map.of("COMPLETED", 80, "FAILED", 20),
                Map.of("PREMIUM", 30, "STANDARD", 70),
                Map.of("agent-001", 50, "agent-002", 50)
        );

        when(reportService.getDailyReport(
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now()
        )).thenReturn(response);

        // When & Then
        mockMvc.perform(get("/api/reports/daily"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalCalls").value(100))
                .andExpect(jsonPath("$.averageNps").value(0.85))
                .andExpect(jsonPath("$.callsByStatus.COMPLETED").value(80));
    }

    @Test
    void shouldReturnDailyReportWithDateRange() throws Exception {
        // Given
        DailyReportResponse response = new DailyReportResponse(
                50, 0.9, Map.of(), Map.of(), Map.of()
        );

        when(reportService.getDailyReport(
                LocalDateTime.of(2024, 1, 1, 0, 0),
                LocalDateTime.of(2024, 1, 31, 23, 59)
        )).thenReturn(response);

        // When & Then
        mockMvc.perform(get("/api/reports/daily")
                        .param("from", "2024-01-01T00:00:00")
                        .param("to", "2024-01-31T23:59:00"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalCalls").value(50));
    }
}
