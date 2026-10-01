package com.example.reportingnps.controller;

import com.example.reportingnps.dto.SentimentDistributionResponse;
import com.example.reportingnps.service.SentimentService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(SentimentController.class)
class SentimentControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private SentimentService sentimentService;

    @Test
    void shouldReturnSentimentDistribution() throws Exception {
        // Given
        SentimentDistributionResponse response = new SentimentDistributionResponse(
                50, 30, 20, 50.0, 30.0, 20.0
        );

        when(sentimentService.getSentimentDistribution(any(), any())).thenReturn(response);

        // When & Then
        mockMvc.perform(get("/api/sentiment/distribution"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.positive").value(50))
                .andExpect(jsonPath("$.neutral").value(30))
                .andExpect(jsonPath("$.negative").value(20))
                .andExpect(jsonPath("$.positivePercent").value(50.0))
                .andExpect(jsonPath("$.neutralPercent").value(30.0))
                .andExpect(jsonPath("$.negativePercent").value(20.0));
    }
}
