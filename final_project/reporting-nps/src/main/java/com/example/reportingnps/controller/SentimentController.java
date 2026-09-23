package com.example.reportingnps.controller;

import com.example.reportingnps.dto.SentimentDistributionResponse;
import com.example.reportingnps.service.SentimentService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/api/sentiment")
public class SentimentController {

    private final SentimentService sentimentService;

    public SentimentController(SentimentService sentimentService) {
        this.sentimentService = sentimentService;
    }

    @GetMapping("/distribution")
    public ResponseEntity<SentimentDistributionResponse> getSentimentDistribution(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {

        if (from == null) {
            from = LocalDateTime.now().minusDays(1);
        }
        if (to == null) {
            to = LocalDateTime.now();
        }

        SentimentDistributionResponse response = sentimentService.getSentimentDistribution(from, to);
        return ResponseEntity.ok(response);
    }
}
