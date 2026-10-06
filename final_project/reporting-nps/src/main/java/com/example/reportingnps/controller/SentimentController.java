package com.example.reportingnps.controller;

import com.example.reportingnps.dto.SentimentDistributionResponse;
import com.example.reportingnps.service.SentimentService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/api/sentiment")
@Tag(name = "Sentiment", description = "API для аналитики настроений клиентов")
public class SentimentController {

    private final SentimentService sentimentService;

    public SentimentController(SentimentService sentimentService) {
        this.sentimentService = sentimentService;
    }

    @GetMapping("/distribution")
    @Operation(
            summary = "Получить распределение настроений",
            description = "Возвращает количество и процент позитивных, нейтральных и негативных отзывов за указанный период."
    )
    @ApiResponse(responseCode = "200", description = "Распределение успешно получено")
    public ResponseEntity<SentimentDistributionResponse> getSentimentDistribution(
            @Parameter(description = "Начало периода (ISO 8601 datetime, по умолчанию — вчера)")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @Parameter(description = "Конец периода (ISO 8601 datetime, по умолчанию — сейчас)")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {

        if (from == null) {
            from = LocalDateTime.now().withHour(0).withMinute(0).withSecond(0).withNano(0);
        }
        if (to == null) {
            to = LocalDateTime.now();
        }

        SentimentDistributionResponse response = sentimentService.getSentimentDistribution(from, to);
        return ResponseEntity.ok(response);
    }
}
