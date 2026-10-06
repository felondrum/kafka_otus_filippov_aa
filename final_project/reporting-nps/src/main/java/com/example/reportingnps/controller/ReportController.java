package com.example.reportingnps.controller;

import com.example.reportingnps.dto.DailyReportResponse;
import com.example.reportingnps.service.ReportService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/api/reports")
@Tag(name = "Reports", description = "API для получения отчётов по звонкам")
public class ReportController {

    private final ReportService reportService;

    public ReportController(ReportService reportService) {
        this.reportService = reportService;
    }

    @GetMapping("/daily")
    @Operation(
            summary = "Получить дневной отчёт",
            description = "Возвращает агрегированный отчёт по звонкам за указанный период: общее количество, средний NPS, разбивка по статусам, сегментам и агентам."
    )
    @ApiResponse(responseCode = "200", description = "Отчёт успешно получен")
    public ResponseEntity<DailyReportResponse> getDailyReport(
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

        DailyReportResponse response = reportService.getDailyReport(from, to);
        return ResponseEntity.ok(response);
    }
}
