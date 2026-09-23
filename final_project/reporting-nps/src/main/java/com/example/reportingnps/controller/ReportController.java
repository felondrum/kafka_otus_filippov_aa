package com.example.reportingnps.controller;

import com.example.reportingnps.dto.DailyReportResponse;
import com.example.reportingnps.service.ReportService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/api/reports")
public class ReportController {

    private final ReportService reportService;

    public ReportController(ReportService reportService) {
        this.reportService = reportService;
    }

    @GetMapping("/daily")
    public ResponseEntity<DailyReportResponse> getDailyReport(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {

        if (from == null) {
            from = LocalDateTime.now().minusDays(1);
        }
        if (to == null) {
            to = LocalDateTime.now();
        }

        DailyReportResponse response = reportService.getDailyReport(from, to);
        return ResponseEntity.ok(response);
    }
}
