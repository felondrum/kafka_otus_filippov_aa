package com.example.reportingnps.controller;

import com.example.reportingnps.dto.AgentReportResponse;
import com.example.reportingnps.service.AgentReportService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/reports")
@Tag(name = "Agent Reports", description = "API для получения отчётов по агентам")
public class AgentReportController {

    private final AgentReportService agentReportService;

    public AgentReportController(AgentReportService agentReportService) {
        this.agentReportService = agentReportService;
    }

    @GetMapping("/agent/{agentId}")
    @Operation(
            summary = "Получить отчёт по агенту",
            description = "Возвращает отчёт по конкретному агенту: количество звонков, средний NPS, разбивка, количество алертов фрода."
    )
    @ApiResponse(responseCode = "200", description = "Отчёт успешно получен")
    @ApiResponse(responseCode = "404", description = "Агент не найден")
    public ResponseEntity<AgentReportResponse> getAgentReport(
            @Parameter(description = "ID агента", required = true)
            @PathVariable String agentId) {
        AgentReportResponse response = agentReportService.getAgentReport(agentId);
        if (!response.isFound()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
        }
        return ResponseEntity.ok(response);
    }
}
