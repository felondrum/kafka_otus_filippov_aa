package com.example.reportingnps.controller;

import com.example.reportingnps.dto.AgentReportResponse;
import com.example.reportingnps.service.AgentReportService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/reports")
public class AgentReportController {

    private final AgentReportService agentReportService;

    public AgentReportController(AgentReportService agentReportService) {
        this.agentReportService = agentReportService;
    }

    @GetMapping("/agent/{agentId}")
    public ResponseEntity<AgentReportResponse> getAgentReport(@PathVariable String agentId) {
        AgentReportResponse response = agentReportService.getAgentReport(agentId);
        if (!response.isFound()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
        }
        return ResponseEntity.ok(response);
    }
}
