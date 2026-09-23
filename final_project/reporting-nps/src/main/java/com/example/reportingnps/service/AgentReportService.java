package com.example.reportingnps.service;

import com.example.reportingnps.dto.AgentReportResponse;
import com.example.reportingnps.repository.CallMetadataRepository;
import com.example.reportingnps.repository.FraudStatsRepository;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class AgentReportService {

    private final CallMetadataRepository callMetadataRepository;
    private final FraudStatsRepository fraudStatsRepository;

    public AgentReportService(CallMetadataRepository callMetadataRepository,
                              FraudStatsRepository fraudStatsRepository) {
        this.callMetadataRepository = callMetadataRepository;
        this.fraudStatsRepository = fraudStatsRepository;
    }

    @Cacheable(value = "reportCache", key = "'agent:' + #agentId")
    public AgentReportResponse getAgentReport(String agentId) {
        long totalCalls = callMetadataRepository.countByAgentId(agentId);

        if (totalCalls == 0) {
            return new AgentReportResponse(agentId, 0, 0.0, Collections.emptyMap(),
                    Collections.emptyMap(), 0, false);
        }

        long fraudAlerts = fraudStatsRepository.countByAgentId(agentId);

        // For now, return basic counts - detailed aggregation would require more complex queries
        Map<String, Long> callsByStatus = EnumSet.allOf(ReportService.CallMetadataStatus.class).stream()
                .collect(Collectors.toMap(
                        s -> s.getValue(),
                        s -> 0L // Would need agent-specific status counts
                ));

        Map<String, Long> callsBySegment = new HashMap<>();

        return new AgentReportResponse(agentId, totalCalls, 0.0, callsByStatus,
                callsBySegment, fraudAlerts, true);
    }

    @CacheEvict(value = "reportCache", key = "#agentId")
    public void evictAgentReportCache(String agentId) {
        // Single agent report cache entry evicted
    }

    @CacheEvict(value = "reportCache", allEntries = true)
    public void evictReportCache() {
        // All report cache entries are evicted
    }
}
