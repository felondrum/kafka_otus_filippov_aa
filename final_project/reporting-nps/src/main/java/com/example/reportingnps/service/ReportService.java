package com.example.reportingnps.service;

import com.example.reportingnps.dto.DailyReportResponse;
import com.example.reportingnps.repository.CallMetadataRepository;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class ReportService {

    private final CallMetadataRepository callMetadataRepository;

    public ReportService(CallMetadataRepository callMetadataRepository) {
        this.callMetadataRepository = callMetadataRepository;
    }

    @Cacheable(value = "reportCache", key = "'daily:' + #from + ':' + #to")
    public DailyReportResponse getDailyReport(LocalDateTime from, LocalDateTime to) {
        long totalCalls = callMetadataRepository.countByCallStartTimeBetween(from, to);
        Double avgSentiment = callMetadataRepository.avgSentimentScoreBetween(from, to);
        double averageNps = avgSentiment != null ? avgSentiment.doubleValue() : 0.0;

        // Calls by status
        Map<String, Long> callsByStatus = EnumSet.allOf(CallMetadataStatus.class).stream()
                .collect(Collectors.toMap(
                        s -> s.getValue(),
                        s -> callMetadataRepository.countByCallStatus(s.getValue())
                ));

        // Calls by segment
        Map<String, Long> callsBySegment = new HashMap<>();
        callMetadataRepository.countBySegmentBetween(from, to).forEach(row -> {
            callsBySegment.put((String) row[0], (Long) row[1]);
        });

        // Calls by agent
        Map<String, Long> callsByAgent = new HashMap<>();
        callMetadataRepository.countByAgentIdBetween(from, to).forEach(row -> {
            callsByAgent.put((String) row[0], (Long) row[1]);
        });

        return new DailyReportResponse(totalCalls, averageNps, callsByStatus, callsBySegment, callsByAgent);
    }

    @CacheEvict(value = "reportCache", allEntries = true)
    public void evictReportCache() {
        // All report cache entries are evicted
    }

    @CacheEvict(value = "metadataCache", key = "#callId")
    public void evictMetadataCache(String callId) {
        // Single metadata cache entry evicted
    }

    @CacheEvict(value = "sentimentCache", allEntries = true)
    public void evictSentimentCache() {
        // All sentiment cache entries are evicted
    }

    public enum CallMetadataStatus {
        INITIATED("INITIATED"),
        IN_PROGRESS("IN_PROGRESS"),
        COMPLETED("COMPLETED"),
        CANCELLED("CANCELLED"),
        FAILED("FAILED");

        private final String value;

        CallMetadataStatus(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
