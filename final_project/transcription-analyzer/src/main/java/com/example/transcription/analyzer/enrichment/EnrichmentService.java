package com.example.transcription.analyzer.enrichment;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class EnrichmentService {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentService.class);

    private final CustomerProfileManager customerProfileManager;

    public EnrichmentService(CustomerProfileManager customerProfileManager) {
        this.customerProfileManager = customerProfileManager;
    }

    /**
     * Enrich transcription summary with customer profile data.
     * Uses broadcast enrichment: lookup customer by phone derived from callId.
     */
    public Map<String, Object> enrich(Map<String, Object> summary, String transcriptionText, String phone) {
        Map<String, String> profile = customerProfileManager.lookup(phone);

        String segment;
        String riskLevel;

        if (profile != null) {
            segment = profile.getOrDefault("segment", "STANDARD");
            riskLevel = profile.getOrDefault("riskLevel", "LOW");
        } else {
            // Default values for missing customer profile
            segment = "STANDARD";
            riskLevel = "LOW";
            log.debug("No customer profile found for phone={}, using defaults", phone);
        }

        String priority = calculatePriority(segment, riskLevel);

        // Use LinkedHashMap to preserve field order and ensure all fields are set
        Map<String, Object> enriched = new java.util.LinkedHashMap<>();
        enriched.put("callId", summary.get("callId"));
        enriched.put("transcriptionText", transcriptionText);
        enriched.put("sentiment", summary.get("sentiment"));
        enriched.put("urgency", summary.get("urgency"));
        enriched.put("problem", summary.get("problem"));
        enriched.put("solution", summary.get("solution"));
        enriched.put("confidence", summary.get("confidence"));
        enriched.put("segment", segment);
        enriched.put("riskLevel", riskLevel);
        enriched.put("priority", priority);

        log.debug("Enriched transcription: callId={}, segment={}, riskLevel={}, priority={}",
                summary.get("callId"), segment, riskLevel, priority);

        return enriched;
    }

    /**
     * Calculate priority based on segment + riskLevel per the priority matrix.
     *
     * | segment   | risk_level → | low     | medium | high     |
     * |-----------|-------------|---------|--------|----------|
     * | premium   |             | high    | high   | critical |
     * | standard  |             | normal  | normal | high     |
     * | corporate |             | normal  | high   | critical |
     */
    private String calculatePriority(String segment, String riskLevel) {
        return switch (segment.toUpperCase()) {
            case "PREMIUM" -> switch (riskLevel.toUpperCase()) {
                case "LOW" -> "high";
                case "MEDIUM" -> "high";
                case "HIGH" -> "critical";
                default -> "normal";
            };
            case "STANDARD" -> switch (riskLevel.toUpperCase()) {
                case "LOW" -> "normal";
                case "MEDIUM" -> "normal";
                case "HIGH" -> "high";
                default -> "normal";
            };
            case "CORPORATE" -> switch (riskLevel.toUpperCase()) {
                case "LOW" -> "normal";
                case "MEDIUM" -> "high";
                case "HIGH" -> "critical";
                default -> "normal";
            };
            default -> "normal";
        };
    }
}
