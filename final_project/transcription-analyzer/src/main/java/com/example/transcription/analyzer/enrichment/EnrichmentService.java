package com.example.transcription.analyzer.enrichment;

import com.example.transcription.analyzer.enrichment.CustomerProfileManager;
import com.example.transcription.avro.CustomerProfile;
import com.example.transcription.avro.EnrichedTranscription;
import com.example.transcription.avro.TranscriptionSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

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
    public EnrichedTranscription enrich(TranscriptionSummary summary, String transcriptionText, String phone) {
        CustomerProfile profile = customerProfileManager.lookup(phone);

        String segment;
        String riskLevel;

        if (profile != null) {
            segment = profile.getSegment().toString();
            riskLevel = profile.getRiskLevel().toString();
        } else {
            // Default values for missing customer profile
            segment = "STANDARD";
            riskLevel = "LOW";
            log.debug("No customer profile found for phone={}, using defaults", phone);
        }

        String priority = calculatePriority(segment, riskLevel);

        EnrichedTranscription enriched = new EnrichedTranscription(
                summary.getCallId(),
                transcriptionText,
                summary.getSentiment(),
                summary.getUrgency(),
                summary.getProblem(),
                summary.getSolution(),
                summary.getConfidence(),
                segment,
                riskLevel,
                priority
        );

        log.debug("Enriched transcription: callId={}, segment={}, riskLevel={}, priority={}",
                summary.getCallId(), segment, riskLevel, priority);

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
