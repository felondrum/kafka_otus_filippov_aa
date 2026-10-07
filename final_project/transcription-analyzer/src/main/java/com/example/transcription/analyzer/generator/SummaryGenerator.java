package com.example.transcription.analyzer.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class SummaryGenerator {

    private static final Logger log = LoggerFactory.getLogger(SummaryGenerator.class);

    private final List<String> keywords;

    public SummaryGenerator(@Value("${transcription.keywords:card,loan,fraud,complaint,transfer,block,limit,payment,balance}") String keywordsStr) {
        this.keywords = Arrays.stream(keywordsStr.split(","))
                .map(String::trim)
                .map(String::toLowerCase)
                .collect(Collectors.toList());
        log.info("Loaded {} keywords: {}", keywords.size(), keywords);
    }

    /**
     * Generate a synthetic summary from transcription text using keyword matching.
     * No real LLM - purely heuristic-based extraction.
     */
    public SummaryResult generate(String callId, String transcriptionText) {
        String lowerText = transcriptionText.toLowerCase();
        List<String> matchedKeywords = new ArrayList<>();

        for (String keyword : keywords) {
            if (lowerText.contains(keyword.toLowerCase())) {
                matchedKeywords.add(keyword);
            }
        }

        String problem = categorizeProblem(lowerText, matchedKeywords);
        String solution = categorizeSolution(lowerText, matchedKeywords, problem);
        String sentiment = analyzeSentiment(lowerText, matchedKeywords);
        String urgency = detectUrgency(lowerText, matchedKeywords);
        float confidence = calculateConfidence(matchedKeywords, problem);

        log.debug("Generated summary for callId={}: problem={}, solution={}, sentiment={}, urgency={}, confidence={}",
                callId, problem, solution, sentiment, urgency, confidence);

        return new SummaryResult(callId, problem, solution, sentiment, urgency, confidence);
    }

    /**
     * Categorize problem based on keyword matching.
     */
    private String categorizeProblem(String lowerText, List<String> matchedKeywords) {
        if (matchedKeywords.isEmpty()) {
            return "other";
        }

        // fraud_suspected: fraud, unauthorized
        if (lowerText.contains("fraud") || lowerText.contains("unauthorized") ||
                lowerText.contains("fraudulent") || lowerText.contains("suspicious")) {
            return "fraud_suspected";
        }

        // card_loss: card, lost, block
        if (lowerText.contains("lost") || lowerText.contains("stolen") ||
                (lowerText.contains("card") && (lowerText.contains("lost") || lowerText.contains("missing")))) {
            return "card_loss";
        }

        // complaint: complaint, complain
        if (lowerText.contains("complaint") || lowerText.contains("complain")) {
            return "complaint";
        }

        // credit_inquiry: loan, credit, limit
        if (lowerText.contains("loan") || lowerText.contains("credit") || lowerText.contains("limit")) {
            return "credit_inquiry";
        }

        // card-related
        if (lowerText.contains("card")) {
            return "card_loss";
        }

        return "other";
    }

    /**
     * Categorize solution based on context and problem.
     */
    private String categorizeSolution(String lowerText, List<String> matchedKeywords, String problem) {
        if (lowerText.contains("blocked") || lowerText.contains("block") || lowerText.contains("freeze")) {
            return "card_blocked";
        }

        if (lowerText.contains("escalat") || lowerText.contains("supervisor") || lowerText.contains("manager")) {
            return "escalation_created";
        }

        if (lowerText.contains("transfer") || lowerText.contains("specialist") || lowerText.contains("agent")) {
            return "transfer_to_specialist";
        }

        if (lowerText.contains("info") || lowerText.contains("provided") || lowerText.contains("explained") ||
                lowerText.contains("resolved") || lowerText.contains("fixed")) {
            return "info_provided";
        }

        if (problem.equals("fraud_suspected")) {
            return "card_blocked";
        }

        return "resolved_on_call";
    }

    /**
     * Analyze sentiment based on keyword matching.
     */
    private String analyzeSentiment(String lowerText, List<String> matchedKeywords) {
        // Negative indicators
        String[] negativeWords = {"angry", "frustrated", "unhappy", "disappointed", "bad", "terrible",
                "worst", "hate", "complaint", "complain", "fraud", "unauthorized", "stolen", "lost",
                "angry", "issue", "problem", "error", "fail"};
        int negativeCount = 0;
        for (String word : negativeWords) {
            if (lowerText.contains(word)) negativeCount++;
        }

        // Positive indicators
        String[] positiveWords = {"happy", "satisfied", "great", "excellent", "good", "thank", "thanks",
                "appreciate", "helpful", "resolved", "fixed", "perfect", "pleased"};
        int positiveCount = 0;
        for (String word : positiveWords) {
            if (lowerText.contains(word)) positiveCount++;
        }

        if (negativeCount > positiveCount) {
            return "negative";
        }
        if (positiveCount > negativeCount) {
            return "positive";
        }
        return "neutral";
    }

    /**
     * Detect urgency level based on keywords.
     */
    private String detectUrgency(String lowerText, List<String> matchedKeywords) {
        // Critical: fraud, unauthorized
        if (lowerText.contains("fraud") || lowerText.contains("unauthorized") ||
                lowerText.contains("stolen") || lowerText.contains("emergency")) {
            return "critical";
        }

        // High: complaint, escalation
        if (lowerText.contains("complaint") || lowerText.contains("escalat") ||
                lowerText.contains("supervisor") || lowerText.contains("manager")) {
            return "high";
        }

        // Medium: block, lost, limit
        if (lowerText.contains("block") || lowerText.contains("lost") ||
                lowerText.contains("limit") || lowerText.contains("transfer")) {
            return "medium";
        }

        // Low: everything else
        return "low";
    }

    /**
     * Calculate confidence score based on keyword match quality.
     * Formula: (matched_keywords / total_keywords_in_dictionary) × 0.5 + (1 if exact category match else 0) × 0.5
     */
    private float calculateConfidence(List<String> matchedKeywords, String problem) {
        if (matchedKeywords.isEmpty()) {
            return 0.0f;
        }

        float keywordRatio = (float) matchedKeywords.size() / keywords.size();
        int categoryMatch = problem.equals("other") ? 0 : 1;

        float confidence = keywordRatio * 0.5f + categoryMatch * 0.5f;

        // Clamp to [0.0, 1.0]
        return Math.max(0.0f, Math.min(1.0f, confidence));
    }

    /**
     * Result of summary generation.
     */
    public static class SummaryResult {
        private final String callId;
        private final String problem;
        private final String solution;
        private final String sentiment;
        private final String urgency;
        private final float confidence;

        public SummaryResult(String callId, String problem, String solution,
                             String sentiment, String urgency, float confidence) {
            this.callId = callId;
            this.problem = problem;
            this.solution = solution;
            this.sentiment = sentiment;
            this.urgency = urgency;
            this.confidence = confidence;
        }

        public String getCallId() { return callId; }
        public String getProblem() { return problem; }
        public String getSolution() { return solution; }
        public String getSentiment() { return sentiment; }
        public String getUrgency() { return urgency; }
        public float getConfidence() { return confidence; }
    }
}
