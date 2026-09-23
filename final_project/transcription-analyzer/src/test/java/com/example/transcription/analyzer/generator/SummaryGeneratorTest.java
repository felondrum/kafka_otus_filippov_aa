package com.example.transcription.analyzer.generator;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SummaryGeneratorTest {

    private final SummaryGenerator generator = new SummaryGenerator(
            List.of("card", "loan", "fraud", "complaint", "transfer", "block", "limit", "payment", "balance"));

    @Test
    void testFraudDetection() {
        SummaryGenerator.SummaryResult result = generator.generate("call-1", "I suspect fraud on my account");

        assertEquals("fraud_suspected", result.getProblem());
        assertEquals("critical", result.getUrgency());
        assertEquals("negative", result.getSentiment());
        assertTrue(result.getConfidence() > 0.0);
    }

    @Test
    void testCardLossDetection() {
        SummaryGenerator.SummaryResult result = generator.generate("call-2", "I lost my card");

        assertEquals("card_loss", result.getProblem());
        assertTrue(List.of("medium", "high", "critical").contains(result.getUrgency()));
    }

    @Test
    void testCreditInquiry() {
        SummaryGenerator.SummaryResult result = generator.generate("call-3", "I want to apply for a loan");

        assertEquals("credit_inquiry", result.getProblem());
        assertEquals("low", result.getUrgency());
    }

    @Test
    void testComplaint() {
        SummaryGenerator.SummaryResult result = generator.generate("call-4", "I have a complaint about your service");

        assertEquals("complaint", result.getProblem());
        assertEquals("high", result.getUrgency());
        assertEquals("negative", result.getSentiment());
    }

    @Test
    void testDefaultValues() {
        SummaryGenerator.SummaryResult result = generator.generate("call-5", "Hello, just checking the weather");

        assertEquals("other", result.getProblem());
        assertEquals("resolved_on_call", result.getSolution());
        assertEquals("neutral", result.getSentiment());
        assertEquals("low", result.getUrgency());
        assertEquals(0.0f, result.getConfidence());
    }

    @Test
    void testSentimentPositive() {
        SummaryGenerator.SummaryResult result = generator.generate("call-6", "Thank you for your helpful service, I am satisfied");

        assertEquals("positive", result.getSentiment());
    }

    @Test
    void testSentimentNegative() {
        SummaryGenerator.SummaryResult result = generator.generate("call-7", "I am angry and frustrated with this terrible service");

        assertEquals("negative", result.getSentiment());
    }

    @Test
    void testConfidenceScore() {
        // High keyword match should have higher confidence
        SummaryGenerator.SummaryResult result1 = generator.generate("call-8", "I have fraud and unauthorized transactions on my card");
        SummaryGenerator.SummaryResult result2 = generator.generate("call-9", "I have fraud on my account");

        assertTrue(result1.getConfidence() >= result2.getConfidence());
    }

    @Test
    void testCaseInsensitiveMatching() {
        SummaryGenerator.SummaryResult result = generator.generate("call-10", "I want to check my BALANCE and make a PAYMENT");

        assertNotNull(result);
        assertTrue(result.getConfidence() > 0.0);
    }

    @Test
    void testSolutionCategorization() {
        SummaryGenerator.SummaryResult result1 = generator.generate("call-11", "Please block my card");
        assertEquals("card_blocked", result1.getSolution());

        SummaryGenerator.SummaryResult result2 = generator.generate("call-12", "I need to escalate to a supervisor");
        assertEquals("escalation_created", result2.getSolution());

        SummaryGenerator.SummaryResult result3 = generator.generate("call-13", "Please transfer me to a specialist");
        assertEquals("transfer_to_specialist", result3.getSolution());
    }
}
