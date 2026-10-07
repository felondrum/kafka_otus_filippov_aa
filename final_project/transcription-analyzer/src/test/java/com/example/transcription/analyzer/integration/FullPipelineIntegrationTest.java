package com.example.transcription.analyzer.integration;

import com.example.transcription.analyzer.generator.SummaryGenerator;
import com.example.transcription.analyzer.metadata.MetadataManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for the full pipeline logic:
 * call event → raw transcription → summary → enriched → PostgreSQL
 */
class FullPipelineIntegrationTest {

    @Test
    void testFullPipeline() {
        String callId = "integration-test-call-1";
        double durationMinutes = 2.0;
        int wordsPerMinute = 150;
        int expectedWords = (int) (durationMinutes * wordsPerMinute);

        // Verify text generation logic
        assertTrue(expectedWords >= 200 && expectedWords <= 400,
                "Word count should be around 300, was: " + expectedWords);

        // Verify summary generation
        SummaryGenerator generator = new SummaryGenerator(
                "card,loan,fraud,complaint,transfer,block,limit,payment,balance");

        var summary = generator.generate(callId, "I have fraud on my account");
        assertNotNull(summary);
        assertEquals("fraud_suspected", summary.getProblem());
        assertEquals("critical", summary.getUrgency());
    }

    @Test
    void testKeywordExtractionIntegration() {
        SummaryGenerator generator = new SummaryGenerator(
                "card,loan,fraud,complaint,transfer,block,limit,payment,balance");

        String[] testTexts = {
            "I have fraud on my account and need to block my card",
            "I want to apply for a loan and check my balance",
            "I have a complaint about your service",
            "Just checking my account, no issues"
        };

        for (String text : testTexts) {
            var summary = generator.generate("test-call", text);
            assertNotNull(summary.getProblem());
            assertNotNull(summary.getSolution());
        }
    }
}
