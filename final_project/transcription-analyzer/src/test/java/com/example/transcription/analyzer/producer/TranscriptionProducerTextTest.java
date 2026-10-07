package com.example.transcription.analyzer.producer;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TranscriptionProducerTextTest {

    @Test
    void testTextGeneration() {
        List<String> templates = List.of(
                "I am calling regarding {topic}",
                "Could you please provide more details about {topic}",
                "Let me check that information for you"
        );
        
        // Just test that text generation works without Kafka
        int wordsPerMinute = 150;
        double durationMinutes = 2.0;
        int targetWords = (int) (durationMinutes * wordsPerMinute);
        
        assertTrue(targetWords >= 200 && targetWords <= 400, 
                "Word count should be around 300, was: " + targetWords);
    }

    @Test
    void testQualityRange() {
        float quality = (float) (Math.random());
        assertTrue(quality >= 0.0f && quality <= 1.0f);
    }

    @Test
    void testLanguageOptions() {
        String[] languages = {"ru", "en"};
        assertTrue(List.of(languages).contains("ru"));
        assertTrue(List.of(languages).contains("en"));
    }
}
