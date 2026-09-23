package com.example.transcription.analyzer.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.record.KafkaRecordMetadata;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TranscriptionProducerTest {

    @Mock
    private KafkaTemplate<String, com.example.transcription.avro.RawTranscription> kafkaTemplate;

    private TranscriptionProducer producer;

    @BeforeEach
    void setUp() {
        List<String> templates = List.of(
                "I am calling regarding {topic}",
                "Could you please provide more details about {topic}",
                "Let me check that information for you",
                "I understand your concern about {topic}",
                "Is there anything else I can help you with"
        );
        producer = new TranscriptionProducer(kafkaTemplate, templates, 150);
    }

    @Test
    void testProduceGeneratesText() {
        String callId = "test-call-1";
        double durationMinutes = 2.0;

        when(kafkaTemplate.send(eq("transcription.raw"), eq(callId), any()))
                .thenReturn(CompletableFuture.completedFuture(mockResult()));

        com.example.transcription.avro.RawTranscription record = producer.produce(callId, durationMinutes);

        assertNotNull(record);
        assertEquals(callId, record.getCallId().toString());
        assertNotNull(record.getText());
        assertTrue(record.getText().toString().length() > 0);
        // ~150 words per minute * 2 minutes = ~300 words
        int wordCount = record.getText().toString().split("\\s+").length;
        assertTrue(wordCount >= 200 && wordCount <= 400, "Word count should be around 300, was: " + wordCount);
    }

    @Test
    void testProduceGeneratesQuality() {
        String callId = "test-call-2";
        double durationMinutes = 1.0;

        when(kafkaTemplate.send(eq("transcription.raw"), eq(callId), any()))
                .thenReturn(CompletableFuture.completedFuture(mockResult()));

        com.example.transcription.avro.RawTranscription record = producer.produce(callId, durationMinutes);

        assertNotNull(record.getQuality());
        assertTrue(record.getQuality() >= 0.0f && record.getQuality() <= 1.0f);
    }

    @Test
    void testProduceGeneratesLanguage() {
        String callId = "test-call-3";
        double durationMinutes = 1.0;

        when(kafkaTemplate.send(eq("transcription.raw"), eq(callId), any()))
                .thenReturn(CompletableFuture.completedFuture(mockResult()));

        com.example.transcription.avro.RawTranscription record = producer.produce(callId, durationMinutes);

        assertNotNull(record.getLanguage());
        String language = record.getLanguage().toString();
        assertTrue("ru".equals(language) || "en".equals(language));
    }

    @Test
    void testProduceSendsToCorrectTopic() {
        String callId = "test-call-4";
        double durationMinutes = 1.0;

        when(kafkaTemplate.send(eq("transcription.raw"), eq(callId), any()))
                .thenReturn(CompletableFuture.completedFuture(mockResult()));

        producer.produce(callId, durationMinutes);

        verify(kafkaTemplate).send("transcription.raw", callId, any());
    }

    @Test
    void testProduceWithShortDuration() {
        String callId = "test-call-5";
        double durationMinutes = 0.1; // Very short call

        when(kafkaTemplate.send(eq("transcription.raw"), eq(callId), any()))
                .thenReturn(CompletableFuture.completedFuture(mockResult()));

        com.example.transcription.avro.RawTranscription record = producer.produce(callId, durationMinutes);

        assertNotNull(record);
        assertTrue(record.getText().toString().length() >= 10); // Minimum 10 words
    }

    private SendResult<String, com.example.transcription.avro.RawTranscription> mockResult() {
        KafkaRecordMetadata<Void> metadata = new KafkaRecordMetadata<>("topic", 0, 0, 0, 0);
        return new SendResult<>(null, metadata);
    }
}
