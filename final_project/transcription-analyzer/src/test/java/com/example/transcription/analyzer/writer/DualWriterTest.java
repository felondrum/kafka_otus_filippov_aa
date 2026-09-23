package com.example.transcription.analyzer.writer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;
import org.apache.kafka.record.KafkaRecordMetadata;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DualWriterTest {

    @Mock
    private KafkaTemplate<String, com.example.transcription.avro.EnrichedTranscription> kafkaTemplate;

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Test
    void testWriteToBothKafkaAndPostgreSQL() {
        com.example.transcription.avro.EnrichedTranscription enriched =
                new com.example.transcription.avro.EnrichedTranscription();
        enriched.setCallId("call-1");
        enriched.setTranscriptionText("test text");
        enriched.setSentiment("neutral");
        enriched.setUrgency("low");
        enriched.setProblem("other");
        enriched.setSolution("resolved_on_call");
        enriched.setConfidence(0.0f);
        enriched.setSegment("STANDARD");
        enriched.setRiskLevel("LOW");
        enriched.setPriority("normal");

        when(kafkaTemplate.send(eq("transcription.enriched"), eq("call-1"), any()))
                .thenReturn(CompletableFuture.completedFuture(mockResult()));
        doNothing().when(jdbcTemplate).update(anyString(), any(), any(), any(), any(), any(), any());

        DualWriter dualWriter = new DualWriter(kafkaTemplate, jdbcTemplate);
        dualWriter.write(enriched);

        verify(kafkaTemplate).send("transcription.enriched", "call-1", enriched);
        verify(jdbcTemplate).update(
                anyString(), eq("call-1"), eq("test text"), eq("neutral"),
                eq("low"), eq("other"), eq("resolved_on_call"), eq(0.0f));
    }

    @Test
    void testWriteFailsWhenKafkaFails() {
        com.example.transcription.avro.EnrichedTranscription enriched =
                new com.example.transcription.avro.EnrichedTranscription();
        enriched.setCallId("call-2");

        when(kafkaTemplate.send(eq("transcription.enriched"), eq("call-2"), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Kafka down")));

        DualWriter dualWriter = new DualWriter(kafkaTemplate, jdbcTemplate);

        assertThrows(RuntimeException.class, () -> dualWriter.write(enriched));
        verify(jdbcTemplate, never()).update(anyString(), any());
    }

    private SendResult<String, com.example.transcription.avro.EnrichedTranscription> mockResult() {
        KafkaRecordMetadata<Void> metadata = new KafkaRecordMetadata<>("topic", 0, 0, 0, 0);
        return new SendResult<>(null, metadata);
    }
}
