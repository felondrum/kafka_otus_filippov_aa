package com.example.transcription.analyzer.writer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DualWriterLogicTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @InjectMocks
    private DualWriter dualWriter;

    @Test
    void shouldWriteToKafkaOnly() {
        // Given
        String callId = UUID.randomUUID().toString();
        Map<String, Object> enriched = new LinkedHashMap<>();
        enriched.put("callId", callId);
        enriched.put("transcriptionText", "Test transcription");
        enriched.put("sentiment", "POSITIVE");
        enriched.put("urgency", "high");
        enriched.put("problem", "card_loss");
        enriched.put("solution", "card_blocked");
        enriched.put("confidence", 0.95);
        enriched.put("segment", "PREMIUM");
        enriched.put("riskLevel", "MEDIUM");
        enriched.put("priority", "HIGH");

        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(
                new SendResult<>(null, null));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(future);

        // When
        dualWriter.write(enriched);

        // Then
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(1)).send(topicCaptor.capture(), keyCaptor.capture(), valueCaptor.capture());

        assertThat(topicCaptor.getValue()).isEqualTo("transcription.enriched");
        assertThat(keyCaptor.getValue()).isEqualTo(callId);
        assertThat(valueCaptor.getValue()).contains("\"schema\"");
        assertThat(valueCaptor.getValue()).contains("\"payload\"");
        assertThat(valueCaptor.getValue()).contains("call_id");
        assertThat(valueCaptor.getValue()).contains("sentiment");
    }

    @Test
    void shouldGenerateNewUuidForInvalidCallId() {
        // Given
        String invalidCallId = "not-a-uuid";
        Map<String, Object> enriched = new LinkedHashMap<>();
        enriched.put("callId", invalidCallId);
        enriched.put("transcriptionText", "Test");
        enriched.put("sentiment", "NEUTRAL");
        enriched.put("urgency", "low");
        enriched.put("problem", "other");
        enriched.put("solution", "resolved_on_call");
        enriched.put("confidence", 0.5);
        enriched.put("segment", "STANDARD");
        enriched.put("riskLevel", "LOW");
        enriched.put("priority", "NORMAL");

        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(
                new SendResult<>(null, null));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(future);

        // When — should not throw, should generate valid UUID
        dualWriter.write(enriched);

        // Then
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(1)).send(anyString(), keyCaptor.capture(), anyString());
        // Generated UUID should be valid
        assertThat(keyCaptor.getValue()).isNotNull();
        assertThat(UUID.fromString(keyCaptor.getValue())).isNotNull();
    }

    @Test
    void shouldSendToDLQOnKafkaFailure() {
        // Given
        String callId = UUID.randomUUID().toString();
        Map<String, Object> enriched = new LinkedHashMap<>();
        enriched.put("callId", callId);
        enriched.put("transcriptionText", "Test");
        enriched.put("sentiment", "NEUTRAL");
        enriched.put("urgency", "low");
        enriched.put("problem", "other");
        enriched.put("solution", "resolved_on_call");
        enriched.put("confidence", 0.5);
        enriched.put("segment", "STANDARD");
        enriched.put("riskLevel", "LOW");
        enriched.put("priority", "NORMAL");

        CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("Kafka unavailable"));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(future);

        // When/Then — should throw RuntimeException
        assertThatThrownBy(() -> dualWriter.write(enriched))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Kafka write failed");

        // Verify DLQ send was attempted
        verify(kafkaTemplate, times(1)).send("transcription.enriched.dlq", anyString(), anyString());
    }
}
