package com.example.reportingnps.consumer;

import com.example.reportingnps.service.ReportService;
import com.example.reportingnps.service.SentimentService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EnrichedTranscriptionConsumerTest {

    @Mock
    private ReportService reportService;

    @Mock
    private SentimentService sentimentService;

    @Mock
    private Acknowledgment ack;

    @InjectMocks
    private EnrichedTranscriptionConsumer consumer;

    @Test
    void shouldInvalidateCachesOnValidEvent() {
        // Given
        String callId = "550e8400-e29b-41d4-a716-446655440000";
        String jsonEvent = String.format(
                "{\"payload\":{\"callId\":\"%s\",\"transcriptionText\":\"Hello\",\"sentiment\":\"POSITIVE\"}}",
                callId
        );

        // When
        consumer.consume(jsonEvent, ack);

        // Then
        verify(reportService, times(1)).evictMetadataCache(callId);
        verify(reportService, times(1)).evictReportCache();
        verify(sentimentService, times(1)).evictSentimentCache();
        verify(ack, times(1)).acknowledge();
    }

    @Test
    void shouldSkipEventWithoutCallId() {
        // Given
        String jsonEvent = "{\"payload\":{\"transcriptionText\":\"Hello\"}}";

        // When
        consumer.consume(jsonEvent, ack);

        // Then
        verify(reportService, never()).evictMetadataCache(anyString());
        verify(ack, times(1)).acknowledge();
    }

    @Test
    void shouldSkipEventWithInvalidCallId() {
        // Given
        String jsonEvent = "{\"payload\":{\"callId\":\"not-a-uuid\",\"transcriptionText\":\"Hello\"}}";

        // When
        consumer.consume(jsonEvent, ack);

        // Then
        verify(reportService, never()).evictMetadataCache(anyString());
        verify(ack, times(1)).acknowledge();
    }

    @Test
    void shouldHandlePlainJsonWithoutPayloadWrapper() {
        // Given
        String callId = "550e8400-e29b-41d4-a716-446655440001";
        String jsonEvent = String.format(
                "{\"callId\":\"%s\",\"sentiment\":\"NEGATIVE\"}",
                callId
        );

        // When
        consumer.consume(jsonEvent, ack);

        // Then
        verify(reportService, times(1)).evictMetadataCache(callId);
        verify(reportService, times(1)).evictReportCache();
        verify(sentimentService, times(1)).evictSentimentCache();
        verify(ack, times(1)).acknowledge();
    }
}
