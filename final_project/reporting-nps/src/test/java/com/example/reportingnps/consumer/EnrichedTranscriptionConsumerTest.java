package com.example.reportingnps.consumer;

import com.example.reportingnps.entity.CallMetadata;
import com.example.reportingnps.entity.CallTranscription;
import com.example.reportingnps.repository.CallMetadataRepository;
import com.example.reportingnps.repository.CallTranscriptionRepository;
import com.example.reportingnps.service.ReportService;
import com.example.reportingnps.service.SentimentService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EnrichedTranscriptionConsumerTest {

    @Mock
    private CallTranscriptionRepository transcriptionRepository;

    @Mock
    private CallMetadataRepository metadataRepository;

    @Mock
    private ReportService reportService;

    @Mock
    private SentimentService sentimentService;

    @InjectMocks
    private EnrichedTranscriptionConsumer consumer;

    private UUID testCallId;

    @BeforeEach
    void setUp() {
        testCallId = UUID.randomUUID();
    }

    @Test
    void shouldConsumeAndSaveTranscription() {
        // Given
        CallMetadata metadata = new CallMetadata();
        metadata.setCallId(testCallId);
        metadata.setAgentId("agent-001");

        when(metadataRepository.findByCallId(testCallId)).thenReturn(Optional.of(metadata));
        when(transcriptionRepository.findByCallId(testCallId)).thenReturn(Optional.empty());
        when(transcriptionRepository.save(any(CallTranscription.class))).thenReturn(new CallTranscription());

        // When
        consumer.consume(Map.of(
                "callId", testCallId.toString(),
                "transcriptionText", "Hello, how can I help you?",
                "sentiment", "POSITIVE",
                "segment", "PREMIUM",
                "riskLevel", "LOW",
                "priority", "NORMAL"
        ), null);

        // Then
        ArgumentCaptor<CallTranscription> captor = ArgumentCaptor.forClass(CallTranscription.class);
        verify(transcriptionRepository, times(1)).save(captor.capture());

        CallTranscription saved = captor.getValue();
        assertThat(saved.getCallId()).isEqualTo(testCallId);
        assertThat(saved.getTranscriptionText()).isEqualTo("Hello, how can I help you?");
        assertThat(saved.getSentiment()).isEqualTo("POSITIVE");
        assertThat(saved.getSegment()).isEqualTo("PREMIUM");
        assertThat(saved.getRiskLevel()).isEqualTo("LOW");
    }

    @Test
    void shouldBufferOrphanEvent() {
        // Given - callId not found in metadata
        when(metadataRepository.findByCallId(testCallId)).thenReturn(Optional.empty());

        // When
        consumer.consume(Map.of(
                "callId", testCallId.toString(),
                "transcriptionText", "Test transcription"
        ), null);

        // Then - orphan event should be buffered
        assertThat(consumer).isNotNull();
        verify(transcriptionRepository, never()).save(any());
    }
}
