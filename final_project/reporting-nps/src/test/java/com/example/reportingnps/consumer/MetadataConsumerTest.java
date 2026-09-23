package com.example.reportingnps.consumer;

import com.example.reportingnps.entity.CallMetadata;
import com.example.reportingnps.repository.CallMetadataRepository;
import com.example.reportingnps.service.ReportService;
import com.example.reportingnps.service.SentimentService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@EmbeddedKafka(partitions = 1, topics = {"calls.metadata"})
class MetadataConsumerTest {

    @Mock
    private CallMetadataRepository callMetadataRepository;

    @Mock
    private ReportService reportService;

    @Mock
    private SentimentService sentimentService;

    @InjectMocks
    private MetadataConsumer metadataConsumer;

    private UUID testCallId;

    @BeforeEach
    void setUp() {
        testCallId = UUID.randomUUID();
    }

    @Test
    void shouldConsumeAndSaveMetadata() {
        // Given
        Map<String, Object> event = Map.of(
                "callId", testCallId.toString(),
                "customerPhone", "+1234567890",
                "agentId", "agent-001",
                "callStartTime", LocalDateTime.now().toString(),
                "callStatus", "COMPLETED",
                "sentiment", "POSITIVE"
        );

        when(callMetadataRepository.save(any(CallMetadata.class))).thenReturn(new CallMetadata());

        // When
        metadataConsumer.consume(event, null);

        // Then
        ArgumentCaptor<CallMetadata> captor = ArgumentCaptor.forClass(CallMetadata.class);
        verify(callMetadataRepository, times(1)).save(captor.capture());

        CallMetadata saved = captor.getValue();
        assertThat(saved.getCallId()).isEqualTo(testCallId);
        assertThat(saved.getCustomerPhone()).isEqualTo("+1234567890");
        assertThat(saved.getAgentId()).isEqualTo("agent-001");
        assertThat(saved.getCallStatus()).isEqualTo("COMPLETED");
        assertThat(saved.getSentiment()).isEqualTo("POSITIVE");
    }

    @Test
    void shouldHandleEmptyFields() {
        // Given
        Map<String, Object> event = Map.of(
                "callId", testCallId.toString(),
                "agentId", "agent-001"
        );

        when(callMetadataRepository.save(any(CallMetadata.class))).thenReturn(new CallMetadata());

        // When
        metadataConsumer.consume(event, null);

        // Then
        ArgumentCaptor<CallMetadata> captor = ArgumentCaptor.forClass(CallMetadata.class);
        verify(callMetadataRepository, times(1)).save(captor.capture());

        CallMetadata saved = captor.getValue();
        assertThat(saved.getCallId()).isEqualTo(testCallId);
        assertThat(saved.getAgentId()).isEqualTo("agent-001");
    }
}
