package com.example.reportingnps.consumer;

import com.example.reportingnps.entity.CallMetadata;
import com.example.reportingnps.repository.CallMetadataRepository;
import com.example.reportingnps.service.ReportService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MetadataConsumerTest {

    @Mock
    private CallMetadataRepository callMetadataRepository;

    @Mock
    private ReportService reportService;

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
        String jsonEvent = String.format(
                "{\"callId\":\"%s\",\"status\":\"COMPLETED\"}",
                testCallId
        );

        CallMetadata existingMetadata = new CallMetadata();
        existingMetadata.setCallId(testCallId);
        existingMetadata.setCallStatus("PENDING");
        when(callMetadataRepository.findByCallId(testCallId)).thenReturn(Optional.of(existingMetadata));
        when(callMetadataRepository.save(any(CallMetadata.class))).thenReturn(existingMetadata);

        // When
        metadataConsumer.consume(jsonEvent, null);

        // Then
        verify(callMetadataRepository, times(1)).save(any(CallMetadata.class));
        verify(reportService, times(1)).evictMetadataCache(testCallId.toString());
        verify(reportService, times(1)).evictReportCache();
    }

    @Test
    void shouldCreateNewMetadataIfNotFound() {
        // Given
        String jsonEvent = String.format(
                "{\"callId\":\"%s\",\"status\":\"TRANSCRIBING\"}",
                testCallId
        );

        when(callMetadataRepository.findByCallId(testCallId)).thenReturn(Optional.empty());
        when(callMetadataRepository.save(any(CallMetadata.class))).thenAnswer(invocation -> {
            CallMetadata metadata = invocation.getArgument(0);
            metadata.setCallId(testCallId);
            return metadata;
        });

        // When
        metadataConsumer.consume(jsonEvent, null);

        // Then
        ArgumentCaptor<CallMetadata> captor = ArgumentCaptor.forClass(CallMetadata.class);
        verify(callMetadataRepository, times(1)).save(captor.capture());

        CallMetadata saved = captor.getValue();
        assertThat(saved.getCallId()).isEqualTo(testCallId);
        assertThat(saved.getCallStatus()).isEqualTo("TRANSCRIBING");
    }

    @Test
    void shouldSkipEventWithoutCallId() {
        // Given
        String jsonEvent = "{\"status\":\"COMPLETED\"}";

        // When
        metadataConsumer.consume(jsonEvent, null);

        // Then
        verify(callMetadataRepository, never()).save(any());
    }

    @Test
    void shouldSkipEventWithInvalidCallId() {
        // Given
        String jsonEvent = "{\"callId\":\"not-a-uuid\",\"status\":\"COMPLETED\"}";

        // When
        metadataConsumer.consume(jsonEvent, null);

        // Then
        verify(callMetadataRepository, never()).save(any());
    }
}
