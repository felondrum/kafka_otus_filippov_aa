package com.example.reportingnps.consumer;

import com.example.reportingnps.entity.CallMetadata;
import com.example.reportingnps.entity.FraudStats;
import com.example.reportingnps.repository.CallMetadataRepository;
import com.example.reportingnps.repository.FraudStatsRepository;
import com.example.reportingnps.service.AgentReportService;
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
class FraudAlertConsumerTest {

    @Mock
    private FraudStatsRepository fraudStatsRepository;

    @Mock
    private CallMetadataRepository metadataRepository;

    @Mock
    private ReportService reportService;

    @Mock
    private AgentReportService agentReportService;

    @InjectMocks
    private FraudAlertConsumer consumer;

    private UUID testCallId;

    @BeforeEach
    void setUp() {
        testCallId = UUID.randomUUID();
    }

    @Test
    void shouldConsumeAndAggregateFraudAlert() {
        // Given
        String jsonEvent = String.format(
                "{\"callId\":\"%s\",\"phone\":\"+1234567890\",\"pattern\":\"FREQUENT_CALLS\",\"severity\":\"HIGH\"}",
                testCallId
        );

        when(metadataRepository.findByCallId(testCallId)).thenReturn(Optional.empty());
        when(fraudStatsRepository.findByPhoneAndPatternAndSeverity("+1234567890", "FREQUENT_CALLS", "HIGH"))
                .thenReturn(Optional.empty());
        when(fraudStatsRepository.save(any(FraudStats.class))).thenAnswer(invocation -> {
            FraudStats stats = invocation.getArgument(0);
            stats.setPhone(stats.getPhone());
            return stats;
        });

        // When
        consumer.consume(jsonEvent, null);

        // Then
        ArgumentCaptor<FraudStats> captor = ArgumentCaptor.forClass(FraudStats.class);
        verify(fraudStatsRepository, times(1)).save(captor.capture());

        FraudStats saved = captor.getValue();
        assertThat(saved.getPhone()).isEqualTo("+1234567890");
        assertThat(saved.getPattern()).isEqualTo("FREQUENT_CALLS");
        assertThat(saved.getSeverity()).isEqualTo("HIGH");
        assertThat(saved.getCount()).isEqualTo(1);
    }

    @Test
    void shouldCorrelateFraudAlertWithAgent() {
        // Given
        CallMetadata metadata = new CallMetadata();
        metadata.setCallId(testCallId);
        metadata.setAgentId("agent-001");

        String jsonEvent = String.format(
                "{\"callId\":\"%s\",\"phone\":\"+1234567890\",\"pattern\":\"NPS_ESCALATION\",\"severity\":\"MEDIUM\"}",
                testCallId
        );

        when(metadataRepository.findByCallId(testCallId)).thenReturn(Optional.of(metadata));
        when(fraudStatsRepository.findByPhoneAndPatternAndSeverity("+1234567890", "NPS_ESCALATION", "MEDIUM"))
                .thenReturn(Optional.empty());
        when(fraudStatsRepository.save(any(FraudStats.class))).thenAnswer(invocation -> {
            FraudStats stats = invocation.getArgument(0);
            stats.setPhone(stats.getPhone());
            return stats;
        });

        // When
        consumer.consume(jsonEvent, null);

        // Then
        ArgumentCaptor<FraudStats> captor = ArgumentCaptor.forClass(FraudStats.class);
        verify(fraudStatsRepository, times(1)).save(captor.capture());

        FraudStats saved = captor.getValue();
        assertThat(saved.getAgentId()).isEqualTo("agent-001");
        verify(agentReportService, times(1)).evictAgentReportCache("agent-001");
    }

    @Test
    void shouldSkipEventWithoutCallId() {
        // Given
        String jsonEvent = "{\"phone\":\"+1234567890\",\"pattern\":\"FREQUENT_CALLS\"}";

        // When
        consumer.consume(jsonEvent, null);

        // Then
        verify(fraudStatsRepository, never()).save(any());
    }
}
