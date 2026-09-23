package com.example.transcription.analyzer.metadata;

import com.example.transcription.avro.CallMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Component
public class MetadataManager {

    private static final Logger log = LoggerFactory.getLogger(MetadataManager.class);

    private final KafkaTemplate<String, CallMetadata> kafkaTemplate;

    public MetadataManager(KafkaTemplate<String, CallMetadata> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Track status transitions for a call.
     * Expected sequence: TRANSCRIBING -> SUMMARIZING -> COMPLETED
     */
    public void transition(String callId, CallMetadata.Status status) {
        CallMetadata metadata = new CallMetadata(callId, status.toString(), System.currentTimeMillis());

        CompletableFuture<SendResult<String, CallMetadata>> future =
                kafkaTemplate.send("calls.metadata", callId, metadata);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Produced metadata event: callId={}, status={}, topic={}, partition={}, offset={}",
                        callId, status,
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to produce metadata event: callId={}, status={}", callId, status, ex);
            }
        });
    }

    /**
     * Convenience methods for each status transition.
     */
    public void onTranscriptionProduced(String callId) {
        transition(callId, CallMetadata.Status.TRANSCRIBING);
    }

    public void onSummaryStarted(String callId) {
        transition(callId, CallMetadata.Status.SUMMARIZING);
    }

    public void onDualWriteComplete(String callId) {
        transition(callId, CallMetadata.Status.COMPLETED);
    }

    /**
     * Verify status transition sequence.
     * Returns true if the new status is valid in the sequence.
     */
    public static boolean isValidTransition(CallMetadata.Status current, CallMetadata.Status next) {
        List<CallMetadata.Status> sequence = List.of(
                CallMetadata.Status.TRANSCRIBING,
                CallMetadata.Status.SUMMARIZING,
                CallMetadata.Status.COMPLETED
        );

        int currentIndex = sequence.indexOf(current);
        int nextIndex = sequence.indexOf(next);

        // Allow transitions to the next status in sequence
        return nextIndex == currentIndex + 1 || (nextIndex == 0 && currentIndex == -1);
    }
}
