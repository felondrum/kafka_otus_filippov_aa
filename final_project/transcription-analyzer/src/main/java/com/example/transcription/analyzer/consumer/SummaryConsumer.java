package com.example.transcription.analyzer.consumer;

import com.example.transcription.analyzer.generator.SummaryGenerator;
import com.example.transcription.analyzer.generator.SummaryGenerator.SummaryResult;
import com.example.transcription.avro.RawTranscription;
import com.example.transcription.avro.TranscriptionSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class SummaryConsumer {

    private static final Logger log = LoggerFactory.getLogger(SummaryConsumer.class);

    private final SummaryGenerator summaryGenerator;
    private final KafkaTemplate<String, TranscriptionSummary> kafkaTemplate;

    public SummaryConsumer(SummaryGenerator summaryGenerator,
                           KafkaTemplate<String, TranscriptionSummary> kafkaTemplate) {
        this.summaryGenerator = summaryGenerator;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "transcription.raw", groupId = "summary-processor")
    public void consume(RawTranscription rawTranscription) {
        String callId = rawTranscription.getCallId();
        log.info("Processing raw transcription for callId={}", callId);

        try {
            SummaryResult summary = summaryGenerator.generate(callId, rawTranscription.getText());

            TranscriptionSummary transcriptionSummary = new TranscriptionSummary(
                    callId,
                    summary.getProblem(),
                    summary.getSolution(),
                    summary.getSentiment(),
                    summary.getUrgency(),
                    summary.getConfidence()
            );

            CompletableFuture<SendResult<String, TranscriptionSummary>> future =
                    kafkaTemplate.send("transcription.summary", callId, transcriptionSummary);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Produced transcription summary: callId={}, topic={}, partition={}, offset={}",
                            callId,
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("Failed to produce transcription summary: callId={}", callId, ex);
                }
            });
        } catch (Exception e) {
            log.error("Error generating summary for callId={}", callId, e);
        }
    }
}
