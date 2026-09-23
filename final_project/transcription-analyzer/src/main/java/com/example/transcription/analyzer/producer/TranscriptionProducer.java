package com.example.transcription.analyzer.producer;

import com.example.transcription.avro.RawTranscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

@Component
public class TranscriptionProducer {

    private static final Logger log = LoggerFactory.getLogger(TranscriptionProducer.class);

    private final KafkaTemplate<String, RawTranscription> kafkaTemplate;
    private final Random random = new Random();
    private final List<String> textTemplates;
    private final int wordsPerMinute;

    public TranscriptionProducer(
            KafkaTemplate<String, RawTranscription> kafkaTemplate,
            @Value("${transcription.text-templates}") List<String> textTemplates,
            @Value("${transcription.words-per-minute:150}") int wordsPerMinute) {
        this.kafkaTemplate = kafkaTemplate;
        this.textTemplates = textTemplates;
        this.wordsPerMinute = wordsPerMinute;
    }

    /**
     * Generate and produce a synthetic transcription for a completed call.
     * @param callId unique call identifier
     * @param durationMinutes call duration in minutes
     * @return the produced RawTranscription record
     */
    public RawTranscription produce(String callId, double durationMinutes) {
        String text = generateText(durationMinutes);
        float quality = generateQuality();
        String language = generateLanguage();

        RawTranscription record = new RawTranscription(callId, text, quality, language);

        CompletableFuture<SendResult<String, RawTranscription>> future =
                kafkaTemplate.send("transcription.raw", callId, record);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Produced raw transcription: callId={}, topic={}, partition={}, offset={}",
                        callId,
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to produce raw transcription: callId={}", callId, ex);
            }
        });

        return record;
    }

    /**
     * Generate synthetic transcription text based on call duration.
     * ~150 words per minute of call duration.
     */
    private String generateText(double durationMinutes) {
        int targetWords = (int) (durationMinutes * wordsPerMinute);
        if (targetWords < 10) targetWords = 10;
        if (targetWords > 5000) targetWords = 5000;

        StringBuilder text = new StringBuilder();
        int wordsGenerated = 0;

        while (wordsGenerated < targetWords) {
            String template = textTemplates.get(random.nextInt(textTemplates.size()));
            String topic = getRandomTopic();
            String sentence = template.replace("{topic}", topic);
            text.append(sentence).append(" ");
            wordsGenerated += countWords(sentence);
        }

        return text.toString().trim();
    }

    /**
     * Generate random quality score between 0.0 and 1.0.
     */
    private float generateQuality() {
        return Math.round(random.nextFloat() * 100.0f) / 100.0f;
    }

    /**
     * Randomly select language: ru or en.
     */
    private String generateLanguage() {
        return random.nextBoolean() ? "ru" : "en";
    }

    /**
     * Get a random topic placeholder for text generation.
     */
    private String getRandomTopic() {
        String[] topics = {"card", "loan", "payment", "balance", "transfer", "fraud", "complaint", "limit"};
        return topics[random.nextInt(topics.length)];
    }

    /**
     * Count words in a string.
     */
    private int countWords(String text) {
        if (text == null || text.trim().isEmpty()) return 0;
        return text.trim().split("\\s+").length;
    }
}
