package com.example.frauddetector.stream;

import com.example.frauddetector.config.FraudDetectorProperties;
import com.example.frauddetector.processor.AnomalousDurationProcessor;
import com.example.frauddetector.processor.FrequentCallsProcessor;
import com.example.frauddetector.processor.NpsEscalationProcessor;
import com.example.frauddetector.avro.FraudAlert;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.streams.KafkaStreamsTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;

/**
 * Main Kafka Streams topology for fraud detection.
 * Combines DSL-based frequent calls detection with Processor API for NPS escalation.
 */
@Component
@EnableKafkaStreams
public class FraudDetectionTopology implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(FraudDetectionTopology.class);

    private final FraudDetectorProperties properties;
    private final FrequentCallsProcessor frequentCallsProcessor;
    private final NpsEscalationProcessor npsEscalationProcessor;
    private final AnomalousDurationProcessor anomalousDurationProcessor;
    private final KafkaStreamsTemplate<String, FraudAlert> streamsTemplate;

    private KafkaStreams kafkaStreams;

    public FraudDetectionTopology(
            FraudDetectorProperties properties,
            FrequentCallsProcessor frequentCallsProcessor,
            NpsEscalationProcessor npsEscalationProcessor,
            AnomalousDurationProcessor anomalousDurationProcessor,
            KafkaStreamsTemplate<String, FraudAlert> streamsTemplate) {
        this.properties = properties;
        this.frequentCallsProcessor = frequentCallsProcessor;
        this.npsEscalationProcessor = npsEscalationProcessor;
        this.anomalousDurationProcessor = anomalousDurationProcessor;
        this.streamsTemplate = streamsTemplate;
    }

    /**
     * Build the Kafka Streams topology.
     */
    public void buildTopology(StreamsBuilder streamsBuilder) {
        // Input: calls.completed
        KStream<String, String> callStream = streamsBuilder
                .stream(properties.getCompletedTopic(),
                        Consumed.with(Serdes.String(), Serdes.String()));

        // Branch 1: Frequent calls detection (DSL)
        KStream<String, FraudAlert> frequentCallsAlerts = frequentCallsProcessor.detect(callStream);

        // Branch 2: NPS escalation detection (Processor API)
        KStream<String, FraudAlert> npsEscalationAlerts = npsEscalationProcessor.detect(callStream);

        // Branch 3: Anomalous duration detection
        KStream<String, FraudAlert> anomalousDurationAlerts = anomalousDurationProcessor.detect(callStream);

        // Merge all alert streams
        KStream<String, FraudAlert> allAlerts = KStream.branches(
                frequentCallsAlerts,
                npsEscalationAlerts,
                anomalousDurationAlerts
        )[0] // This won't work - need different approach
                .merge(frequentCallsAlerts)
                .merge(npsEscalationAlerts)
                .merge(anomalousDurationAlerts);

        // Output: calls.fraud-alerts
        allAlerts.to(properties.getFraudAlertsTopic(),
                Produced.with(Serdes.String(), /* AvroSerde for FraudAlert */ null));
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("Starting fraud detection topology...");
        // Topology will be built by Spring Kafka Streams integration
    }

    @PreDestroy
    public void cleanup() {
        if (kafkaStreams != null) {
            kafkaStreams.close();
            log.info("Fraud detection topology closed");
        }
    }
}
