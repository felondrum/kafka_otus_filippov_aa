package com.example.frauddetector.config;

import com.example.frauddetector.processor.AnomalousDurationProcessor;
import com.example.frauddetector.processor.FrequentCallsProcessor;
import com.example.frauddetector.processor.NpsEscalationProcessor;
import com.example.frauddetector.avro.FraudAlert;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

/**
 * Kafka Streams topology configuration for fraud detection.
 */
@Configuration
public class StreamsTopologyConfig {

    private static final Logger log = LoggerFactory.getLogger(StreamsTopologyConfig.class);

    private final FraudDetectorProperties properties;
    private final FrequentCallsProcessor frequentCallsProcessor;
    private final NpsEscalationProcessor npsEscalationProcessor;
    private final AnomalousDurationProcessor anomalousDurationProcessor;

    public StreamsTopologyConfig(
            FraudDetectorProperties properties,
            FrequentCallsProcessor frequentCallsProcessor,
            NpsEscalationProcessor npsEscalationProcessor,
            AnomalousDurationProcessor anomalousDurationProcessor) {
        this.properties = properties;
        this.frequentCallsProcessor = frequentCallsProcessor;
        this.npsEscalationProcessor = npsEscalationProcessor;
        this.anomalousDurationProcessor = anomalousDurationProcessor;
    }

    @Bean
    public Topology fraudDetectionTopology(StreamsBuilder builder) {
        KStream<String, String> callStream = builder
                .stream(properties.getCompletedTopic(),
                        Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, FraudAlert> frequentCallsAlerts = frequentCallsProcessor.detect(callStream);
        KStream<String, FraudAlert> npsEscalationAlerts = npsEscalationProcessor.detect(callStream);
        KStream<String, FraudAlert> anomalousDurationAlerts = anomalousDurationProcessor.detect(callStream);

        KStream<String, FraudAlert> allAlerts = frequentCallsAlerts
                .merge(npsEscalationAlerts)
                .merge(anomalousDurationAlerts);

        allAlerts.to(properties.getFraudAlertsTopic(),
                Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}
