package ru.otus.filippov.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    public static final String BOOTSTRAP_SERVERS = "localhost:9093";

    public static final String EVENTS_TOPIC = "events";
    public static final String EVENTS_AGGREGATED_TOPIC = "events-aggregated";

    public static final Map<String, Object> adminConfig = Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

    private static final Map<String, Object> streamsConfig = Map.of(
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

    public static StreamsConfig createStreamsConfig(Consumer<Map<String, Object>> builder) {
        var map = new HashMap<>(streamsConfig);
        builder.accept(map);
        return new StreamsConfig(map);
    }


    public static void doAdminAction(AdminClientConsumer action) {
        try (var client = Admin.create(Utils.adminConfig)) {
            action.accept(client);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public interface AdminClientConsumer {
        void accept(Admin client) throws Exception;
    }

    public static void recreateTopics(int numPartitions, int replicationFactor, String... topics) {
        doAdminAction(admin -> {
            admin.deleteTopics(Stream.of(topics).toList());
            Thread.sleep(1000);
            admin.createTopics(Stream.of(topics)
                    .map(it -> new NewTopic(it, numPartitions, (short) replicationFactor))
                    .toList());
            Thread.sleep(2000);
        });
    }

    public static void recreateEventsTopics(int numOfPartitions) {
        log.info("About to recreate topics");
        recreateTopics(numOfPartitions, 1, EVENTS_TOPIC, EVENTS_AGGREGATED_TOPIC);
    }

    public static void runEventsApp(StreamsBuilder builder, String name,
                                   int numOfPartitions,
                                   AbstractProducer producer,
                                   Consumer<Map<String, Object>> configBuilder) throws Exception {
        log.info("Recreating topics...");
        recreateEventsTopics(numOfPartitions);

        var topology = builder.build();

        log.info("Topology built:");
        log.info(topology.describe().toString());

        try (
                var kafkaStreams = new KafkaStreams(topology, Utils.createStreamsConfig(b -> {
                    b.put(StreamsConfig.APPLICATION_ID_CONFIG, name + "-" + UUID.randomUUID().hashCode());
                    b.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
                    configBuilder.accept(b);
                }))
        ) {
            log.info("App Started");
            log.info("Starting Kafka Streams...");
            kafkaStreams.start();
            if (producer != null) {
                producer.start();
                log.info("Sending events...");
                producer.sendEvents();
                log.info("Events sent, flushing producer...");
                producer.join();
            }

            log.info("Waiting for session windows to close...");
            Thread.sleep(15000);
            log.info("Session windows closed");
            log.info("Waiting for processing...");
            Thread.sleep(15000);
            log.info("Shutting down now");
            log.info("Shutdown complete");
        }
    }
}
