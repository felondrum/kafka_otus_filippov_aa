package ru.otus.filippov;

import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.otus.filippov.producer.MockEventProducer;
import ru.otus.filippov.utils.Utils;

public class Application {
    
    private static final Logger log = LoggerFactory.getLogger(Application.class);
    
    public static void main(String[] args) throws Exception {
        log.info("Application start");
        var builder = new StreamsBuilder();
        EventSessionAggregator.startSession(builder);
        int EVENT_COUNT = 50;
        MockEventProducer producer = new MockEventProducer(Utils.BOOTSTRAP_SERVERS, Utils.EVENTS_TOPIC, EVENT_COUNT);
        Utils.runEventsApp(builder, "event-session-aggregator", 1, producer, b -> {});
    }
}
