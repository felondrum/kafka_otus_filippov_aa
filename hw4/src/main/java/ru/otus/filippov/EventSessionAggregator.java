package ru.otus.filippov;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.otus.filippov.model.Event;
import ru.otus.filippov.model.EventKey;
import ru.otus.filippov.model.EventKeySerde;
import ru.otus.filippov.model.EventSerde;
import ru.otus.filippov.utils.Utils;

import java.time.Duration;

public class EventSessionAggregator {

    private static final Logger log = LoggerFactory.getLogger(EventSessionAggregator.class);

    private static final Duration SESSION_TIMEOUT = Duration.ofSeconds(5);

    public static void startSession(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        Serde<Event> eventSerde = new EventSerde();
        Serde<EventKey> eventKeySerde = new EventKeySerde();
        Serde<Long> longSerde = Serdes.Long();

        KStream<String, Event> eventsStream = builder.stream(
                Utils.EVENTS_TOPIC,
                Consumed.with(stringSerde, eventSerde)
        );

        log.info("Starting event aggregation...");

        KTable<Windowed<EventKey>, Long> sessionCounts = eventsStream
                .groupBy((key, event) -> {
                            log.debug("Grouping event with key: {}", event.getKey());
                            return EventKey.builder().key(event.getKey()).build();
                        },
                        Grouped.with(eventKeySerde, eventSerde))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(SESSION_TIMEOUT))
                .count();

        sessionCounts.toStream().foreach((k, v) -> log.info("Window {}: {}", k, v));

        log.info("Session counts created, creating stream...");

        KStream<String, Long> summary = sessionCounts.toStream()
                .map((windowedKey, count) -> {
                    EventKey key = windowedKey.key();
                    return KeyValue.pair(key.getKey(), count);
                });

        summary.foreach((k, v) -> log.info("Count {}: {}", k, v));

        summary.to(Utils.EVENTS_AGGREGATED_TOPIC,
                org.apache.kafka.streams.kstream.Produced.with(stringSerde, longSerde));
    }
}
