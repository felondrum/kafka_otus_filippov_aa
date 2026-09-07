package ru.otus.filippov.producer;

import com.github.javafaker.Faker;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.otus.filippov.model.Event;
import ru.otus.filippov.utils.AbstractProducer;

import java.util.Arrays;
import java.util.List;

public class MockEventProducer extends AbstractProducer {
    
    private static final Logger log = LoggerFactory.getLogger(MockEventProducer.class);
    
    private static final List<String> KEYS = Arrays.asList("user1", "user2", "user3", "user4", "user5");
    private final int eventCount;
    private final Faker faker = new Faker();
    private final Gson gson = new Gson();
    
    public MockEventProducer(String bootstrapServers, String topic, int eventCount) {
        super(bootstrapServers, topic);
        this.eventCount = eventCount;
    }
    
    @Override
    public void sendEvents() throws Exception {
        // Send all events for each key sequentially to ensure session closure
        for (String key : KEYS) {
            for (int i = 0; i < eventCount / KEYS.size(); i++) {
                Event event = Event.builder()
                        .key(key)
                        .value(faker.lorem().sentence())
                        .timestamp(String.valueOf(System.currentTimeMillis()))
                        .build();
                
                String value = gson.toJson(event);
                producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, key, value));
                
                log.info("Sent event with key: {}, value: {}", key, value);
                
                // Small delay to ensure proper ordering
                Thread.sleep(50);
            }
        }
    }
}
