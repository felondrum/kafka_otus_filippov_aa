package ru.otus.filippov.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public abstract class AbstractProducer implements AutoCloseable {
    
    private static final Logger log = LoggerFactory.getLogger(AbstractProducer.class);
    
    protected KafkaProducer<String, String> producer;
    protected String bootstrapServers;
    protected String topic;
    
    public AbstractProducer(String bootstrapServers, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
    }
    
    public void start() {
        log.info("Starting producer...");
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        
        this.producer = new KafkaProducer<>(props);
        log.info("Producer started successfully");
    }
    
    public abstract void sendEvents() throws Exception;
    
    public void join() {
        log.info("Flushing producer...");
        producer.flush();
        log.info("Producer flushed successfully");
    }
    
    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
