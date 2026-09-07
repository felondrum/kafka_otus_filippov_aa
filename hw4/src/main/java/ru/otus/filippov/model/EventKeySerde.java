package ru.otus.filippov.model;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class EventKeySerde implements Serde<EventKey> {

    private final Gson gson = new Gson();

    @Override
    public Serializer<EventKey> serializer() {
        return new Serializer<>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public byte[] serialize(String topic, EventKey data) {
                if (data == null) return null;
                return gson.toJson(data).getBytes();
            }
        };
    }

    @Override
    public Deserializer<EventKey> deserializer() {
        return new Deserializer<>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public EventKey deserialize(String topic, byte[] data) {
                if (data == null) return null;
                return gson.fromJson(new String(data), EventKey.class);
            }
        };
    }
}
