package db.playground.dynamo.https;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class HttpsSessionEventSerde implements Serde<HttpsSessionEvent> {

    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public Serializer<HttpsSessionEvent> serializer() {
        return (topic, event) -> {
            try {
                return MAPPER.writeValueAsBytes(event);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize HttpsSessionEvent for topic " + topic, e);
            }
        };
    }

    @Override
    public Deserializer<HttpsSessionEvent> deserializer() {
        return (topic, bytes) -> {
            try {
                return MAPPER.readValue(bytes, HttpsSessionEvent.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize HttpsSessionEvent for topic " + topic, e);
            }
        };
    }
}
