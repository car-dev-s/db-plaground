package db.playground.flink.https;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class HttpsSessionDeserializationSchema implements KafkaRecordDeserializationSchema<HttpsSessionEvent> {

    private transient ObjectMapper objectMapper;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<HttpsSessionEvent> out) throws IOException {
        if (record.value() == null) {
            return;
        }
        out.collect(mapper().readValue(record.value(), HttpsSessionEvent.class));
    }

    @Override
    public TypeInformation<HttpsSessionEvent> getProducedType() {
        return TypeInformation.of(HttpsSessionEvent.class);
    }

    private ObjectMapper mapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        }
        return objectMapper;
    }
}
