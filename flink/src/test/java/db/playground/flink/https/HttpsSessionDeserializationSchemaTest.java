package db.playground.flink.https;

import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HttpsSessionDeserializationSchemaTest {

    @Test
    void deserializesJsonIntoEventWithInstantTimestamp() throws Exception {
        Instant timestamp = Instant.ofEpochSecond(1787598206L);
        String json = """
                {
                  "sourceIp": "10.1.2.3",
                  "sourcePort": 51234,
                  "destinationIp": "93.184.216.34",
                  "destinationPort": 443,
                  "domain": "api.example.com",
                  "method": "GET",
                  "statusCode": 200,
                  "bytesSent": 512,
                  "bytesReceived": 2048,
                  "durationMillis": 42,
                  "timestamp": %d,
                  "timestampIso": "%s"
                }
                """.formatted(timestamp.getEpochSecond(), timestamp);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "playground.https-sessions", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        List<HttpsSessionEvent> collected = new ArrayList<>();
        Collector<HttpsSessionEvent> collector = new Collector<>() {
            @Override
            public void collect(HttpsSessionEvent event) {
                collected.add(event);
            }

            @Override
            public void close() {
            }
        };

        new HttpsSessionDeserializationSchema().deserialize(record, collector);

        assertEquals(1, collected.size());
        HttpsSessionEvent event = collected.get(0);
        assertEquals("10.1.2.3", event.getSourceIp());
        assertEquals(51234, event.getSourcePort());
        assertEquals(443, event.getDestinationPort());
        assertEquals("GET", event.getMethod());
        assertEquals(200, event.getStatusCode());
        assertEquals(timestamp, event.getTimestamp());
        assertEquals(timestamp.toString(), event.getTimestampIso());
    }
}
