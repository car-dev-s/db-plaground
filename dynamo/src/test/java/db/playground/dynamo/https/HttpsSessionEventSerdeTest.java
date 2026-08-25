package db.playground.dynamo.https;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class HttpsSessionEventSerdeTest {

    @Test
    void roundTripsAllFieldsThroughSerializeAndDeserialize() {
        HttpsSessionEvent event = new HttpsSessionEvent();
        event.setSourceIp("10.0.0.1");
        event.setSourcePort(51000);
        event.setDestinationIp("93.0.0.1");
        event.setDestinationPort(443);
        event.setDomain("example.com");
        event.setMethod("GET");
        event.setStatusCode(200);
        event.setBytesSent(500);
        event.setBytesReceived(1000);
        event.setDurationMillis(100);
        Instant now = Instant.now();
        event.setTimestamp(now);
        event.setTimestampIso(now.toString());

        HttpsSessionEventSerde serde = new HttpsSessionEventSerde();
        byte[] bytes = serde.serializer().serialize("playground.https-sessions", event);
        HttpsSessionEvent deserialized = serde.deserializer().deserialize("playground.https-sessions", bytes);

        assertThat(deserialized.getSourceIp()).isEqualTo("10.0.0.1");
        assertThat(deserialized.getSourcePort()).isEqualTo(51000);
        assertThat(deserialized.getDestinationIp()).isEqualTo("93.0.0.1");
        assertThat(deserialized.getDestinationPort()).isEqualTo(443);
        assertThat(deserialized.getDomain()).isEqualTo("example.com");
        assertThat(deserialized.getMethod()).isEqualTo("GET");
        assertThat(deserialized.getStatusCode()).isEqualTo(200);
        assertThat(deserialized.getBytesSent()).isEqualTo(500);
        assertThat(deserialized.getBytesReceived()).isEqualTo(1000);
        assertThat(deserialized.getDurationMillis()).isEqualTo(100);
        assertThat(deserialized.getTimestamp()).isEqualTo(now);
        assertThat(deserialized.getTimestampIso()).isEqualTo(now.toString());
    }
}
