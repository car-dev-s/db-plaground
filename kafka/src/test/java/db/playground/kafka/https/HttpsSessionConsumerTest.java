package db.playground.kafka.https;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class HttpsSessionConsumerTest {

    @Test
    void delegatesReceivedSessionToStore() {
        HttpsSessionStore store = new HttpsSessionStore();
        HttpsSessionConsumer consumer = new HttpsSessionConsumer(store);
        Instant now = Instant.now();
        HttpsSessionMock session = new HttpsSessionMock(
                "10.0.0.1", 51000,
                "93.0.0.1", 443,
                "example.com", "GET", 200,
                500, 1000, 100, now, now.toString());

        consumer.onMessage(session);

        assertThat(store.sessions()).containsExactly(session);
    }
}
