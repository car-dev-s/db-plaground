package db.playground.kafka.https;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class HttpsSessionStoreTest {

    @Test
    void collectsAddedSessionsInOrder() {
        HttpsSessionStore store = new HttpsSessionStore();
        HttpsSessionMock first = sessionWith(1);
        HttpsSessionMock second = sessionWith(2);

        store.add(first);
        store.add(second);

        assertThat(store.sessions()).containsExactly(first, second);
    }

    private HttpsSessionMock sessionWith(int seed) {
        Instant now = Instant.now();
        return new HttpsSessionMock(
                "10.0.0." + seed, 50000 + seed,
                "93.0.0." + seed, 443,
                "example.com", "GET", 200,
                500, 1000, 100, now, now.toString());
    }
}
