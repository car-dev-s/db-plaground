package db.playground.kafka;

import db.playground.kafka.https.HttpsSessionProducer;
import db.playground.kafka.https.HttpsSessionProperties;
import db.playground.kafka.https.HttpsSessionStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
class HttpsSessionIntegrationTest {

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));

    @Autowired
    private HttpsSessionProducer httpsSessionProducer;

    @Autowired
    private HttpsSessionStore httpsSessionStore;

    @Autowired
    private HttpsSessionProperties properties;

    @Test
    void loadedSessionsAreFullyConsumedAndLookLikeRealHttpsTraffic() {
        httpsSessionProducer.loadSessions();

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() ->
                        assertThat(httpsSessionStore.sessions()).hasSize(properties.sessionCount()));

        assertThat(httpsSessionStore.sessions()).allSatisfy(session -> {
            assertThat(session.sourceIp()).matches("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$");
            assertThat(session.destinationIp()).matches("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$");
            assertThat(session.destinationPort()).isIn(443, 8443);
            assertThat(session.bytesSent()).isPositive();
            assertThat(session.bytesReceived()).isPositive();
        });
    }
}
