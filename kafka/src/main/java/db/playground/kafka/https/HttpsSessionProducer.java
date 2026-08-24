package db.playground.kafka.https;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class HttpsSessionProducer {

    private static final Logger log = LoggerFactory.getLogger(HttpsSessionProducer.class);

    private static final List<String> DOMAINS = List.of(
            "api.example.com", "www.example.org", "cdn.example.net",
            "auth.example.io", "checkout.example.shop");
    private static final List<String> METHODS = List.of("GET", "GET", "GET", "POST", "PUT", "DELETE");
    private static final int[] STATUS_CODES = {200, 200, 200, 200, 301, 404, 500};

    private final KafkaTemplate<String, HttpsSessionMock> kafkaTemplate;
    private final HttpsSessionProperties properties;

    public HttpsSessionProducer(KafkaTemplate<String, HttpsSessionMock> kafkaTemplate,
                                 HttpsSessionProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    public void loadSessions() {
        log.info("Loading {} mock HTTPS sessions into topic '{}'", properties.sessionCount(), properties.topic());
        for (int i = 0; i < properties.sessionCount(); i++) {
            HttpsSessionMock session = randomSession();
            String key = session.sourceIp() + ":" + session.sourcePort();
            log.debug("Sending session {}", session);
            kafkaTemplate.send(properties.topic(), key, session);
        }
        log.info("Finished loading {} mock HTTPS sessions into topic '{}'", properties.sessionCount(), properties.topic());
    }

    private HttpsSessionMock randomSession() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Instant now = Instant.now();

        return new HttpsSessionMock(
                randomPrivateIp(random),
                random.nextInt(49152, 65536),
                randomPublicIp(random),
                random.nextBoolean() ? 443 : 8443,
                DOMAINS.get(random.nextInt(DOMAINS.size())),
                METHODS.get(random.nextInt(METHODS.size())),
                STATUS_CODES[random.nextInt(STATUS_CODES.length)],
                random.nextLong(200, 4096),
                random.nextLong(500, 500_000),
                random.nextLong(5, 3000),
                now,
                now.toString());
    }

    private String randomPrivateIp(ThreadLocalRandom random) {
        return "10.%d.%d.%d".formatted(random.nextInt(256), random.nextInt(256), random.nextInt(1, 255));
    }

    private String randomPublicIp(ThreadLocalRandom random) {
        return "%d.%d.%d.%d".formatted(
                random.nextInt(1, 224), random.nextInt(256), random.nextInt(256), random.nextInt(1, 255));
    }
}
