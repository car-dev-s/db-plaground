package db.playground.kafka.https;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class HttpsSessionConsumer {

    private static final Logger log = LoggerFactory.getLogger(HttpsSessionConsumer.class);

    private final HttpsSessionStore store;

    public HttpsSessionConsumer(HttpsSessionStore store) {
        this.store = store;
    }

    @KafkaListener(topics = "${playground.kafka.https.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(HttpsSessionMock session) {
        log.debug("Received session {}", session);
        store.add(session);
    }
}
