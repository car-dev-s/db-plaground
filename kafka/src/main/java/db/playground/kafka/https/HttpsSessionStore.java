package db.playground.kafka.https;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class HttpsSessionStore {

    private static final Logger log = LoggerFactory.getLogger(HttpsSessionStore.class);

    private final List<HttpsSessionMock> sessions = new CopyOnWriteArrayList<>();

    public void add(HttpsSessionMock session) {
        sessions.add(session);
        log.debug("Stored session, total stored: {}", sessions.size());
    }

    public List<HttpsSessionMock> sessions() {
        return List.copyOf(sessions);
    }
}
