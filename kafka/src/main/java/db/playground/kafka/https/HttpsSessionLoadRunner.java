package db.playground.kafka.https;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("load-https-sessions")
public class HttpsSessionLoadRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(HttpsSessionLoadRunner.class);

    private final HttpsSessionProducer httpsSessionProducer;

    public HttpsSessionLoadRunner(HttpsSessionProducer httpsSessionProducer) {
        this.httpsSessionProducer = httpsSessionProducer;
    }

    @Override
    public void run(String... args) {
        log.info("Starting HTTPS session load run");
        httpsSessionProducer.loadSessions();
        log.info("Completed HTTPS session load run");
    }
}
