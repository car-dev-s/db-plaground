package db.playground.kafka.https;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "playground.kafka.https")
public record HttpsSessionProperties(String topic, int sessionCount) {
}
