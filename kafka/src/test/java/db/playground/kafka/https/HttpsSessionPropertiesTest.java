package db.playground.kafka.https;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class HttpsSessionPropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class)
            .withPropertyValues(
                    "playground.kafka.https.topic=test-https-topic",
                    "playground.kafka.https.session-count=42");

    @Test
    void bindsTopicAndSessionCountFromProperties() {
        contextRunner.run(context -> {
            HttpsSessionProperties properties = context.getBean(HttpsSessionProperties.class);
            assertThat(properties.topic()).isEqualTo("test-https-topic");
            assertThat(properties.sessionCount()).isEqualTo(42);
        });
    }

    @Configuration
    @ConfigurationPropertiesScan(basePackageClasses = HttpsSessionProperties.class)
    static class TestConfig {
    }
}
