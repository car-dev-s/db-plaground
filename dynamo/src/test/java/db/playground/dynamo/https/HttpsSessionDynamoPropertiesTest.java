package db.playground.dynamo.https;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class HttpsSessionDynamoPropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class)
            .withPropertyValues(
                    "playground.dynamo.https.topic=test-topic",
                    "playground.dynamo.https.dynamo.region=us-west-2",
                    "playground.dynamo.https.dynamo.endpoint-override=http://localhost:8000",
                    "playground.dynamo.https.dynamo.access-key-id=test-key",
                    "playground.dynamo.https.dynamo.secret-access-key=test-secret",
                    "playground.dynamo.https.dynamo.aggregate-table=test_aggregates",
                    "playground.dynamo.https.dynamo.event-table=test_events");

    @Test
    void bindsAllFieldsFromProperties() {
        contextRunner.run(context -> {
            HttpsSessionDynamoProperties properties = context.getBean(HttpsSessionDynamoProperties.class);
            assertThat(properties.topic()).isEqualTo("test-topic");
            assertThat(properties.dynamo().region()).isEqualTo("us-west-2");
            assertThat(properties.dynamo().endpointOverride()).isEqualTo("http://localhost:8000");
            assertThat(properties.dynamo().accessKeyId()).isEqualTo("test-key");
            assertThat(properties.dynamo().secretAccessKey()).isEqualTo("test-secret");
            assertThat(properties.dynamo().aggregateTable()).isEqualTo("test_aggregates");
            assertThat(properties.dynamo().eventTable()).isEqualTo("test_events");
        });
    }

    @Configuration
    @ConfigurationPropertiesScan(basePackageClasses = HttpsSessionDynamoProperties.class)
    static class TestConfig {
    }
}
