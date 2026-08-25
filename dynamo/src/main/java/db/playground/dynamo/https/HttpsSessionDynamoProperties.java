package db.playground.dynamo.https;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "playground.dynamo.https")
public record HttpsSessionDynamoProperties(String topic, Dynamo dynamo) {

    public record Dynamo(
            String region,
            String endpointOverride,
            String accessKeyId,
            String secretAccessKey,
            String aggregateTable,
            String eventTable) {
    }
}
