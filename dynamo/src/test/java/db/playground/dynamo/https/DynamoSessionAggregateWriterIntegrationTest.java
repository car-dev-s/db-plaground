package db.playground.dynamo.https;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class DynamoSessionAggregateWriterIntegrationTest {

    static final GenericContainer<?> DYNAMO_LOCAL = new GenericContainer<>(
            DockerImageName.parse("amazon/dynamodb-local:2.5.2"))
            .withExposedPorts(8000);

    static DynamoDbAsyncClient client;
    static final String TABLE = "test_aggregates";

    @BeforeAll
    static void setUp() {
        DYNAMO_LOCAL.start();
        client = DynamoDbAsyncClient.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create("http://" + DYNAMO_LOCAL.getHost() + ":" + DYNAMO_LOCAL.getMappedPort(8000)))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                .build();

        client.createTable(CreateTableRequest.builder()
                        .tableName(TABLE)
                        .billingMode(BillingMode.PAY_PER_REQUEST)
                        .attributeDefinitions(AttributeDefinition.builder()
                                .attributeName("sourceIp").attributeType(ScalarAttributeType.S).build())
                        .keySchema(KeySchemaElement.builder()
                                .attributeName("sourceIp").keyType(KeyType.HASH).build())
                        .build())
                .join();
    }

    @AfterAll
    static void tearDown() {
        client.close();
        DYNAMO_LOCAL.stop();
    }

    @Test
    void secondEventForSameSourceIpIncrementsCountersAndOverwritesLastFields() {
        DynamoSessionAggregateWriter writer = new DynamoSessionAggregateWriter(client, TABLE);

        HttpsSessionEvent first = sampleEvent("203.0.113.5", "example.com", "GET", 200, 100, 200, "2026-08-25T10:00:00Z");
        HttpsSessionEvent second = sampleEvent("203.0.113.5", "other.example.com", "POST", 201, 300, 400, "2026-08-25T10:05:00Z");

        writer.update(first).join();
        writer.update(second).join();

        Map<String, AttributeValue> item = client.getItem(GetItemRequest.builder()
                        .tableName(TABLE)
                        .key(Map.of("sourceIp", AttributeValue.fromS("203.0.113.5")))
                        .build())
                .join()
                .item();

        assertThat(item.get("eventCount").n()).isEqualTo("2");
        assertThat(item.get("totalBytesSent").n()).isEqualTo("400");
        assertThat(item.get("totalBytesReceived").n()).isEqualTo("600");
        assertThat(item.get("lastSeen").s()).isEqualTo("2026-08-25T10:05:00Z");
        assertThat(item.get("lastDomain").s()).isEqualTo("other.example.com");
        assertThat(item.get("lastMethod").s()).isEqualTo("POST");
        assertThat(item.get("lastStatusCode").n()).isEqualTo("201");
    }

    private static HttpsSessionEvent sampleEvent(String sourceIp, String domain, String method, int statusCode,
                                                   long bytesSent, long bytesReceived, String timestampIso) {
        HttpsSessionEvent event = new HttpsSessionEvent();
        event.setSourceIp(sourceIp);
        event.setSourcePort(51000);
        event.setDestinationIp("93.0.0.1");
        event.setDestinationPort(443);
        event.setDomain(domain);
        event.setMethod(method);
        event.setStatusCode(statusCode);
        event.setBytesSent(bytesSent);
        event.setBytesReceived(bytesReceived);
        event.setDurationMillis(100L);
        event.setTimestamp(Instant.parse(timestampIso));
        event.setTimestampIso(timestampIso);
        return event;
    }
}
