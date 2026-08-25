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
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.net.URI;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class DynamoSessionEventWriterIntegrationTest {

    static final GenericContainer<?> DYNAMO_LOCAL = new GenericContainer<>(
            DockerImageName.parse("amazon/dynamodb-local:2.5.2"))
            .withExposedPorts(8000);

    static DynamoDbAsyncClient client;
    static final String TABLE = "test_events";

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
                        .attributeDefinitions(
                                AttributeDefinition.builder().attributeName("sourceIp").attributeType(ScalarAttributeType.S).build(),
                                AttributeDefinition.builder().attributeName("timestampIso").attributeType(ScalarAttributeType.S).build())
                        .keySchema(
                                KeySchemaElement.builder().attributeName("sourceIp").keyType(KeyType.HASH).build(),
                                KeySchemaElement.builder().attributeName("timestampIso").keyType(KeyType.RANGE).build())
                        .build())
                .join();
    }

    @AfterAll
    static void tearDown() {
        client.close();
        DYNAMO_LOCAL.stop();
    }

    @Test
    void writesFullEventAsOneItemKeyedBySourceIpAndTimestamp() {
        DynamoSessionEventWriter writer = new DynamoSessionEventWriter(client, TABLE);

        HttpsSessionEvent event = new HttpsSessionEvent();
        event.setSourceIp("203.0.113.9");
        event.setSourcePort(51000);
        event.setDestinationIp("93.0.0.1");
        event.setDestinationPort(443);
        event.setDomain("example.com");
        event.setMethod("GET");
        event.setStatusCode(200);
        event.setBytesSent(500);
        event.setBytesReceived(1000);
        event.setDurationMillis(150);
        event.setTimestamp(Instant.parse("2026-08-25T10:00:00Z"));
        event.setTimestampIso("2026-08-25T10:00:00Z");

        writer.put(event);

        Map<String, AttributeValue> item = client.getItem(GetItemRequest.builder()
                        .tableName(TABLE)
                        .key(Map.of(
                                "sourceIp", AttributeValue.fromS("203.0.113.9"),
                                "timestampIso", AttributeValue.fromS("2026-08-25T10:00:00Z")))
                        .build())
                .join()
                .item();

        assertThat(item.get("domain").s()).isEqualTo("example.com");
        assertThat(item.get("method").s()).isEqualTo("GET");
        assertThat(item.get("statusCode").n()).isEqualTo("200");
        assertThat(item.get("bytesSent").n()).isEqualTo("500");
        assertThat(item.get("bytesReceived").n()).isEqualTo("1000");
        assertThat(item.get("durationMillis").n()).isEqualTo("150");
    }
}
