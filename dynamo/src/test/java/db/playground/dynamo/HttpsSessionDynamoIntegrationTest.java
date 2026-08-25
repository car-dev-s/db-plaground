package db.playground.dynamo;

import db.playground.dynamo.https.HttpsSessionDynamoProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
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
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
class HttpsSessionDynamoIntegrationTest {

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));

    @Container
    static final GenericContainer<?> DYNAMO_LOCAL = new GenericContainer<>(
            DockerImageName.parse("amazon/dynamodb-local:2.5.2"))
            .withExposedPorts(8000);

    @BeforeAll
    static void createTopic() throws Exception {
        Map<String, Object> adminProps = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.createTopics(java.util.List.of(new NewTopic("playground.https-sessions", 1, (short) 1))).all().get();
        }
    }

    @DynamicPropertySource
    static void dynamoProperties(DynamicPropertyRegistry registry) {
        registry.add("playground.dynamo.https.dynamo.endpoint-override",
                () -> "http://" + DYNAMO_LOCAL.getHost() + ":" + DYNAMO_LOCAL.getMappedPort(8000));
        registry.add("spring.kafka.producer.key-serializer",
                () -> "org.apache.kafka.common.serialization.StringSerializer");
        registry.add("spring.kafka.producer.value-serializer",
                () -> "org.springframework.kafka.support.serializer.JsonSerializer");
    }

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private HttpsSessionDynamoProperties properties;

    private static DynamoDbAsyncClient testClient() {
        return DynamoDbAsyncClient.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create("http://" + DYNAMO_LOCAL.getHost() + ":" + DYNAMO_LOCAL.getMappedPort(8000)))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                .build();
    }

    @Test
    void eventPublishedToKafkaEndsUpInBothDynamoTables() {
        try (DynamoDbAsyncClient client = testClient()) {
            client.createTable(CreateTableRequest.builder()
                            .tableName(properties.dynamo().aggregateTable())
                            .billingMode(BillingMode.PAY_PER_REQUEST)
                            .attributeDefinitions(AttributeDefinition.builder()
                                    .attributeName("sourceIp").attributeType(ScalarAttributeType.S).build())
                            .keySchema(KeySchemaElement.builder()
                                    .attributeName("sourceIp").keyType(KeyType.HASH).build())
                            .build())
                    .join();

            client.createTable(CreateTableRequest.builder()
                            .tableName(properties.dynamo().eventTable())
                            .billingMode(BillingMode.PAY_PER_REQUEST)
                            .attributeDefinitions(
                                    AttributeDefinition.builder().attributeName("sourceIp").attributeType(ScalarAttributeType.S).build(),
                                    AttributeDefinition.builder().attributeName("timestampIso").attributeType(ScalarAttributeType.S).build())
                            .keySchema(
                                    KeySchemaElement.builder().attributeName("sourceIp").keyType(KeyType.HASH).build(),
                                    KeySchemaElement.builder().attributeName("timestampIso").keyType(KeyType.RANGE).build())
                            .build())
                    .join();

            String sourceIp = "203.0.113.42";
            String timestampIso = Instant.now().toString();
            Map<String, Object> event = new HashMap<>();
            event.put("sourceIp", sourceIp);
            event.put("sourcePort", 51000);
            event.put("destinationIp", "93.0.0.1");
            event.put("destinationPort", 443);
            event.put("domain", "example.com");
            event.put("method", "GET");
            event.put("statusCode", 200);
            event.put("bytesSent", 500);
            event.put("bytesReceived", 1000);
            event.put("durationMillis", 100);
            event.put("timestamp", Instant.now().toString());
            event.put("timestampIso", timestampIso);

            kafkaTemplate.send(properties.topic(), sourceIp, event);

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                Map<String, AttributeValue> aggregate = client.getItem(GetItemRequest.builder()
                                .tableName(properties.dynamo().aggregateTable())
                                .key(Map.of("sourceIp", AttributeValue.fromS(sourceIp)))
                                .build())
                        .join()
                        .item();
                assertThat(aggregate).isNotEmpty();
                assertThat(aggregate.get("eventCount").n()).isEqualTo("1");

                Map<String, AttributeValue> eventItem = client.getItem(GetItemRequest.builder()
                                .tableName(properties.dynamo().eventTable())
                                .key(Map.of(
                                        "sourceIp", AttributeValue.fromS(sourceIp),
                                        "timestampIso", AttributeValue.fromS(timestampIso)))
                                .build())
                        .join()
                        .item();
                assertThat(eventItem).isNotEmpty();
                assertThat(eventItem.get("domain").s()).isEqualTo("example.com");
            });
        }
    }
}
