package db.playground.dynamo.https;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class DynamoSessionEventWriter implements HttpsSessionEventWriter {

    private final DynamoDbAsyncClient client;
    private final String tableName;

    public DynamoSessionEventWriter(DynamoDbAsyncClient client, String tableName) {
        this.client = client;
        this.tableName = tableName;
    }

    @Override
    public CompletableFuture<Void> put(HttpsSessionEvent event) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("sourceIp", AttributeValue.fromS(event.getSourceIp()));
        item.put("timestampIso", AttributeValue.fromS(event.getTimestampIso()));
        putIfPresent(item, "sourcePort", event.getSourcePort());
        putIfPresent(item, "destinationIp", event.getDestinationIp());
        putIfPresent(item, "destinationPort", event.getDestinationPort());
        putIfPresent(item, "domain", event.getDomain());
        putIfPresent(item, "method", event.getMethod());
        putIfPresent(item, "statusCode", event.getStatusCode());
        putIfPresent(item, "bytesSent", event.getBytesSent());
        putIfPresent(item, "bytesReceived", event.getBytesReceived());
        putIfPresent(item, "durationMillis", event.getDurationMillis());

        return client.putItem(PutItemRequest.builder()
                        .tableName(tableName)
                        .item(item)
                        .build())
                .thenRun(() -> { });
    }

    private void putIfPresent(Map<String, AttributeValue> item, String name, String value) {
        if (value != null) {
            item.put(name, AttributeValue.fromS(value));
        }
    }

    private void putIfPresent(Map<String, AttributeValue> item, String name, Number value) {
        if (value != null) {
            item.put(name, AttributeValue.fromN(String.valueOf(value)));
        }
    }
}
