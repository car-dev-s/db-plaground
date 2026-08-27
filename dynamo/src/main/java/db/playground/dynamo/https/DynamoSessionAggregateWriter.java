package db.playground.dynamo.https;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class DynamoSessionAggregateWriter implements HttpsSessionAggregateWriter {

    private final DynamoDbAsyncClient client;
    private final String tableName;

    public DynamoSessionAggregateWriter(DynamoDbAsyncClient client, String tableName) {
        this.client = client;
        this.tableName = tableName;
    }

    @Override
    public CompletableFuture<Void> update(HttpsSessionEvent event) {
        // ADD is not idempotent: under at-least-once delivery, a rebalance/restart/retry that
        // replays this event inflates the counters permanently. See docs/delivery-semantics.md.
        StringBuilder addClause = new StringBuilder("ADD eventCount :one");
        StringBuilder setClause = new StringBuilder("SET lastSeen = :lastSeen");
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":one", AttributeValue.fromN("1"));
        values.put(":lastSeen", AttributeValue.fromS(event.getTimestampIso()));

        if (event.getBytesSent() != null) {
            addClause.append(", totalBytesSent :bytesSent");
            values.put(":bytesSent", AttributeValue.fromN(String.valueOf(event.getBytesSent())));
        }
        if (event.getBytesReceived() != null) {
            addClause.append(", totalBytesReceived :bytesReceived");
            values.put(":bytesReceived", AttributeValue.fromN(String.valueOf(event.getBytesReceived())));
        }
        if (event.getDomain() != null) {
            setClause.append(", lastDomain = :domain");
            values.put(":domain", AttributeValue.fromS(event.getDomain()));
        }
        if (event.getMethod() != null) {
            setClause.append(", lastMethod = :method");
            values.put(":method", AttributeValue.fromS(event.getMethod()));
        }
        if (event.getStatusCode() != null) {
            setClause.append(", lastStatusCode = :statusCode");
            values.put(":statusCode", AttributeValue.fromN(String.valueOf(event.getStatusCode())));
        }

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(Map.of("sourceIp", AttributeValue.fromS(event.getSourceIp())))
                .updateExpression(addClause + " " + setClause)
                .expressionAttributeValues(values)
                .build();

        return client.updateItem(request).thenRun(() -> { });
    }
}
