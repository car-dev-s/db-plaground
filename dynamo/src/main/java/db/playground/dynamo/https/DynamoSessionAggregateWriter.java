package db.playground.dynamo.https;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.util.Map;

public class DynamoSessionAggregateWriter implements HttpsSessionAggregateWriter {

    private final DynamoDbAsyncClient client;
    private final String tableName;

    public DynamoSessionAggregateWriter(DynamoDbAsyncClient client, String tableName) {
        this.client = client;
        this.tableName = tableName;
    }

    @Override
    public void update(HttpsSessionEvent event) {
        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(Map.of("sourceIp", AttributeValue.fromS(event.getSourceIp())))
                .updateExpression(
                        "ADD eventCount :one, totalBytesSent :bytesSent, totalBytesReceived :bytesReceived "
                                + "SET lastSeen = :lastSeen, lastDomain = :domain, lastMethod = :method, lastStatusCode = :statusCode")
                .expressionAttributeValues(Map.of(
                        ":one", AttributeValue.fromN("1"),
                        ":bytesSent", AttributeValue.fromN(String.valueOf(event.getBytesSent())),
                        ":bytesReceived", AttributeValue.fromN(String.valueOf(event.getBytesReceived())),
                        ":lastSeen", AttributeValue.fromS(event.getTimestampIso()),
                        ":domain", AttributeValue.fromS(event.getDomain()),
                        ":method", AttributeValue.fromS(event.getMethod()),
                        ":statusCode", AttributeValue.fromN(String.valueOf(event.getStatusCode()))))
                .build();

        client.updateItem(request).join();
    }
}
