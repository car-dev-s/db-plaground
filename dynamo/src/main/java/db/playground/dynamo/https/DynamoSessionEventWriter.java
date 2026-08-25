package db.playground.dynamo.https;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.HashMap;
import java.util.Map;

public class DynamoSessionEventWriter implements HttpsSessionEventWriter {

    private final DynamoDbAsyncClient client;
    private final String tableName;

    public DynamoSessionEventWriter(DynamoDbAsyncClient client, String tableName) {
        this.client = client;
        this.tableName = tableName;
    }

    @Override
    public void put(HttpsSessionEvent event) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("sourceIp", AttributeValue.fromS(event.getSourceIp()));
        item.put("timestampIso", AttributeValue.fromS(event.getTimestampIso()));
        item.put("sourcePort", AttributeValue.fromN(String.valueOf(event.getSourcePort())));
        item.put("destinationIp", AttributeValue.fromS(event.getDestinationIp()));
        item.put("destinationPort", AttributeValue.fromN(String.valueOf(event.getDestinationPort())));
        item.put("domain", AttributeValue.fromS(event.getDomain()));
        item.put("method", AttributeValue.fromS(event.getMethod()));
        item.put("statusCode", AttributeValue.fromN(String.valueOf(event.getStatusCode())));
        item.put("bytesSent", AttributeValue.fromN(String.valueOf(event.getBytesSent())));
        item.put("bytesReceived", AttributeValue.fromN(String.valueOf(event.getBytesReceived())));
        item.put("durationMillis", AttributeValue.fromN(String.valueOf(event.getDurationMillis())));

        client.putItem(PutItemRequest.builder()
                        .tableName(tableName)
                        .item(item)
                        .build())
                .join();
    }
}
