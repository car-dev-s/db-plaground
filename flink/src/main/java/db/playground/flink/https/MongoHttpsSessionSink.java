package db.playground.flink.https;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.bson.Document;

import java.util.Date;

public class MongoHttpsSessionSink implements Sink<HttpsSessionEvent> {

    private final String connectionUri;
    private final String database;

    public MongoHttpsSessionSink(String connectionUri, String database) {
        this.connectionUri = connectionUri;
        this.database = database;
    }

    @Override
    public SinkWriter<HttpsSessionEvent> createWriter(WriterInitContext context) {
        return new MongoSinkWriter(connectionUri, database);
    }

    private static class MongoSinkWriter implements SinkWriter<HttpsSessionEvent> {

        private final MongoClient client;
        private final MongoCollection<Document> collection;

        MongoSinkWriter(String connectionUri, String database) {
            client = MongoClients.create(connectionUri);
            collection = client.getDatabase(database).getCollection("https_sessions");
        }

        @Override
        public void write(HttpsSessionEvent event, Context context) {
            Document document = new Document()
                    .append("sourceIp", event.getSourceIp())
                    .append("sourcePort", event.getSourcePort())
                    .append("destinationIp", event.getDestinationIp())
                    .append("destinationPort", event.getDestinationPort())
                    .append("domain", event.getDomain())
                    .append("method", event.getMethod())
                    .append("statusCode", event.getStatusCode())
                    .append("bytesSent", event.getBytesSent())
                    .append("bytesReceived", event.getBytesReceived())
                    .append("durationMillis", event.getDurationMillis())
                    .append("timestamp", Date.from(event.getTimestamp()))
                    .append("timestampIso", event.getTimestampIso());
            collection.insertOne(document);
        }

        @Override
        public void flush(boolean endOfInput) {
        }

        @Override
        public void close() {
            client.close();
        }
    }
}
