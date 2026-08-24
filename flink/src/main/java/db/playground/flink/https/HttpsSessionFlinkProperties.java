package db.playground.flink.https;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "playground.flink.https")
public record HttpsSessionFlinkProperties(
        String topic,
        Kafka kafka,
        Cassandra cassandra,
        Mongo mongo) {

    public record Kafka(String bootstrapServers, String groupId) {
    }

    public record Cassandra(String contactPoint, int port, String localDatacenter, String keyspace) {
    }

    public record Mongo(String uri, String database) {
    }
}
