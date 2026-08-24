package db.playground.flink.https;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class HttpsSessionFlinkJob implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(HttpsSessionFlinkJob.class);

    private final HttpsSessionFlinkProperties properties;

    public HttpsSessionFlinkJob(HttpsSessionFlinkProperties properties) {
        this.properties = properties;
    }

    @Override
    public void run(String... args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<HttpsSessionEvent> source = KafkaSource.<HttpsSessionEvent>builder()
                .setBootstrapServers(properties.kafka().bootstrapServers())
                .setTopics(properties.topic())
                .setGroupId(properties.kafka().groupId())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new HttpsSessionDeserializationSchema())
                .build();

        DataStream<HttpsSessionEvent> sessions = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "kafka-https-sessions");

        sessions.sinkTo(new CassandraHttpsSessionSink(
                properties.cassandra().contactPoint(),
                properties.cassandra().port(),
                properties.cassandra().localDatacenter(),
                properties.cassandra().keyspace()));

        sessions.sinkTo(new MongoHttpsSessionSink(
                properties.mongo().uri(),
                properties.mongo().database()));

        log.info("Starting Flink https-sessions sink job (Kafka topic '{}' -> Cassandra + MongoDB)", properties.topic());
        env.execute("https-sessions-flink-sink");
    }
}
