package db.playground.dynamo.https;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpsSessionDynamoTopology {

    private static final Logger log = LoggerFactory.getLogger(HttpsSessionDynamoTopology.class);

    private final HttpsSessionAggregateWriter aggregateWriter;
    private final HttpsSessionEventWriter eventWriter;

    public HttpsSessionDynamoTopology(HttpsSessionAggregateWriter aggregateWriter, HttpsSessionEventWriter eventWriter) {
        this.aggregateWriter = aggregateWriter;
        this.eventWriter = eventWriter;
    }

    public void build(StreamsBuilder streamsBuilder, String topic) {
        streamsBuilder.stream(topic, Consumed.with(Serdes.String(), new HttpsSessionEventSerde()))
                .foreach(this::writeToBothTables);
    }

    void writeToBothTables(String key, HttpsSessionEvent event) {
        RuntimeException aggregateFailure = attempt(() -> aggregateWriter.update(event), "aggregate", event.getSourceIp());
        RuntimeException eventFailure = attempt(() -> eventWriter.put(event), "event", event.getSourceIp());

        if (aggregateFailure != null || eventFailure != null) {
            throw new HttpsSessionDynamoWriteException(event.getSourceIp(), aggregateFailure, eventFailure);
        }
    }

    private RuntimeException attempt(Runnable write, String writerName, String sourceIp) {
        try {
            write.run();
            return null;
        } catch (RuntimeException e) {
            log.error("Dynamo {} write failed for sourceIp {}", writerName, sourceIp, e);
            return e;
        }
    }
}
