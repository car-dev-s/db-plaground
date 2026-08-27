package db.playground.dynamo.https;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

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
                .filter(this::isValid)
                .foreach(this::writeToBothTables);
    }

    private boolean isValid(String key, HttpsSessionEvent event) {
        if (event == null) {
            return false;
        }
        if (event.getSourceIp() == null || event.getSourceIp().isBlank()
                || event.getTimestampIso() == null || event.getTimestampIso().isBlank()) {
            log.warn("Dropping HttpsSessionEvent with missing sourceIp/timestampIso for key {}", key);
            return false;
        }
        return true;
    }

    void writeToBothTables(String key, HttpsSessionEvent event) {
        CompletableFuture<Void> aggregateWrite = start(() -> aggregateWriter.update(event));
        CompletableFuture<Void> eventWrite = start(() -> eventWriter.put(event));

        RuntimeException aggregateFailure = await(aggregateWrite, "aggregate", event.getSourceIp());
        RuntimeException eventFailure = await(eventWrite, "event", event.getSourceIp());

        if (aggregateFailure != null || eventFailure != null) {
            throw new HttpsSessionDynamoWriteException(event.getSourceIp(), aggregateFailure, eventFailure);
        }
    }

    private CompletableFuture<Void> start(Supplier<CompletableFuture<Void>> write) {
        try {
            return write.get();
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private RuntimeException await(CompletableFuture<Void> future, String writerName, String sourceIp) {
        try {
            future.join();
            return null;
        } catch (CompletionException e) {
            RuntimeException cause = e.getCause() instanceof RuntimeException re ? re : e;
            log.error("Dynamo {} write failed for sourceIp {}", writerName, sourceIp, cause);
            return cause;
        } catch (RuntimeException e) {
            log.error("Dynamo {} write failed for sourceIp {}", writerName, sourceIp, e);
            return e;
        }
    }
}
