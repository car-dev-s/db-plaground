package db.playground.dynamo.https;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpsSessionDynamoTopologyTest {

    @Test
    void attemptsBothWritersAndReturnsNormally_whenBothSucceed() {
        List<String> calls = new ArrayList<>();
        HttpsSessionAggregateWriter aggregateWriter = event -> calls.add("aggregate");
        HttpsSessionEventWriter eventWriter = event -> calls.add("event");
        HttpsSessionDynamoTopology topology = new HttpsSessionDynamoTopology(aggregateWriter, eventWriter);

        topology.writeToBothTables("key", sampleEvent());

        assertThat(calls).containsExactly("aggregate", "event");
    }

    @Test
    void stillAttemptsEventWriter_whenAggregateWriterFails() {
        List<String> calls = new ArrayList<>();
        HttpsSessionAggregateWriter aggregateWriter = event -> {
            calls.add("aggregate");
            throw new RuntimeException("aggregate boom");
        };
        HttpsSessionEventWriter eventWriter = event -> calls.add("event");
        HttpsSessionDynamoTopology topology = new HttpsSessionDynamoTopology(aggregateWriter, eventWriter);

        assertThatThrownBy(() -> topology.writeToBothTables("key", sampleEvent()))
                .isInstanceOf(HttpsSessionDynamoWriteException.class);
        assertThat(calls).containsExactly("aggregate", "event");
    }

    @Test
    void stillAttemptsAggregateWriter_whenEventWriterFails() {
        List<String> calls = new ArrayList<>();
        HttpsSessionAggregateWriter aggregateWriter = event -> calls.add("aggregate");
        HttpsSessionEventWriter eventWriter = event -> {
            calls.add("event");
            throw new RuntimeException("event boom");
        };
        HttpsSessionDynamoTopology topology = new HttpsSessionDynamoTopology(aggregateWriter, eventWriter);

        assertThatThrownBy(() -> topology.writeToBothTables("key", sampleEvent()))
                .isInstanceOf(HttpsSessionDynamoWriteException.class);
        assertThat(calls).containsExactly("aggregate", "event");
    }

    private static HttpsSessionEvent sampleEvent() {
        HttpsSessionEvent event = new HttpsSessionEvent();
        event.setSourceIp("203.0.113.5");
        event.setSourcePort(51000);
        event.setDestinationIp("93.0.0.1");
        event.setDestinationPort(443);
        event.setDomain("example.com");
        event.setMethod("GET");
        event.setStatusCode(200);
        event.setBytesSent(500);
        event.setBytesReceived(1000);
        event.setDurationMillis(100);
        Instant now = Instant.now();
        event.setTimestamp(now);
        event.setTimestampIso(now.toString());
        return event;
    }
}
