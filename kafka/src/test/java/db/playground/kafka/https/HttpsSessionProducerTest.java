package db.playground.kafka.https;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class HttpsSessionProducerTest {

    @Test
    void sendsConfiguredNumberOfSessionsToConfiguredTopic() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, HttpsSessionMock> kafkaTemplate = mock(KafkaTemplate.class);
        HttpsSessionProperties properties = new HttpsSessionProperties("test-https-topic", 5);
        HttpsSessionProducer producer = new HttpsSessionProducer(kafkaTemplate, properties);

        producer.loadSessions();

        verify(kafkaTemplate, times(5)).send(eq("test-https-topic"), any(), any(HttpsSessionMock.class));
    }

    @Test
    void generatesSessionsThatLookLikeRealHttpsTraffic() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, HttpsSessionMock> kafkaTemplate = mock(KafkaTemplate.class);
        HttpsSessionProperties properties = new HttpsSessionProperties("test-https-topic", 50);
        HttpsSessionProducer producer = new HttpsSessionProducer(kafkaTemplate, properties);

        producer.loadSessions();

        var captor = org.mockito.ArgumentCaptor.forClass(HttpsSessionMock.class);
        verify(kafkaTemplate, times(50)).send(eq("test-https-topic"), any(), captor.capture());

        assertThat(captor.getAllValues()).allSatisfy(session -> {
            assertThat(session.sourceIp()).matches("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$");
            assertThat(session.destinationIp()).matches("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$");
            assertThat(session.sourcePort()).isBetween(49152, 65535);
            assertThat(session.destinationPort()).isIn(443, 8443);
            assertThat(session.bytesSent()).isPositive();
            assertThat(session.bytesReceived()).isPositive();
            assertThat(session.durationMillis()).isPositive();
            assertThat(session.timestamp()).isNotNull();
        });
    }
}
