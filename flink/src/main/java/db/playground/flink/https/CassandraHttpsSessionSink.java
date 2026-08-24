package db.playground.flink.https;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.net.InetSocketAddress;

public class CassandraHttpsSessionSink implements Sink<HttpsSessionEvent> {

    private final String contactPoint;
    private final int port;
    private final String localDatacenter;
    private final String keyspace;

    public CassandraHttpsSessionSink(String contactPoint, int port, String localDatacenter, String keyspace) {
        this.contactPoint = contactPoint;
        this.port = port;
        this.localDatacenter = localDatacenter;
        this.keyspace = keyspace;
    }

    @Override
    public SinkWriter<HttpsSessionEvent> createWriter(WriterInitContext context) {
        return new CassandraSinkWriter(contactPoint, port, localDatacenter, keyspace);
    }

    private static class CassandraSinkWriter implements SinkWriter<HttpsSessionEvent> {

        private static final String INSERT_CQL = """
                INSERT INTO https_sessions
                (source_ip, timestamp, source_port, destination_ip, destination_port,
                 domain, method, status_code, bytes_sent, bytes_received, duration_millis, timestamp_iso)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        private final CqlSession session;
        private final PreparedStatement insertStatement;

        CassandraSinkWriter(String contactPoint, int port, String localDatacenter, String keyspace) {
            session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(contactPoint, port))
                    .withLocalDatacenter(localDatacenter)
                    .withKeyspace(keyspace)
                    .build();
            insertStatement = session.prepare(INSERT_CQL);
        }

        @Override
        public void write(HttpsSessionEvent event, Context context) {
            BoundStatement bound = insertStatement.bind(
                    event.getSourceIp(),
                    event.getTimestamp(),
                    event.getSourcePort(),
                    event.getDestinationIp(),
                    event.getDestinationPort(),
                    event.getDomain(),
                    event.getMethod(),
                    event.getStatusCode(),
                    event.getBytesSent(),
                    event.getBytesReceived(),
                    event.getDurationMillis(),
                    event.getTimestampIso());
            session.execute(bound);
        }

        @Override
        public void flush(boolean endOfInput) {
        }

        @Override
        public void close() {
            session.close();
        }
    }
}
