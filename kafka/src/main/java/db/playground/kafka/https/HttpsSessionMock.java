package db.playground.kafka.https;

import java.time.Instant;

public record HttpsSessionMock(
        String sourceIp,
        int sourcePort,
        String destinationIp,
        int destinationPort,
        String domain,
        String method,
        int statusCode,
        long bytesSent,
        long bytesReceived,
        long durationMillis,
        Instant timestamp,
        String timestampIso) {
}
