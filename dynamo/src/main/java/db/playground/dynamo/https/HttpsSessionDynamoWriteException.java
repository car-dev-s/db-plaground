package db.playground.dynamo.https;

public class HttpsSessionDynamoWriteException extends RuntimeException {

    public HttpsSessionDynamoWriteException(String sourceIp, Exception aggregateFailure, Exception eventFailure) {
        super("Dynamo write failed for sourceIp " + sourceIp
                        + " (aggregateFailed=" + (aggregateFailure != null)
                        + ", eventFailed=" + (eventFailure != null) + ")",
                aggregateFailure != null ? aggregateFailure : eventFailure);
        if (aggregateFailure != null && eventFailure != null) {
            addSuppressed(eventFailure);
        }
    }
}
