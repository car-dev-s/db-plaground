package db.playground.dynamo.https;

import java.util.concurrent.CompletableFuture;

public interface HttpsSessionAggregateWriter {

    CompletableFuture<Void> update(HttpsSessionEvent event);
}
