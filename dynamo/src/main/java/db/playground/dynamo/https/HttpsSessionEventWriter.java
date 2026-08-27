package db.playground.dynamo.https;

import java.util.concurrent.CompletableFuture;

public interface HttpsSessionEventWriter {

    CompletableFuture<Void> put(HttpsSessionEvent event);
}
