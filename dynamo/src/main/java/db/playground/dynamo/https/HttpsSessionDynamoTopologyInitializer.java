package db.playground.dynamo.https;

import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.stereotype.Component;

@Component
public class HttpsSessionDynamoTopologyInitializer {

    public HttpsSessionDynamoTopologyInitializer(StreamsBuilder streamsBuilder,
                                                   HttpsSessionDynamoTopology topology,
                                                   HttpsSessionDynamoProperties properties) {
        topology.build(streamsBuilder, properties.topic());
    }
}
