package db.playground.dynamo;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

// auto-startup disabled: Kafka Streams validates the topology eagerly on start, and no
// topology is defined until HttpsSessionDynamoTopology exists (later task)
@SpringBootTest(properties = "spring.kafka.streams.auto-startup=false")
class DynamoApplicationTest {

    @Test
    void contextLoads() {
    }
}
