package db.playground.dynamo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@ConfigurationPropertiesScan(basePackages = "db.playground.dynamo")
@EnableKafkaStreams
public class DynamoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DynamoApplication.class, args);
    }
}
