package db.playground.dynamo.https;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;

import java.net.URI;

@Configuration
public class HttpsSessionDynamoConfig {

    @Bean
    public DynamoDbAsyncClient dynamoDbAsyncClient(HttpsSessionDynamoProperties properties) {
        HttpsSessionDynamoProperties.Dynamo dynamo = properties.dynamo();
        DynamoDbAsyncClientBuilder builder = DynamoDbAsyncClient.builder().region(Region.of(dynamo.region()));

        if (dynamo.endpointOverride() != null && !dynamo.endpointOverride().isBlank()) {
            builder.endpointOverride(URI.create(dynamo.endpointOverride()));
        }
        if (dynamo.accessKeyId() != null && !dynamo.accessKeyId().isBlank()) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(dynamo.accessKeyId(), dynamo.secretAccessKey())));
        }
        return builder.build();
    }

    @Bean
    public HttpsSessionAggregateWriter httpsSessionAggregateWriter(DynamoDbAsyncClient client, HttpsSessionDynamoProperties properties) {
        return new DynamoSessionAggregateWriter(client, properties.dynamo().aggregateTable());
    }

    @Bean
    public HttpsSessionEventWriter httpsSessionEventWriter(DynamoDbAsyncClient client, HttpsSessionDynamoProperties properties) {
        return new DynamoSessionEventWriter(client, properties.dynamo().eventTable());
    }

    @Bean
    public HttpsSessionDynamoTopology httpsSessionDynamoTopology(HttpsSessionAggregateWriter aggregateWriter, HttpsSessionEventWriter eventWriter) {
        return new HttpsSessionDynamoTopology(aggregateWriter, eventWriter);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsUncaughtExceptionHandlerConfigurer() {
        StreamsUncaughtExceptionHandler handler = throwable ->
                StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        return factoryBean -> factoryBean.setStreamsUncaughtExceptionHandler(handler);
    }
}
