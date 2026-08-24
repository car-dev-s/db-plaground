package db.playground.kafka.https;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class HttpsSessionLoadRunnerTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class);

    @Test
    void runnerIsAbsentWithoutLoadHttpsSessionsProfile() {
        contextRunner.run(context -> assertThat(context).doesNotHaveBean(HttpsSessionLoadRunner.class));
    }

    @Test
    void runnerIsPresentWithLoadHttpsSessionsProfile() {
        contextRunner.withPropertyValues("spring.profiles.active=load-https-sessions")
                .run(context -> assertThat(context).hasSingleBean(HttpsSessionLoadRunner.class));
    }

    @Configuration
    @ComponentScan(
            basePackageClasses = HttpsSessionLoadRunner.class,
            useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = HttpsSessionLoadRunner.class))
    static class TestConfig {

        @Bean
        HttpsSessionProducer httpsSessionProducer() {
            return mock(HttpsSessionProducer.class);
        }
    }
}
