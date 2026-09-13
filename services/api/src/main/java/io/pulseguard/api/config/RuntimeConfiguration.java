package io.pulseguard.api.config;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

@Configuration
public class RuntimeConfiguration {
    @Bean
    Clock clock() { return Clock.systemUTC(); }

    @Bean(destroyMethod = "close")
    AdminClient kafkaHealthClient(KafkaProperties properties) {
        return AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers(),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 2000,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 2000));
    }

    @Bean("kafkaHealthIndicator")
    HealthIndicator kafkaHealthIndicator(AdminClient kafkaHealthClient) {
        return () -> {
            try {
                kafkaHealthClient.describeCluster().nodes().get(2, TimeUnit.SECONDS);
                return Health.up().build();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return Health.down().build();
            } catch (Exception exception) {
                return Health.down().build();
            }
        };
    }

    @Bean
    ApplicationRunner validateApiKey(PulseGuardProperties properties, Environment environment) {
        return args -> {
            if (properties.apiKey().equals("local-dev-key")) {
                if (environment.acceptsProfiles(Profiles.of("prod"))) {
                    throw new IllegalStateException("Set a non-default INGEST_API_KEY for the prod profile");
                }
                LoggerFactory.getLogger(RuntimeConfiguration.class)
                        .warn("Local demo API key is active. Set INGEST_API_KEY before sharing the service.");
            }
        };
    }
}
