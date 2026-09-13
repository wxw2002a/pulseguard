package io.pulseguard.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import io.pulseguard.api.config.PulseGuardProperties;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(PulseGuardProperties.class)
public class PulseGuardApplication {
    public static void main(String[] args) {
        SpringApplication.run(PulseGuardApplication.class, args);
    }
}
