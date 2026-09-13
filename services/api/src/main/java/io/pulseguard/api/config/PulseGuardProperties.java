package io.pulseguard.api.config;

import java.time.Duration;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "pulseguard")
public record PulseGuardProperties(@NotBlank String apiKey, @NotBlank String transactionTopic,
                                  @Valid @NotNull Outbox outbox) {
    public record Outbox(boolean enabled, @Min(1) long pollIntervalMs, @Min(1) int batchSize,
                         @NotNull Duration leaseDuration) { }
}
