package io.pulseguard.api.transaction;

import java.time.Instant;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.data.convert.ValueConverter;

public record TransactionPayload(
        @NotNull @Min(1) @Max(1) Integer schemaVersion,
        @NotBlank @Size(max = 64) @Pattern(regexp = "[A-Za-z0-9_-]+") String transactionId,
        @NotBlank @Size(max = 64) @Pattern(regexp = "[A-Za-z0-9_-]+") String accountId,
        @NotBlank @Size(max = 64) @Pattern(regexp = "[A-Za-z0-9_-]+") String merchantId,
        @NotNull @Min(1) @Max(100_000_000_000L) Long amountMinor,
        @NotNull Currency currency,
        @NotBlank @Pattern(regexp = "[A-Z]{2}") String country,
        @NotNull Channel channel,
        @NotNull @ValueConverter(EventTimeConverter.class) Instant eventTime) {
    public enum Currency { USD, CAD, EUR, GBP }
    public enum Channel { WEB, MOBILE, POS }
}
