package io.pulseguard.streaming;

import java.io.Serializable;
import java.time.Instant;

/** Immutable money amounts use minor units, never floating point. */
public record TransactionEvent(String transactionId, String accountId, String merchantId,
                               long amountMinor, String currency, String country,
                               String channel, Instant eventTime) implements Serializable {
}
