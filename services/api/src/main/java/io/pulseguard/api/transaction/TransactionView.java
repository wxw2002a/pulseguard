package io.pulseguard.api.transaction;

import java.time.Instant;

public record TransactionView(int schemaVersion, String transactionId, String accountId,
                              String merchantId, long amountMinor, TransactionPayload.Currency currency,
                              String country, TransactionPayload.Channel channel, Instant eventTime,
                              Instant ingestedAt, String deliveryStatus) {
    public static TransactionView from(TransactionDocument document) {
        TransactionPayload payload = document.payload();
        return new TransactionView(payload.schemaVersion(), payload.transactionId(), payload.accountId(),
                payload.merchantId(), payload.amountMinor(), payload.currency(), payload.country(),
                payload.channel(), payload.eventTime(), document.ingestedAt(), document.outbox().status());
    }
}
