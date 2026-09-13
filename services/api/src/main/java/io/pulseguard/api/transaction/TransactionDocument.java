package io.pulseguard.api.transaction;

import java.time.Instant;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.data.mongodb.core.mapping.MongoId;

@Document("transactions")
public record TransactionDocument(@MongoId(FieldType.STRING) String id, TransactionPayload payload, Instant ingestedAt,
                                  Instant eventTimeDate, Outbox outbox) {
    public record Outbox(String status, int attempts, Instant nextAttemptAt, String leaseOwner,
                         Instant leaseUntil, Instant publishedAt, String lastError) {
        public static Outbox pending(Instant now) {
            return new Outbox("PENDING", 0, now, null, null, null, null);
        }
    }
    public static TransactionDocument create(TransactionPayload payload, Instant now) {
        return new TransactionDocument(payload.transactionId(), payload, now, payload.eventTime(), Outbox.pending(now));
    }
}
