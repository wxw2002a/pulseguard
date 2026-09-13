package io.pulseguard.api.outbox;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import io.pulseguard.api.transaction.TransactionDocument;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
public class OutboxStore {
    private final MongoTemplate mongo;

    public OutboxStore(MongoTemplate mongo) { this.mongo = mongo; }

    public TransactionDocument claim(Instant now, Duration leaseDuration) {
        Criteria available = new Criteria().orOperator(
                new Criteria().andOperator(Criteria.where("outbox.status").is("PENDING"),
                        Criteria.where("outbox.nextAttemptAt").lte(now)),
                new Criteria().andOperator(Criteria.where("outbox.status").is("IN_FLIGHT"),
                        Criteria.where("outbox.leaseUntil").lte(now)));
        Query query = Query.query(available).with(Sort.by(Sort.Direction.ASC, "ingestedAt", "_id"));
        Update update = new Update().set("outbox.status", "IN_FLIGHT")
                .set("outbox.leaseOwner", UUID.randomUUID().toString())
                .set("outbox.leaseUntil", now.plus(leaseDuration)).inc("outbox.attempts", 1);
        return mongo.findAndModify(query, update, FindAndModifyOptions.options().returnNew(true),
                TransactionDocument.class);
    }

    public boolean markPublished(TransactionDocument claimed, Instant now) {
        Update update = new Update().set("outbox.status", "SENT").set("outbox.publishedAt", now)
                .unset("outbox.leaseOwner").unset("outbox.leaseUntil").unset("outbox.lastError");
        return mongo.updateFirst(ownedLease(claimed), update, TransactionDocument.class).getModifiedCount() == 1;
    }

    public boolean retry(TransactionDocument claimed, Instant now, Throwable exception) {
        // Capped exponential delay keeps an unavailable broker from causing a tight retry loop.
        long delaySeconds = Math.min(60, 1L << Math.min(claimed.outbox().attempts(), 6));
        String error = exception.getClass().getSimpleName() + ": " + String.valueOf(exception.getMessage());
        Update update = new Update().set("outbox.status", "PENDING")
                .set("outbox.nextAttemptAt", now.plusSeconds(delaySeconds))
                .set("outbox.lastError", error.substring(0, Math.min(error.length(), 500)))
                .unset("outbox.leaseOwner").unset("outbox.leaseUntil");
        return mongo.updateFirst(ownedLease(claimed), update, TransactionDocument.class).getModifiedCount() == 1;
    }

    private Query ownedLease(TransactionDocument claimed) {
        // A callback from an expired/reassigned lease must never overwrite the new owner's state.
        return Query.query(Criteria.where("_id").is(claimed.id())
                .and("outbox.status").is("IN_FLIGHT")
                .and("outbox.leaseOwner").is(claimed.outbox().leaseOwner()));
    }
}
