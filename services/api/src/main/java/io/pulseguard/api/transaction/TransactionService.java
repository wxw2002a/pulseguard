package io.pulseguard.api.transaction;

import java.time.Clock;
import java.util.List;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class TransactionService {
    private final MongoTemplate mongo;
    private final Clock clock;
    private final Counter accepted;
    private final Counter duplicates;

    public TransactionService(MongoTemplate mongo, Clock clock, MeterRegistry registry) {
        this.mongo = mongo;
        this.clock = clock;
        accepted = registry.counter("pulseguard.transactions.accepted");
        duplicates = registry.counter("pulseguard.transactions.duplicates");
    }

    public IngestionResult accept(TransactionPayload payload) {
        try {
            Math.addExact(Math.multiplyExact(payload.eventTime().getEpochSecond(), 1_000_000L),
                    payload.eventTime().getNano() / 1_000L);
        } catch (ArithmeticException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "eventTime is outside the supported timestamp range");
        }
        if (payload.eventTime().isAfter(clock.instant().plusSeconds(30))) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "eventTime must not exceed server time by more than 30 seconds");
        }
        // A single Mongo insert atomically persists business data and its delivery intent.
        // The built-in _id unique index is the concurrency-safe idempotency boundary.
        try {
            mongo.insert(TransactionDocument.create(payload, clock.instant()));
            accepted.increment();
            return new IngestionResult(payload.transactionId(), "ACCEPTED", false);
        } catch (DuplicateKeyException exception) {
            TransactionDocument existing = mongo.findById(payload.transactionId(), TransactionDocument.class);
            if (existing != null && existing.payload().equals(payload)) {
                duplicates.increment();
                return new IngestionResult(payload.transactionId(), "ACCEPTED", true);
            }
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "transactionId already exists with a different payload");
        }
    }

    public List<TransactionView> recent(int limit, String accountId) {
        Query query = new Query().with(Sort.by(Sort.Direction.DESC, "ingestedAt", "_id")).limit(limit);
        if (accountId != null) query.addCriteria(Criteria.where("payload.accountId").is(accountId));
        return mongo.find(query, TransactionDocument.class).stream().map(TransactionView::from).toList();
    }

    public record IngestionResult(String transactionId, String status, boolean duplicate) { }
}
