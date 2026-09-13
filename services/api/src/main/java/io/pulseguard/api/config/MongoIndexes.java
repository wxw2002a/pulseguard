package io.pulseguard.api.config;

import io.pulseguard.api.transaction.TransactionDocument;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.stereotype.Component;

@Component
public class MongoIndexes implements ApplicationRunner {
    private final MongoTemplate mongo;
    public MongoIndexes(MongoTemplate mongo) { this.mongo = mongo; }

    @Override
    public void run(ApplicationArguments arguments) {
        var transactions = mongo.indexOps(TransactionDocument.class);
        transactions.createIndex(new Index().on("ingestedAt", Sort.Direction.DESC).on("_id", Sort.Direction.DESC));
        transactions.createIndex(new Index().on("payload.accountId", Sort.Direction.ASC).on("ingestedAt", Sort.Direction.DESC).on("_id", Sort.Direction.DESC));
        transactions.createIndex(new Index().on("payload.accountId", Sort.Direction.ASC).on("payload.currency", Sort.Direction.ASC)
                .on("eventTimeDate", Sort.Direction.ASC).on("_id", Sort.Direction.ASC));
        transactions.createIndex(new Index().on("outbox.status", Sort.Direction.ASC).on("outbox.nextAttemptAt", Sort.Direction.ASC).on("ingestedAt", Sort.Direction.ASC));
        transactions.createIndex(new Index().on("outbox.status", Sort.Direction.ASC).on("outbox.leaseUntil", Sort.Direction.ASC).on("ingestedAt", Sort.Direction.ASC));
        mongo.indexOps("alerts").createIndex(new Index().on("createdAt", Sort.Direction.DESC).on("_id", Sort.Direction.DESC));
        mongo.indexOps("alerts").createIndex(new Index().on("accountId", Sort.Direction.ASC).on("createdAt", Sort.Direction.DESC));
        mongo.indexOps("alerts").createIndex(new Index().on("severity", Sort.Direction.ASC).on("createdAt", Sort.Direction.DESC));
        mongo.indexOps("windows").createIndex(new Index().on("windowEnd", Sort.Direction.DESC).on("_id", Sort.Direction.DESC));
        mongo.indexOps("windows").createIndex(new Index().on("accountId", Sort.Direction.ASC).on("windowEnd", Sort.Direction.DESC));
    }
}
