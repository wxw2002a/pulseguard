package io.pulseguard.api.investigation;

import java.time.Clock;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import io.pulseguard.api.transaction.TransactionDocument;
import io.pulseguard.api.transaction.TransactionPayload;

@Service
public class InvestigationService {
    private static final List<String> ALERT_FIELDS = List.of("transactionId", "accountId", "currency", "rule",
            "severity", "score", "reasons", "amountMinor", "windowStart", "windowEnd", "eventTime", "createdAt",
            "status", "reviewedAt", "transactionCount", "totalAmountMinor");
    private static final List<String> WINDOW_FIELDS = List.of("accountId", "currency", "windowStart", "windowEnd",
            "transactionCount", "totalAmountMinor", "highValueCount", "updatedAt");
    private final MongoTemplate mongo;
    private final Clock clock;

    public InvestigationService(MongoTemplate mongo, Clock clock) {
        this.mongo = mongo;
        this.clock = clock;
    }

    public List<Map<String, Object>> alerts(int limit, Severity severity, String accountId) {
        Query query = recentQuery("createdAt", limit, accountId);
        if (severity != null) query.addCriteria(Criteria.where("severity").is(severity.name()));
        return mongo.find(query, Document.class, "alerts").stream()
                .map(document -> publicDocument(document, ALERT_FIELDS, true)).toList();
    }

    public List<Map<String, Object>> windows(int limit, String accountId) {
        return mongo.find(recentQuery("windowEnd", limit, accountId), Document.class, "windows").stream()
                .map(document -> publicDocument(document, WINDOW_FIELDS, false)).toList();
    }

    public Map<String, Object> review(String id, ReviewStatus status) {
        Document document = mongo.findAndModify(Query.query(Criteria.where("_id").is(id)),
                new Update().set("status", status.name()).set("reviewedAt", clock.instant()),
                FindAndModifyOptions.options().returnNew(true), Document.class, "alerts");
        if (document == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Alert not found");
        return publicDocument(document, ALERT_FIELDS, true);
    }

    public Overview overview() {
        Map<String, Long> volume = new LinkedHashMap<>();
        for (TransactionPayload.Currency currency : TransactionPayload.Currency.values()) volume.put(currency.name(), 0L);
        mongo.getCollection("transactions").aggregate(List.of(new Document("$group", new Document("_id", "$payload.currency")
                        .append("amountMinor", new Document("$sum", "$payload.amountMinor")))))
                .forEach(document -> volume.put(document.getString("_id"), ((Number) document.get("amountMinor")).longValue()));
        return new Overview(mongo.count(new Query(), TransactionDocument.class),
                mongo.count(new Query(), "alerts"),
                mongo.count(Query.query(Criteria.where("severity").in("HIGH", "CRITICAL").and("status").ne("RESOLVED")), "alerts"),
                volume,
                mongo.count(Query.query(Criteria.where("outbox.status").ne("SENT")), TransactionDocument.class),
                mongo.count(Query.query(Criteria.where("status").in("INVESTIGATING", "RESOLVED")), "alerts"));
    }

    private Query recentQuery(String dateField, int limit, String accountId) {
        Query query = new Query().with(Sort.by(Sort.Direction.DESC, dateField, "_id")).limit(limit);
        if (accountId != null) query.addCriteria(Criteria.where("accountId").is(accountId));
        return query;
    }

    private Map<String, Object> publicDocument(Document document, List<String> fields, boolean alert) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", document.get("_id").toString());
        for (String field : fields) {
            if (document.containsKey(field)) {
                Object value = document.get(field);
                result.put(field, value instanceof Date date ? date.toInstant() : value);
            }
        }
        if (alert) result.putIfAbsent("status", ReviewStatus.OPEN.name());
        return result;
    }

    public enum Severity { LOW, MEDIUM, HIGH, CRITICAL }
    public enum ReviewStatus { OPEN, INVESTIGATING, RESOLVED }
    public record Overview(long transactions, long alerts, long highRisk, Map<String, Long> volumeByCurrency,
                           long pendingDelivery, long reviewedAlerts) { }
}
