package io.pulseguard.api.investigation;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
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
import io.pulseguard.api.transaction.TransactionView;

@Service
public class InvestigationService {
    public static final int MAX_REVIEW_HISTORY = 500;
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
        query.fields().exclude("reviewHistory");
        return mongo.find(query, Document.class, "alerts").stream()
                .map(document -> publicDocument(document, ALERT_FIELDS, true)).toList();
    }

    public List<Map<String, Object>> windows(int limit, String accountId) {
        return mongo.find(recentQuery("windowEnd", limit, accountId), Document.class, "windows").stream()
                .map(document -> publicDocument(document, WINDOW_FIELDS, false)).toList();
    }

    public Map<String, Object> detail(String id, int historyLimit) {
        return detailDocument(requireAlert(id), historyLimit);
    }

    public List<TransactionView> evidence(String id, int limit) {
        Document alert = requireAlert(id);
        String transactionId = alert.getString("transactionId");
        if (transactionId != null) {
            TransactionDocument transaction = mongo.findById(transactionId, TransactionDocument.class);
            return transaction == null ? List.of() : List.of(TransactionView.from(transaction));
        }
        if (alert.get("windowStart") == null || alert.get("windowEnd") == null) return List.of();
        // UTC minute boundaries are exact milliseconds. Keep the full original ISO payload
        // for equality, and query its indexed BSON date projection for investigation evidence.
        Query query = Query.query(Criteria.where("payload.accountId").is(alert.getString("accountId"))
                .and("payload.currency").is(alert.getString("currency"))
                .and("eventTimeDate").gte(instant(alert.get("windowStart"))).lt(instant(alert.get("windowEnd"))))
                .with(Sort.by(Sort.Direction.ASC, "eventTimeDate", "_id")).limit(limit);
        return mongo.find(query, TransactionDocument.class).stream().map(TransactionView::from).toList();
    }

    public Map<String, Object> review(String id, ReviewStatus status, String note, String analyst) {
        // Recheck normalized lengths for direct callers and prevent whitespace-only audit notes.
        if (note == null || note.strip().length() < 3 || note.length() > 1000
                || analyst == null || analyst.strip().length() < 2 || analyst.length() > 64) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "A review note (3-1000 characters) and analyst label (2-64 characters) are required");
        }
        Instant now = clock.instant();
        Document historyEntry = new Document("status", status.name()).append("note", note.strip())
                .append("analyst", analyst.strip()).append("reviewedAt", Date.from(now));
        // The capacity predicate and append execute in the same atomic document update:
        // concurrent analysts retain every accepted note and cannot exceed the audit cap.
        Query writable = Query.query(Criteria.where("_id").is(id)
                .and("reviewHistory." + (MAX_REVIEW_HISTORY - 1)).exists(false));
        Document document = mongo.findAndModify(writable,
                new Update().set("status", status.name()).set("reviewedAt", now).push("reviewHistory", historyEntry),
                FindAndModifyOptions.options().returnNew(true), Document.class, "alerts");
        if (document == null) {
            if (!mongo.exists(Query.query(Criteria.where("_id").is(id)), "alerts")) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Alert not found");
            }
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Review history has reached its 500-entry limit; previous notes are preserved");
        }
        return detailDocument(document, 50);
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

    private Document requireAlert(String id) {
        Document document = mongo.findById(id, Document.class, "alerts");
        if (document == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Alert not found");
        return document;
    }

    private Map<String, Object> detailDocument(Document document, int historyLimit) {
        Map<String, Object> result = publicDocument(document, ALERT_FIELDS, true);
        List<?> history = document.getList("reviewHistory", Document.class, List.of());
        List<Map<String, Object>> recent = new ArrayList<>();
        for (int index = Math.max(0, history.size() - historyLimit); index < history.size(); index++) {
            Document entry = (Document) history.get(index);
            Map<String, Object> item = new LinkedHashMap<>();
            for (String field : List.of("status", "note", "analyst", "reviewedAt")) {
                Object value = entry.get(field);
                item.put(field, value instanceof Date date ? date.toInstant() : value);
            }
            recent.add(item);
        }
        result.put("reviewHistory", recent);
        result.put("reviewHistoryCount", history.size());
        return result;
    }

    private Instant instant(Object value) {
        if (value instanceof Date date) return date.toInstant();
        if (value instanceof Instant timestamp) return timestamp;
        return Instant.parse(value.toString());
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
