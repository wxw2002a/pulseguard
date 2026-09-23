package io.pulseguard.api.investigation;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
            "status", "reviewedAt", "transactionCount", "totalAmountMinor", "version", "owner", "disposition",
            "resolvedAt", "evidenceVersion", "evidenceCount", "evidenceTruncated");
    private static final List<String> WINDOW_FIELDS = List.of("accountId", "currency", "windowStart", "windowEnd",
            "transactionCount", "totalAmountMinor", "highValueCount", "updatedAt");
    private final MongoTemplate mongo;
    private final Clock clock;

    public InvestigationService(MongoTemplate mongo, Clock clock) { this.mongo = mongo; this.clock = clock; }

    public List<Map<String, Object>> alerts(int limit, Severity severity, String accountId) {
        return alertPage(limit, severity, accountId, null, null, null).items();
    }

    public AlertPage alertPage(int limit, Severity severity, String accountId, ReviewStatus status, String owner, String cursor) {
        List<Criteria> filters = new ArrayList<>();
        if (severity != null) filters.add(Criteria.where("severity").is(severity.name()));
        if (accountId != null) filters.add(Criteria.where("accountId").is(accountId));
        if (status == ReviewStatus.OPEN) {
            filters.add(new Criteria().orOperator(Criteria.where("status").is("OPEN"), Criteria.where("status").exists(false)));
        } else if (status != null) filters.add(Criteria.where("status").is(status.name()));
        if (owner != null) filters.add(Criteria.where("owner").is(owner));
        if (cursor != null) filters.add(cursorCriteria(cursor));
        Query query = filters.isEmpty() ? new Query() : Query.query(new Criteria().andOperator(filters));
        query.with(Sort.by(Sort.Direction.DESC, "createdAt", "_id")).limit(limit + 1);
        query.fields().exclude("reviewHistory").exclude("evidenceTransactionIds");
        List<Document> found = mongo.find(query, Document.class, "alerts");
        boolean more = found.size() > limit;
        List<Document> page = found.subList(0, Math.min(limit, found.size()));
        return new AlertPage(page.stream().map(document -> publicDocument(document, ALERT_FIELDS, true)).toList(),
                more ? encodeCursor(page.get(page.size() - 1)) : null);
    }

    public List<Map<String, Object>> windows(int limit, String accountId) {
        return mongo.find(recentQuery("windowEnd", limit, accountId), Document.class, "windows").stream()
                .map(document -> publicDocument(document, WINDOW_FIELDS, false)).toList();
    }

    public Map<String, Object> detail(String id, int historyLimit) { return detailDocument(requireAlert(id), historyLimit); }

    public EvidencePage evidence(String id, int limit) {
        Document alert = requireAlert(id);
        if (Objects.equals(alert.get("evidenceVersion"), 1)) {
            List<String> ids = alert.getList("evidenceTransactionIds", String.class, List.of());
            List<TransactionView> found = ids.isEmpty() ? List.of() : mongo.find(
                    Query.query(Criteria.where("_id").in(ids)), TransactionDocument.class).stream()
                    .map(TransactionView::from).sorted(Comparator.comparing(TransactionView::eventTime)
                            .thenComparing(TransactionView::transactionId)).toList();
            long expected = ((Number) alert.getOrDefault("evidenceCount", ids.size())).longValue();
            long missing = ids.size() - found.size();
            boolean truncated = Boolean.TRUE.equals(alert.get("evidenceTruncated")) || expected > ids.size() || found.size() > limit;
            List<TransactionView> items = found.subList(0, Math.min(limit, found.size()));
            return new EvidencePage(items, "PINNED_DETECTION", expected, found.size(), missing, truncated,
                    !truncated && missing == 0 && expected == items.size());
        }
        String transactionId = alert.getString("transactionId");
        if (transactionId != null) {
            TransactionDocument transaction = mongo.findById(transactionId, TransactionDocument.class);
            List<TransactionView> items = transaction == null ? List.of() : List.of(TransactionView.from(transaction));
            return new EvidencePage(items, "LEGACY_TRANSACTION_CONTEXT", 1, items.size(), transaction == null ? 1 : 0, false, false);
        }
        if (alert.get("windowStart") == null || alert.get("windowEnd") == null) {
            return new EvidencePage(List.of(), "LEGACY_WINDOW_CONTEXT", 0, 0, 0, false, false);
        }
        // Legacy membership was not pinned: later-arriving ledger entries are context only.
        Query query = Query.query(Criteria.where("payload.accountId").is(alert.getString("accountId"))
                .and("payload.currency").is(alert.getString("currency"))
                .and("eventTimeDate").gte(instant(alert.get("windowStart"))).lt(instant(alert.get("windowEnd"))));
        long count = mongo.count(query, TransactionDocument.class);
        List<TransactionView> items = mongo.find(query.with(Sort.by(Sort.Direction.ASC, "eventTimeDate", "_id")).limit(limit),
                TransactionDocument.class).stream().map(TransactionView::from).toList();
        return new EvidencePage(items, "LEGACY_WINDOW_CONTEXT", count, count, 0, count > items.size(), false);
    }

    public Map<String, Object> review(String id, ReviewCommand request) {
        ReviewCommand command = normalize(request);
        Document before = requireAlert(id);
        if (isReplay(before, command)) return detailDocument(before, 50);
        if (version(before) != command.expectedVersion()) throw conflict("This alert changed. Refresh its detail before retrying your decision");
        if (history(before).size() >= MAX_REVIEW_HISTORY) throw conflict("Review history has reached its 500-entry limit; previous notes are preserved");
        ReviewStatus previous = ReviewStatus.valueOf(before.getString("status") == null ? "OPEN" : before.getString("status"));
        String owner = before.getString("owner");
        ReviewStatus next = previous;
        switch (command.action()) {
            case CLAIM -> {
                // Older INVESTIGATING records have no owner and can be explicitly adopted.
                if (owner != null || previous == ReviewStatus.RESOLVED) throw conflict("Only an unowned open investigation can be claimed");
                next = ReviewStatus.INVESTIGATING;
            }
            case COMMENT, RELEASE, RESOLVE -> {
                if (previous != ReviewStatus.INVESTIGATING || !command.analyst().equals(owner))
                    throw conflict("Claim this alert first; only its current owner can comment, release or resolve it");
                if (command.action() == ReviewAction.RELEASE) next = ReviewStatus.OPEN;
                if (command.action() == ReviewAction.RESOLVE) next = ReviewStatus.RESOLVED;
            }
            case REOPEN -> {
                if (previous != ReviewStatus.RESOLVED) throw conflict("Only a resolved alert can be reopened");
                next = ReviewStatus.INVESTIGATING;
            }
        }
        Instant now = clock.instant();
        Document entry = commandDocument(command).append("status", next.name())
                .append("version", command.expectedVersion() + 1).append("reviewedAt", Date.from(now));
        Update update = new Update().set("status", next.name()).set("reviewedAt", now)
                .inc("version", 1L).push("reviewHistory", entry);
        if (command.action() == ReviewAction.CLAIM || command.action() == ReviewAction.REOPEN) update.set("owner", command.analyst());
        if (command.action() == ReviewAction.RELEASE) update.unset("owner");
        if (command.action() == ReviewAction.RESOLVE) update.set("disposition", command.disposition().name()).set("resolvedAt", now);
        if (command.action() == ReviewAction.REOPEN) update.unset("disposition").unset("resolvedAt");
        Criteria expectedVersion = command.expectedVersion() == 0
                ? new Criteria().orOperator(Criteria.where("version").is(0L), Criteria.where("version").exists(false))
                : Criteria.where("version").is(command.expectedVersion());
        Query writable = Query.query(new Criteria().andOperator(Criteria.where("_id").is(id), expectedVersion,
                Criteria.where("reviewHistory." + (MAX_REVIEW_HISTORY - 1)).exists(false),
                Criteria.where("reviewHistory.operationId").ne(command.operationId())));
        // Version, audit capacity and operation identity are guarded by one Mongo document CAS.
        Document changed = mongo.findAndModify(writable, update, FindAndModifyOptions.options().returnNew(true), Document.class, "alerts");
        if (changed == null) {
            Document current = requireAlert(id);
            if (isReplay(current, command)) return detailDocument(current, 50);
            throw conflict("This alert changed. Refresh its detail before retrying your decision");
        }
        return detailDocument(changed, 50);
    }

    public Outcomes outcomes() {
        List<RuleOutcome> rules = new ArrayList<>();
        mongo.getCollection("alerts").aggregate(List.of(
                new Document("$match", new Document("status", "RESOLVED")),
                new Document("$group", new Document("_id", "$rule").append("resolved", new Document("$sum", 1))
                        .append("confirmedRisk", outcomeSum("CONFIRMED_RISK"))
                        .append("falsePositive", outcomeSum("FALSE_POSITIVE")).append("benign", outcomeSum("BENIGN"))),
                new Document("$sort", new Document("_id", 1))))
                .forEach(row -> rules.add(new RuleOutcome(row.getString("_id"), number(row, "resolved"),
                        number(row, "confirmedRisk"), number(row, "falsePositive"), number(row, "benign"),
                        number(row, "resolved") - number(row, "confirmedRisk") - number(row, "falsePositive") - number(row, "benign"))));
        return new Outcomes(rules.stream().mapToLong(RuleOutcome::resolved).sum(), rules);
    }

    public Overview overview() {
        Map<String, Long> volume = new LinkedHashMap<>();
        for (TransactionPayload.Currency currency : TransactionPayload.Currency.values()) volume.put(currency.name(), 0L);
        mongo.getCollection("transactions").aggregate(List.of(new Document("$group", new Document("_id", "$payload.currency")
                        .append("amountMinor", new Document("$sum", "$payload.amountMinor")))))
                .forEach(document -> volume.put(document.getString("_id"), ((Number) document.get("amountMinor")).longValue()));
        return new Overview(mongo.count(new Query(), TransactionDocument.class), mongo.count(new Query(), "alerts"),
                mongo.count(Query.query(Criteria.where("severity").in("HIGH", "CRITICAL").and("status").ne("RESOLVED")), "alerts"),
                volume, mongo.count(Query.query(Criteria.where("outbox.status").ne("SENT")), TransactionDocument.class),
                mongo.count(Query.query(Criteria.where("status").in("INVESTIGATING", "RESOLVED")), "alerts"));
    }

    private ReviewCommand normalize(ReviewCommand command) {
        if (command == null || command.action() == null || command.expectedVersion() == null || command.expectedVersion() < 0
                || command.expectedVersion() == Long.MAX_VALUE || command.operationId() == null
                || !command.operationId().matches("[A-Za-z0-9_-]{8,80}")
                || command.note() == null || command.note().strip().length() < 3 || command.note().length() > 1000
                || command.analyst() == null || command.analyst().strip().length() < 2 || command.analyst().length() > 64)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Action, expectedVersion, operationId, review note (3-1000) and analyst label (2-64) are required");
        if ((command.action() == ReviewAction.RESOLVE) != (command.disposition() != null))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "A disposition is required only when resolving an alert");
        return new ReviewCommand(command.action(), command.expectedVersion(), command.operationId(),
                command.note().strip(), command.analyst().strip(), command.disposition());
    }

    private boolean isReplay(Document alert, ReviewCommand command) {
        for (Document entry : history(alert)) {
            if (!command.operationId().equals(entry.getString("operationId"))) continue;
            boolean same = commandDocument(command).entrySet().stream()
                    .allMatch(field -> Objects.equals(field.getValue(), entry.get(field.getKey())));
            if (!same) throw conflict("operationId already belongs to a different review command");
            return true;
        }
        return false;
    }

    private Document commandDocument(ReviewCommand command) {
        return new Document("action", command.action().name()).append("expectedVersion", command.expectedVersion())
                .append("operationId", command.operationId()).append("note", command.note()).append("analyst", command.analyst())
                .append("disposition", command.disposition() == null ? null : command.disposition().name());
    }
    private List<Document> history(Document document) { return document.getList("reviewHistory", Document.class, List.of()); }
    private long version(Document document) { return number(document, "version"); }
    private long number(Document document, String field) { return ((Number) document.getOrDefault(field, 0L)).longValue(); }
    private ResponseStatusException conflict(String reason) { return new ResponseStatusException(HttpStatus.CONFLICT, reason); }
    private Document outcomeSum(String disposition) {
        return new Document("$sum", new Document("$cond", List.of(new Document("$eq", List.of("$disposition", disposition)), 1, 0)));
    }

    private Criteria cursorCriteria(String cursor) {
        try {
            String[] value = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8).split("\n", 2);
            if (value.length != 2 || value[1].isBlank() || value[1].length() > 256) throw new IllegalArgumentException();
            Instant time = Instant.ofEpochMilli(Long.parseLong(value[0]));
            return new Criteria().orOperator(Criteria.where("createdAt").lt(time),
                    Criteria.where("createdAt").is(time).and("_id").lt(value[1]));
        } catch (RuntimeException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid alert cursor");
        }
    }
    private String encodeCursor(Document document) {
        String value = instant(document.get("createdAt")).toEpochMilli() + "\n" + document.getString("_id");
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
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
        List<Document> history = history(document);
        List<Map<String, Object>> recent = new ArrayList<>();
        for (int index = Math.max(0, history.size() - historyLimit); index < history.size(); index++) {
            Document entry = history.get(index);
            Map<String, Object> item = new LinkedHashMap<>();
            for (String field : List.of("status", "note", "analyst", "reviewedAt", "action", "operationId", "version", "disposition")) {
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
        if (alert) { result.putIfAbsent("status", ReviewStatus.OPEN.name()); result.putIfAbsent("version", 0L); }
        return result;
    }

    public enum Severity { LOW, MEDIUM, HIGH, CRITICAL }
    public enum ReviewStatus { OPEN, INVESTIGATING, RESOLVED }
    public enum ReviewAction { CLAIM, COMMENT, RELEASE, RESOLVE, REOPEN }
    public enum Disposition { CONFIRMED_RISK, FALSE_POSITIVE, BENIGN }
    public record ReviewCommand(ReviewAction action, Long expectedVersion, String operationId, String note, String analyst, Disposition disposition) { }
    public record AlertPage(List<Map<String, Object>> items, String nextCursor) { }
    public record EvidencePage(List<TransactionView> items, String provenance, long evidenceCount, long matchedCount,
                               long missingCount, boolean truncated, boolean complete) { }
    public record RuleOutcome(String rule, long resolved, long confirmedRisk, long falsePositive, long benign, long unclassified) { }
    public record Outcomes(long resolvedAlerts, List<RuleOutcome> byRule) { }
    public record Overview(long transactions, long alerts, long highRisk, Map<String, Long> volumeByCurrency,
                           long pendingDelivery, long reviewedAlerts) { }
}
