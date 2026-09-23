package io.pulseguard.api.investigation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.web.server.ResponseStatusException;
import io.pulseguard.api.transaction.TransactionDocument;
import io.pulseguard.api.transaction.TransactionPayload;
import static io.pulseguard.api.investigation.InvestigationService.*;

class InvestigationServiceTest {
    private final Instant now = Instant.parse("2026-09-13T12:00:00Z");
    private final MongoTemplate mongo = mock(MongoTemplate.class);
    private final InvestigationService service = new InvestigationService(mongo, Clock.fixed(now, ZoneOffset.UTC));

    @Test
    void claimUsesVersionCapacityAndOperationGuardsAndNormalizesLabels() {
        Document before = new Document("_id", "alert-1");
        when(mongo.findById("alert-1", Document.class, "alerts")).thenReturn(before);
        when(mongo.findAndModify(any(Query.class), any(Update.class), any(FindAndModifyOptions.class), eq(Document.class), eq("alerts")))
                .thenReturn(new Document("_id", "alert-1").append("version", 1L).append("owner", "alice").append("status", "INVESTIGATING"));
        var result = service.review("alert-1", new ReviewCommand(ReviewAction.CLAIM, 0L, "operation-1", " Checked evidence ", " alice ", null));
        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> update = ArgumentCaptor.forClass(Update.class);
        verify(mongo).findAndModify(query.capture(), update.capture(), any(FindAndModifyOptions.class), eq(Document.class), eq("alerts"));
        String condition = query.getValue().getQueryObject().toJson();
        assertThat(condition).contains("reviewHistory.499", "operation-1", "version", "$exists");
        Document pushed = update.getValue().getUpdateObject().get("$push", Document.class).get("reviewHistory", Document.class);
        assertThat(pushed).containsEntry("note", "Checked evidence").containsEntry("analyst", "alice").containsEntry("version", 1L);
        assertThat(update.getValue().getUpdateObject().get("$set", Document.class)).containsEntry("owner", "alice");
        assertThat(result).containsEntry("version", 1L).containsEntry("status", "INVESTIGATING");
    }

    @Test
    void wrongOwnerAndStaleVersionNeverReachUpdate() {
        when(mongo.findById("alert-1", Document.class, "alerts")).thenReturn(owned());
        rejects(409, () -> service.review("alert-1", command(ReviewAction.COMMENT, 1, "bob", null)));
        rejects(409, () -> service.review("alert-1", command(ReviewAction.COMMENT, 0, "alice", null)));
        verify(mongo, never()).findAndModify(any(), any(Update.class), any(), eq(Document.class), eq("alerts"));
    }

    @Test
    void resolveRequiresOutcomeAndOnlyResolutionAcceptsOutcome() {
        rejects(400, () -> service.review("alert-1", command(ReviewAction.RESOLVE, 1, "alice", null)));
        rejects(400, () -> service.review("alert-1", command(ReviewAction.COMMENT, 1, "alice", Disposition.BENIGN)));
        rejects(400, () -> service.review("alert-1", new ReviewCommand(ReviewAction.CLAIM, null, "operation-1", "note", "alice", null)));
        rejects(400, () -> service.review("alert-1", new ReviewCommand(ReviewAction.CLAIM, 0L, "operation-1", "   ", "alice", null)));
        verifyNoInteractions(mongo);
    }

    @Test
    void sameCommandRetryReturnsCurrentStateButOperationCollisionConflicts() {
        Document history = new Document("action", "CLAIM").append("expectedVersion", 0L).append("operationId", "operation-1")
                .append("note", "Checked evidence").append("analyst", "alice").append("disposition", null);
        when(mongo.findById("alert-1", Document.class, "alerts")).thenReturn(owned().append("version", 4L).append("reviewHistory", List.of(history)));
        var result = service.review("alert-1", command(ReviewAction.CLAIM, 0, "alice", null));
        assertThat(result).containsEntry("version", 4L).containsEntry("reviewHistoryCount", 1);
        rejects(409, () -> service.review("alert-1", command(ReviewAction.CLAIM, 0, "bob", null)));
        verify(mongo, never()).findAndModify(any(), any(Update.class), any(), eq(Document.class), eq("alerts"));
    }

    @Test
    void losingCompareAndSetDoesNotSilentlyOverwrite() {
        when(mongo.findById("alert-1", Document.class, "alerts")).thenReturn(owned(), owned().append("version", 2L));
        rejects(409, () -> service.review("alert-1", command(ReviewAction.COMMENT, 1, "alice", null)));
    }

    @Test
    void fullHistoryCannotBeExtended() {
        when(mongo.findById("alert-1", Document.class, "alerts")).thenReturn(owned().append("reviewHistory", java.util.Collections.nCopies(500, new Document())));
        rejects(409, () -> service.review("alert-1", command(ReviewAction.COMMENT, 1, "alice", null)));
        verify(mongo, never()).findAndModify(any(), any(Update.class), any(), eq(Document.class), eq("alerts"));
    }

    @Test
    void reopenClearsOutcomeAndRecordsNewOwner() {
        when(mongo.findById("alert-1", Document.class, "alerts")).thenReturn(owned().append("status", "RESOLVED").append("disposition", "BENIGN"));
        when(mongo.findAndModify(any(Query.class), any(Update.class), any(FindAndModifyOptions.class), eq(Document.class), eq("alerts")))
                .thenReturn(owned().append("owner", "bob").append("version", 2L));
        service.review("alert-1", command(ReviewAction.REOPEN, 1, "bob", null));
        ArgumentCaptor<Update> update = ArgumentCaptor.forClass(Update.class);
        verify(mongo).findAndModify(any(Query.class), update.capture(), any(), eq(Document.class), eq("alerts"));
        assertThat(update.getValue().getUpdateObject().get("$unset", Document.class)).containsKeys("disposition", "resolvedAt");
        assertThat(update.getValue().getUpdateObject().get("$set", Document.class)).containsEntry("owner", "bob");
    }

    @Test
    void pinnedEvidenceExposesMissingLedgerAndRequestTruncation() {
        Document alert = new Document("_id", "alert-1").append("evidenceVersion", 1)
                .append("evidenceTransactionIds", List.of("txn-1", "txn-2", "external-kafka"))
                .append("evidenceCount", 3L).append("evidenceTruncated", false);
        when(mongo.findById("alert-1", Document.class, "alerts")).thenReturn(alert);
        when(mongo.find(any(Query.class), eq(TransactionDocument.class))).thenReturn(List.of(transaction("txn-2"), transaction("txn-1")));
        var evidence = service.evidence("alert-1", 1);
        assertThat(evidence.items()).extracting(view -> view.transactionId()).containsExactly("txn-1");
        assertThat(evidence.provenance()).isEqualTo("PINNED_DETECTION");
        assertThat(evidence.missingCount()).isEqualTo(1);
        assertThat(evidence.evidenceCount()).isEqualTo(3);
        assertThat(evidence.truncated()).isTrue();
        assertThat(evidence.complete()).isFalse();
        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        verify(mongo).find(query.capture(), eq(TransactionDocument.class));
        assertThat(query.getValue().getQueryObject()).containsEntry("_id", new Document("$in", List.of("txn-1", "txn-2", "external-kafka")));
    }

    @Test
    void pinnedCompleteEvidenceIsExplicit() {
        when(mongo.findById("alert-1", Document.class, "alerts")).thenReturn(new Document("_id", "alert-1").append("evidenceVersion", 1)
                .append("evidenceTransactionIds", List.of("txn-1")).append("evidenceCount", 1L));
        when(mongo.find(any(Query.class), eq(TransactionDocument.class))).thenReturn(List.of(transaction("txn-1")));
        assertThat(service.evidence("alert-1", 200).complete()).isTrue();
    }

    @Test
    void windowEvidenceIsClearlyLegacyContextAndUsesHalfOpenBsonRange() {
        Document alert = new Document("_id", "window-alert").append("accountId", "acct-1").append("currency", "CAD")
                .append("windowStart", Date.from(now)).append("windowEnd", Date.from(now.plusSeconds(60)));
        when(mongo.findById("window-alert", Document.class, "alerts")).thenReturn(alert);
        when(mongo.find(any(Query.class), eq(TransactionDocument.class))).thenReturn(List.of());
        assertThat(service.evidence("window-alert", 200).provenance()).isEqualTo("LEGACY_WINDOW_CONTEXT");
        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        verify(mongo).find(query.capture(), eq(TransactionDocument.class));
        assertThat(query.getValue().getQueryObject()).containsEntry("payload.accountId", "acct-1")
                .containsEntry("payload.currency", "CAD")
                .containsEntry("eventTimeDate", new Document("$gte", now).append("$lt", now.plusSeconds(60)));
    }

    @Test
    void queueCursorUsesStableTieBreakAndRejectsMalformedInput() {
        when(mongo.find(any(Query.class), eq(Document.class), eq("alerts"))).thenReturn(List.of(
                new Document("_id", "z").append("createdAt", Date.from(now)),
                new Document("_id", "a").append("createdAt", Date.from(now))));
        var page = service.alertPage(1, null, null, ReviewStatus.OPEN, null, null);
        assertThat(page.items()).hasSize(1);
        assertThat(page.nextCursor()).isNotBlank();
        service.alertPage(1, null, null, null, "alice", page.nextCursor());
        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        verify(mongo, times(2)).find(query.capture(), eq(Document.class), eq("alerts"));
        assertThat(query.getAllValues().get(1).getQueryObject().toString()).contains("alice", "$lt", "z");
        rejects(400, () -> service.alertPage(1, null, null, null, null, "not-a-cursor"));
    }

    @Test
    void unknownAlertEvidenceReturnsNotFound() { rejects(404, () -> service.evidence("missing", 200)); }

    private Document owned() { return new Document("_id", "alert-1").append("status", "INVESTIGATING").append("version", 1L).append("owner", "alice"); }
    private ReviewCommand command(ReviewAction action, long version, String analyst, Disposition disposition) {
        return new ReviewCommand(action, version, "operation-1", "Checked evidence", analyst, disposition);
    }
    private TransactionDocument transaction(String id) {
        return TransactionDocument.create(new TransactionPayload(1, id, "acct-1", "merchant-1", 500L,
                TransactionPayload.Currency.CAD, "CA", TransactionPayload.Channel.WEB, now), now);
    }
    private void rejects(int status, org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOf(ResponseStatusException.class)
                .satisfies(error -> assertThat(((ResponseStatusException) error).getStatusCode().value()).isEqualTo(status));
    }
}
