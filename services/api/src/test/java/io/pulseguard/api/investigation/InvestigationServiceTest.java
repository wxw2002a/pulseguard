package io.pulseguard.api.investigation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
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

class InvestigationServiceTest {
    private final Instant now = Instant.parse("2026-09-13T12:00:00Z");
    private final MongoTemplate mongo = mock(MongoTemplate.class);
    private final InvestigationService service = new InvestigationService(mongo, Clock.fixed(now, ZoneOffset.UTC));

    @Test
    void reviewAppendsNoteAndCurrentStatusInOneCapacityGuardedUpdate() {
        Document alert = new Document("_id", "alert-1").append("status", "INVESTIGATING")
                .append("reviewHistory", List.of(new Document("status", "INVESTIGATING")
                        .append("note", "Checked evidence").append("analyst", "ops-team").append("reviewedAt", Date.from(now))));
        when(mongo.findAndModify(any(Query.class), any(Update.class), any(FindAndModifyOptions.class), eq(Document.class), eq("alerts")))
                .thenReturn(alert);
        var result = service.review("alert-1", InvestigationService.ReviewStatus.INVESTIGATING, " Checked evidence ", " ops-team ");
        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> update = ArgumentCaptor.forClass(Update.class);
        verify(mongo).findAndModify(query.capture(), update.capture(), any(FindAndModifyOptions.class), eq(Document.class), eq("alerts"));
        assertThat(query.getValue().getQueryObject()).containsEntry("reviewHistory.499", new Document("$exists", false));
        Document pushed = update.getValue().getUpdateObject().get("$push", Document.class).get("reviewHistory", Document.class);
        assertThat(pushed).containsEntry("note", "Checked evidence").containsEntry("analyst", "ops-team");
        assertThat(update.getValue().getUpdateObject().get("$set", Document.class)).containsEntry("status", "INVESTIGATING");
        assertThat(result.get("reviewHistoryCount")).isEqualTo(1);
    }

    @Test
    void fullHistoryReturnsConflictWithoutDeletingAnyEntry() {
        when(mongo.exists(any(Query.class), eq("alerts"))).thenReturn(true);
        assertThatThrownBy(() -> service.review("alert-1", InvestigationService.ReviewStatus.RESOLVED, "Verified evidence", "ops-team"))
                .isInstanceOf(ResponseStatusException.class).satisfies(error ->
                        assertThat(((ResponseStatusException) error).getStatusCode().value()).isEqualTo(409));
    }

    @Test
    void whitespaceOnlyAuditNoteNeverReachesStorage() {
        assertThatThrownBy(() -> service.review("alert-1", InvestigationService.ReviewStatus.RESOLVED, "   ", "ops-team"))
                .isInstanceOf(ResponseStatusException.class);
        verifyNoInteractions(mongo);
    }

    @Test
    void windowEvidenceFiltersAccountCurrencyAndHalfOpenBsonTimeRange() {
        Document alert = new Document("_id", "window-alert").append("accountId", "acct-1").append("currency", "CAD")
                .append("windowStart", Date.from(now)).append("windowEnd", Date.from(now.plusSeconds(60)));
        when(mongo.findById("window-alert", Document.class, "alerts")).thenReturn(alert);
        when(mongo.find(any(Query.class), eq(TransactionDocument.class))).thenReturn(List.of());
        assertThat(service.evidence("window-alert", 200)).isEmpty();
        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        verify(mongo).find(query.capture(), eq(TransactionDocument.class));
        assertThat(query.getValue().getQueryObject()).containsEntry("payload.accountId", "acct-1")
                .containsEntry("payload.currency", "CAD")
                .containsEntry("eventTimeDate", new Document("$gte", now).append("$lt", now.plusSeconds(60)));
        assertThat(query.getValue().getLimit()).isEqualTo(200);
    }

    @Test
    void unknownAlertEvidenceReturnsNotFound() {
        assertThatThrownBy(() -> service.evidence("missing", 200)).isInstanceOf(ResponseStatusException.class)
                .satisfies(error -> assertThat(((ResponseStatusException) error).getStatusCode().value()).isEqualTo(404));
    }
}
