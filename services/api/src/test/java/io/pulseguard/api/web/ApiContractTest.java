package io.pulseguard.api.web;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.util.List;
import java.util.Map;
import io.pulseguard.api.config.PulseGuardProperties;
import io.pulseguard.api.investigation.InvestigationService;
import io.pulseguard.api.transaction.TransactionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest({TransactionController.class, InvestigationController.class})
@EnableConfigurationProperties(PulseGuardProperties.class)
@TestPropertySource(properties = "pulseguard.api-key=test-key")
class ApiContractTest {
    private static final String PAYLOAD = """
            {"schemaVersion":1,"transactionId":"tx-1","accountId":"acct-1","merchantId":"shop-1",
            "amountMinor":1000,"currency":"USD","country":"US","channel":"WEB","eventTime":"2026-09-13T12:00:00Z"}
            """;
    @Autowired MockMvc mvc;
    @MockitoBean TransactionService transactions;
    @MockitoBean InvestigationService investigation;

    @Test
    void unauthenticatedWriteDoesNotReachBusinessLogic() throws Exception {
        mvc.perform(post("/api/v1/transactions").contentType(MediaType.APPLICATION_JSON).content(PAYLOAD))
                .andExpect(status().isUnauthorized()).andExpect(jsonPath("status").value(401));
        verifyNoInteractions(transactions);
    }

    @Test
    void validRequestHasStableAcceptedContract() throws Exception {
        when(transactions.accept(any())).thenReturn(new TransactionService.IngestionResult("tx-1", "ACCEPTED", false));
        mvc.perform(post("/api/v1/transactions").header("X-API-Key", "test-key")
                        .contentType(MediaType.APPLICATION_JSON).content(PAYLOAD))
                .andExpect(status().isAccepted()).andExpect(jsonPath("transactionId").value("tx-1"))
                .andExpect(jsonPath("duplicate").value(false));
    }

    @Test
    void negativeAmountReportsFieldError() throws Exception {
        mvc.perform(post("/api/v1/transactions").header("X-API-Key", "test-key")
                        .contentType(MediaType.APPLICATION_JSON).content(PAYLOAD.replace("1000", "-1")))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("errors.amountMinor").exists());
        verifyNoInteractions(transactions);
    }

    @Test
    void fractionalMinorUnitsAreRejectedInsteadOfSilentlyTruncated() throws Exception {
        mvc.perform(post("/api/v1/transactions").header("X-API-Key", "test-key")
                        .contentType(MediaType.APPLICATION_JSON).content(PAYLOAD.replace("1000", "12.34")))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(transactions);
    }

    @Test
    void unknownPayloadFieldsAreRejected() throws Exception {
        mvc.perform(post("/api/v1/transactions").header("X-API-Key", "test-key")
                        .contentType(MediaType.APPLICATION_JSON).content(PAYLOAD.replace("\"schemaVersion\":1", "\"schemaVersion\":1,\"typo\":42")))
                .andExpect(status().isBadRequest());
    }

    @Test
    void anonymousReadsAreBoundedAndUseItemsWrapper() throws Exception {
        when(transactions.recent(50, null)).thenReturn(List.of());
        mvc.perform(get("/api/v1/transactions")).andExpect(status().isOk()).andExpect(jsonPath("items").isArray());
        mvc.perform(get("/api/v1/transactions?limit=201")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/alerts?severity=INVALID")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/windows?accountId=invalid%2Fid")).andExpect(status().isBadRequest());
    }

    @Test
    void reviewRequiresKeyAndRecognizedAction() throws Exception {
        mvc.perform(patch("/api/v1/alerts/HIGH_VALUE:tx-1/review")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"action\":\"RESOLVE\"}"))
                .andExpect(status().isUnauthorized());
        mvc.perform(patch("/api/v1/alerts/HIGH_VALUE:tx-1/review").header("X-API-Key", "test-key")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"action\":\"IGNORED\"}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(investigation);
    }

    @Test
    void reviewRequiresNoteAndAnalystLabel() throws Exception {
        mvc.perform(patch("/api/v1/alerts/HIGH_VALUE:tx-1/review").header("X-API-Key", "test-key")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"action\":\"RESOLVE\"}"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("errors.note").exists())
                .andExpect(jsonPath("errors.analyst").exists());
        verifyNoInteractions(investigation);
    }

    @Test
    void reviewRequiresVersionAndOperationIdentityAndAcceptsValidClaim() throws Exception {
        mvc.perform(patch("/api/v1/alerts/alert-1/review").header("X-API-Key", "test-key")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"action\":\"CLAIM\",\"analyst\":\"alice\",\"note\":\"Review evidence\"}"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("errors.expectedVersion").exists())
                .andExpect(jsonPath("errors.operationId").exists());
        when(investigation.review(eq("alert-1"), any())).thenReturn(Map.of("id", "alert-1", "owner", "alice", "version", 1));
        mvc.perform(patch("/api/v1/alerts/alert-1/review").header("X-API-Key", "test-key")
                        .contentType(MediaType.APPLICATION_JSON).content("""
                                {"action":"CLAIM","analyst":"alice","note":"Review evidence","expectedVersion":0,"operationId":"claim-request-1"}
                                """))
                .andExpect(status().isOk()).andExpect(jsonPath("owner").value("alice")).andExpect(jsonPath("version").value(1));
    }

    @Test
    void queueAndOutcomesExposePaginationAndUnclassifiedLegacyResults() throws Exception {
        when(investigation.alertPage(50, null, null, InvestigationService.ReviewStatus.INVESTIGATING, "alice", null))
                .thenReturn(new InvestigationService.AlertPage(List.of(Map.of("id", "alert-1")), "next-page"));
        mvc.perform(get("/api/v1/alerts?status=INVESTIGATING&owner=alice"))
                .andExpect(status().isOk()).andExpect(jsonPath("items[0].id").value("alert-1"))
                .andExpect(jsonPath("nextCursor").value("next-page"));
        when(investigation.outcomes()).thenReturn(new InvestigationService.Outcomes(2,
                List.of(new InvestigationService.RuleOutcome("HIGH_VALUE", 2, 1, 0, 0, 1))));
        mvc.perform(get("/api/v1/outcomes")).andExpect(status().isOk()).andExpect(jsonPath("resolvedAlerts").value(2))
                .andExpect(jsonPath("byRule[0].unclassified").value(1));
    }

    @Test
    void investigationDetailAndEvidenceHaveBoundedReadContracts() throws Exception {
        when(investigation.detail("HIGH_VALUE:tx-1", 50)).thenReturn(Map.of("id", "HIGH_VALUE:tx-1", "reviewHistory", List.of(), "reviewHistoryCount", 0));
        when(investigation.evidence("HIGH_VALUE:tx-1", 200)).thenReturn(new InvestigationService.EvidencePage(
                List.of(), "PINNED_DETECTION", 1, 0, 1, false, false));
        mvc.perform(get("/api/v1/alerts/HIGH_VALUE:tx-1")).andExpect(status().isOk())
                .andExpect(jsonPath("reviewHistory").isArray()).andExpect(jsonPath("reviewHistoryCount").value(0));
        mvc.perform(get("/api/v1/alerts/HIGH_VALUE:tx-1/evidence")).andExpect(status().isOk()).andExpect(jsonPath("items").isArray());
        mvc.perform(get("/api/v1/alerts/HIGH_VALUE:tx-1/evidence?limit=201")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/alerts/HIGH_VALUE:tx-1?historyLimit=501")).andExpect(status().isBadRequest());
    }
}
