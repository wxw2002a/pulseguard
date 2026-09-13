package io.pulseguard.api.web;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.util.List;
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
    void reviewRequiresKeyAndRecognizedStatus() throws Exception {
        mvc.perform(patch("/api/v1/alerts/HIGH_VALUE:tx-1/review")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"status\":\"RESOLVED\"}"))
                .andExpect(status().isUnauthorized());
        mvc.perform(patch("/api/v1/alerts/HIGH_VALUE:tx-1/review").header("X-API-Key", "test-key")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"status\":\"IGNORED\"}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(investigation);
    }
}
