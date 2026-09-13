package io.pulseguard.api.web;

import io.pulseguard.api.transaction.TransactionPayload;
import io.pulseguard.api.transaction.TransactionService;
import io.pulseguard.api.transaction.TransactionView;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/transactions")
@Validated
public class TransactionController {
    private final TransactionService service;
    public TransactionController(TransactionService service) { this.service = service; }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public TransactionService.IngestionResult ingest(@Valid @RequestBody TransactionPayload payload) {
        return service.accept(payload);
    }

    @GetMapping
    public ItemPage<TransactionView> list(
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) int limit,
            @RequestParam(required = false) @Pattern(regexp = "[A-Za-z0-9_-]{1,64}") String accountId) {
        return new ItemPage<>(service.recent(limit, accountId));
    }
}
