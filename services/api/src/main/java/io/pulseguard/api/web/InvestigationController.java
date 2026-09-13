package io.pulseguard.api.web;

import java.util.Map;
import io.pulseguard.api.investigation.InvestigationService;
import io.pulseguard.api.investigation.InvestigationService.ReviewStatus;
import io.pulseguard.api.investigation.InvestigationService.Severity;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
@Validated
public class InvestigationController {
    private final InvestigationService service;
    public InvestigationController(InvestigationService service) { this.service = service; }

    @GetMapping("/alerts")
    public ItemPage<Map<String, Object>> alerts(
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) int limit,
            @RequestParam(required = false) Severity severity,
            @RequestParam(required = false) @Pattern(regexp = "[A-Za-z0-9_-]{1,64}") String accountId) {
        return new ItemPage<>(service.alerts(limit, severity, accountId));
    }

    @GetMapping("/windows")
    public ItemPage<Map<String, Object>> windows(
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) int limit,
            @RequestParam(required = false) @Pattern(regexp = "[A-Za-z0-9_-]{1,64}") String accountId) {
        return new ItemPage<>(service.windows(limit, accountId));
    }

    @GetMapping("/overview")
    public InvestigationService.Overview overview() { return service.overview(); }

    @PatchMapping("/alerts/{id}/review")
    public Map<String, Object> review(@PathVariable @Size(min = 1, max = 256) String id,
                                      @Valid @RequestBody ReviewRequest request) {
        return service.review(id, request.status());
    }

    public record ReviewRequest(@NotNull ReviewStatus status) { }
}
