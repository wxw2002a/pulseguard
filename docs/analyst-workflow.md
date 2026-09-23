# Payment-risk investigation workflow

## The actual problem

A merchant's payment service retries a callback after a timeout. During the same minute, an account makes several small payments. An operations team receives alerts, but a list of scores is not enough: which original events triggered this particular signal, who is investigating it, and was the result confirmed risk or an authorized test?

PulseGuard addresses **reliable post-event triage and investigation**. It accepts normalized immutable payment events, derives transparent signals, and gives an operator a versioned decision journal. It does not decide payment authorization, move money, contact customers or prove that a transaction is fraudulent.

| Before | Implemented behavior | Repeatable evidence |
|---|---|---|
| Later arrivals appear in a previously reviewed window's evidence | Detection pins only its contributing transaction IDs; CARD_TESTING excludes larger payments | Streaming tests and `analyst_scenario.py` |
| Two people both claim an alert and overwrite each other | One atomic claim succeeds; the other receives HTTP 409 | Two simultaneous HTTP clients and real Mongo integration tests |
| A timed-out review creates a second audit entry | Same operation ID and normalized command returns current detail without appending | Retry assertions before and after resolution |
| A stale browser closes someone else's investigation | Expected version, state and cooperative owner are checked with the update | Stale version and wrong-owner assertions |
| The inbox only shows the most recent 200 alerts | Server filters plus descending `(createdAt, id)` cursor pagination | Equal-timestamp pagination and browser next-page tests |
| Every closed alert is treated the same | Resolution requires CONFIRMED_RISK, FALSE_POSITIVE or BENIGN | Outcome aggregation and reopen assertions |

## Five-minute demonstration

1. Start the stack with `docker compose up -d --build`.
2. Run `python scripts/analyst_scenario.py --timeout 300`. It sends nine synthetic events through the HTTP ingestion API, never inserts mock alerts into the database, and saves its assertions in `artifacts/analyst-scenario.json`.
3. Open `http://localhost:8080`, leave Sample data off, and open Alert inbox. Set the status filter to RESOLVED. The script leaves one confirmed-risk HIGH_VALUE case and one CARD_TESTING false-positive case.
4. Inspect the card-testing evidence: it contains only the small payments that triggered the initial detection. The ordinary payment and subsequent same-minute payment stay out of that snapshot.
5. Inspect the high-value history: claim, resolution, reopen and final resolution remain in order. Per-rule outcomes count the current resolution once, not every historical decision.
6. To try manual work, enter `local-dev-key` in Connection settings, choose an open alert, and supply an operator label and note. Claim it, investigate, then select a disposition when resolving. A second tab with an older version will receive a conflict and must refresh before making a new decision.

The script's dispositions are deliberately scripted labels. They demonstrate the workflow, **not real fraud ground truth, detection precision or financial loss prevention**. The browser's separate Sample data mode uses internally consistent synthetic fixtures and never writes to the API. GitHub Pages is a read-only CI snapshot, not a hosted backend.

## Review command contract

Fetch the detail first and send its current `version`. Legacy alerts without a stored version read as `0`.

```json
{
  "action": "CLAIM",
  "expectedVersion": 0,
  "operationId": "claim-case-001",
  "analyst": "risk-operator-a",
  "note": "Checking the original payments and merchant context"
}
```

Send it to `PATCH /api/v1/alerts/{percent-encoded-id}/review` with `X-API-Key`.

| Action | Precondition | Result |
|---|---|---|
| CLAIM | Unowned OPEN; unowned legacy INVESTIGATING can be adopted | INVESTIGATING, owner assigned |
| COMMENT | INVESTIGATING, same owner | Append a note, increment version |
| RELEASE | INVESTIGATING, same owner | OPEN, owner cleared |
| RESOLVE | INVESTIGATING, same owner, disposition required | RESOLVED with disposition and resolution time |
| REOPEN | RESOLVED | INVESTIGATING, new owner, current disposition cleared |

An atomic MongoDB update matches the expected version and appends the action while incrementing the version. The same operation ID is retained inside the same document, so timeout recovery does not depend on a second transaction or an external cache. An identical normalized command returns the **current detail**, which may include later work; the original entry remains in history. Reusing an ID with changed content is rejected. On a conflict, reload the alert and make a deliberate new command with a new ID; do not blindly retry with a fresh version.

History is capped at 500 accepted actions per alert; new actions then fail with 409. Previous entries, including their operation IDs, are not evicted. This bounds document growth and preserves the retry contract within that alert. A production archival/continuation design is outside this release.

Operator labels remain self-reported under a shared write key. Ownership coordinates cooperating users; it is **not user authentication or an authorization boundary**. Sensitive deployments require private ingress and authenticated principals before enabling customer data. The implementation's atomic compare-and-set follows [MongoDB's documented single-document concurrency model](https://www.mongodb.com/docs/manual/core/write-operations-atomicity/).

## Evidence and state boundaries

New alerts record `evidenceVersion=1`, sorted rule-specific IDs, total contributing count and truncation metadata. At most 200 IDs are captured. The existing Spark `collect_set(transactionId, amountMinor)` state is reused; evidence arrays are derived after aggregation, without adding a new stateful operator. This cap bounds the stored evidence array, **not the existing hot-account deduplication state**.

The first alert snapshot uses `$setOnInsert`; later window updates and Kafka replay do not change it or any review. An evidence response distinguishes:

- `PINNED_DETECTION`: only the stored contributing IDs are looked up in the immutable ingestion ledger. It can still be incomplete when records were published directly to Kafka or capture/response limits apply.
- `LEGACY_WINDOW_CONTEXT`: an older window alert has no pinned membership. Its account/currency/time-range records are context, not proof that each record contributed to that detection. No historical membership is fabricated.
- `LEGACY_TRANSACTION_CONTEXT`: an older event alert links to its original transaction ID but predates the versioned provenance contract; it is not relabeled as a newly captured detection.

The response carries counts, `missingCount`, `truncated` and `complete`. A missing record is not silently replaced by another same-window transaction. The alert may intentionally show fewer events than today's evolving window aggregate.

Alert queue cursors use a stable descending date/ID pair. They are not database snapshots: concurrent new alerts, ownership changes and resolution can change which records match. Reset pagination after changing filters or handling a case. Outcomes count current resolved alerts by rule, including legacy unclassified resolutions; reopening removes that case from current resolved totals while retaining its audit trail.

## Deployment and migration

No automatic data rewrite is required. Existing alerts default to version 0; unowned legacy investigations can be claimed, and resolved cases can be reopened. Evidence without stored IDs remains explicitly contextual. Existing Spark checkpoint state is unchanged, but production upgrades should still follow the [runbook](runbook.md) and retain backups.

The review request is a breaking client-contract change, reflected by OpenAPI contract version 2.0.0: old `status` writes are rejected instead of bypassing concurrency protection. Deploy the matching dashboard with the API. Ingestion's transaction-v1 contract is unchanged.
