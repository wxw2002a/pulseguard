# Integrating payment events

PulseGuard consumes a normalized transaction event. It is a downstream monitoring service; it does not replace the payment provider's authorization, settlement or refund APIs.

## Choose the business event

Publish one canonical **successful payment** event per transaction. Normalize the payment provider's payment ID into `transactionId`, the customer/account identifier into `accountId`, and the business's merchant identifier into `merchantId`. Use opaque identifiers; do not include card numbers, bank credentials, addresses or customer names.

Use the payment ID rather than a new UUID on every webhook retry. If the provider retries delivery, the same business transaction must retain the same ID, amount, currency and event time. A payload that changes under an existing ID returns 409 and needs investigation rather than a blind retry.

The current v1 contract has no reversal, refund, decline or lifecycle-status field. Do not feed those events into the successful-payment stream as if they were new positive payments. Supporting a full payment ledger would require explicit event types and projection semantics.

## Normalize without losing meaning

| Field | Mapping rule |
|---|---|
| `schemaVersion` | Always `1` for this contract |
| `transactionId` | Stable payment ID, normalized to 1–64 letters/digits/underscores/hyphens |
| `accountId` | Stable opaque account key; Kafka partition key |
| `merchantId` | Stable opaque merchant key |
| `amountMinor` | Positive integer minor units; supported currencies use two decimal places |
| `currency` | `USD`, `CAD`, `EUR` or `GBP`; no implicit currency conversion |
| `country` | Two uppercase letters supplied by the source |
| `channel` | `WEB`, `MOBILE` or `POS` |
| `eventTime` | Original business event time as an ISO instant, preserved on retries |

The original timestamp is essential: replacing it with the retry time can move an event to a different risk window. Application acceptance rejects events more than 30 seconds in the future. Synchronize clocks and quarantine source-data errors rather than rewriting timestamps to bypass validation.

## HTTP delivery behavior

Send `POST /api/v1/transactions`, `Content-Type: application/json` and `X-API-Key`. Store a pending delivery record in the source system if it needs guaranteed delivery to PulseGuard. The PulseGuard outbox starts **after** a request has reached and been accepted by its API; it cannot recover events that the sender never delivered.

| Response / condition | Source action |
|---|---|
| `202`, `duplicate=false` | Accepted durably; the source can acknowledge its delivery |
| `202`, `duplicate=true` | Already accepted with the same contents; safe to acknowledge |
| Connection timeout or reset | Retry the exact same payload and ID with bounded exponential backoff |
| `500` / `503` | Retry the exact same payload; keep the source's pending delivery until acceptance |
| `400` | Invalid mapping/data; record the error and correct the integration |
| `401` | Fix the API key; do not create a rapid retry loop |
| `409` | Existing ID conflicts with new data; escalate the source inconsistency |

An HTTP `202` is an ingestion acknowledgement, **not a fraud verdict**. Risk alerts arrive asynchronously after Spark processes the Kafka stream. The source should not wait for synchronous allow/deny behavior that this API does not provide.

## Investigate an alert

1. Query `GET /api/v1/alerts` to locate the signal.
2. Fetch `/api/v1/alerts/{id}` for reasons and review history.
3. Fetch `/api/v1/alerts/{id}/evidence` for the original transaction or contributing account/currency/window transactions.
4. Record a review with `PATCH /api/v1/alerts/{id}/review` and a nonempty decision note. `INVESTIGATING` can identify an active case; `RESOLVED` closes it in the unresolved high-risk count.

Window evidence is capped at 200 transactions per request. Its count may be smaller than the full aggregate in a high-volume window; the API does not currently provide an exhaustive evidence export. Window alerts preserve their first detection snapshot while the separate current window projection may continue to change as accepted events arrive.

## Verify before connecting a new source

Run the supplied `scripts/e2e.py --with-recovery` against a disposable Compose stack. It verifies accepted events, duplicate/conflicting IDs, risk rules, original evidence, review preservation, malformed-record quarantine, Spark restart and Kafka outage recovery. Then replay representative source fixtures through the same HTTP contract and compare expected counts and currency totals.

The included synthetic generator is a deterministic integration test driver; it does not establish a provider-specific integration or real-world fraud accuracy. Authentication, authorization, transport encryption, retention and workload capacity need an explicit deployment design before using real customer events.
