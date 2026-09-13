# Verification record

Observed on **2026-09-13**. All figures below come from retained CI output. The source events are synthetic; Kafka, MongoDB, Spark, the Java API, browser and Kubernetes cluster are real running components.

## Complete automated toolchain

The [full verification run](https://github.com/wxw2002a/pulseguard/actions/runs/34743432685) for commit `7376d10c3360a690efec2298a2724b0b0165d53a` passed **contracts, dashboard, Java, Compose/k6, Kubernetes and Quality gate**. Its [consolidated report and raw evidence](artifacts/automation-7376d10/README.md) are retained in the repository.

- JUnit / Mockito / Testcontainers / Spark: **57 passed, zero failures, errors or skips**.
- JaCoCo line coverage: **222/261 API lines (85.06%)**, **128/232 streaming lines (55.17%)**. These counters measure Maven test JVMs; the separate Compose and Kubernetes processes are not instrumented. Uncovered application code remains included.
- k6: **601 accepted events** at a configured **20 events/s for 30 seconds**, **6/6 thresholds passed**, zero failed HTTP requests and dropped iterations. Ingestion latency **p95 9.39 ms**, **p99 18.61 ms**. This is a controlled HTTP regression workload, not maximum capacity or end-to-end Spark latency.
- Real Compose checks passed replay, original evidence, review preservation, malformed-record quarantine, Spark restart and Kafka outage/recovery. The real kind deployment passed its seven-event pipeline scenario.
- The [Pages publication and public browser workflow](https://github.com/wxw2002a/pulseguard/actions/runs/34743594340) also passed. Its read-only snapshot contains **730 accepted transactions and 49 signals**, with zero pending outbox entries. The captured inputs are synthetic.

The remaining sections document the earlier baseline runs and their original artifacts, which are retained separately.

## Earlier passing runs

| Run | Commit | Observed result |
|---|---|---|
| [Full verify workflow](https://github.com/wxw2002a/pulseguard/actions/runs/34742387094) | `7554a95` | Java tests, browser checks and full Compose recovery pipeline all pass |
| [Kubernetes smoke](https://github.com/wxw2002a/pulseguard/actions/runs/34742236882) | `1897933` | Real one-node kind cluster; development manifests deploy and the payment event pipeline passes |

The difference between these two recorded commits changes documentation, a UI ordering caption and the Compose-only BSON assertion/export. The application backend, Spark engine and Kubernetes manifests tested in the cluster are identical. The hosted dashboard's current capture links to its own source verification run; see [GitHub Pages publication](github-pages.md) for that separate deployment and its browser checks.

## Java: 57 tests, zero failures, zero skips

The [retained JUnit summary](artifacts/java-test-summary.json) contains all suite counts:

- **28 API unit/MVC tests:** request validation, immutable IDs and conflicts, outbox leases/acknowledgements, exact Kafka wire timestamp, nanosecond-preserving storage, review validation and investigation queries.
- **5 real MongoDB/Kafka integration tests:** actual publication, concurrent ingestion, lease recovery, exact evidence boundaries and concurrent review-history capacity.
- **21 streaming unit tests:** strict parsing, integer amounts, rule boundaries, stable identities and Mongo insert-only updates.
- **3 real Spark tests:** SQL decoding, account/currency deduplication, and cross-batch duplicate delivery followed by checkpoint restore and late-window eviction.

These include actual Spark execution and Testcontainers services; their absence is not silently converted into a successful skip.

## Full Compose failure/recovery scenario

[Retained e2e report](artifacts/e2e-report.json) and [final container state](artifacts/compose-status.txt). All checks passed:

1. Reject incorrect API keys and invalid negative amounts.
2. Accept seven initial unique transactions; accept an identical retry without another ledger record and reject a conflicting retry with 409.
3. Materialize `HIGH_VALUE`, `VELOCITY` and `CARD_TESTING` signals. The six-event small-payment window has exactly **3,000 minor units**, without double counting.
4. Link the original single-event evidence and all six matching window transactions; preserve their identities, amounts and timestamps.
5. Record an operator label, review rationale and INVESTIGATING status.
6. Inject duplicate Kafka events and a malformed record. Quarantine the malformed record with a deterministic topic/partition/offset ID.
7. Restart Spark with persisted checkpoints. Require a new alert **and** a new window update; verify counts, alert identities, quarantine identity and review history are preserved.
8. Stop Kafka. Accept a new transaction into the durable outbox while the broker is unavailable. Restart Kafka; wait for the transaction to become SENT and produce its high-value alert.

The scenario accepted **9 unique events**, plus explicit duplicate and malformed deliveries. It completed in **43.812 seconds after the test began waiting for API readiness**. This is scenario duration, not event latency or a capacity benchmark.

## Kubernetes development deployment

The passing workflow created a real **kind 0.33.0 / Kubernetes 1.34.11** single-node cluster, built/loaded application images, applied Kustomize resources, waited for StatefulSet/Deployment readiness and completed a seven-event HTTP → Kafka → Spark → MongoDB scenario.

The [Kubernetes e2e report](artifacts/kubernetes-e2e-report.json) verifies all three rules, duplicate/conflict handling and original evidence. The [resource snapshot](artifacts/kubernetes-resources.txt) records running API/Kafka/MongoDB/Spark pods and three Bound PVCs. The HPA is present; metrics-server is not installed in this test, so automatic scaling is **not** exercised.

This is the **single-pod `local[2]` Spark development configuration**. The optional native distributed driver/executor submission is not covered by this result. Kafka outage and Spark restart scenarios ran in Compose, not in the Kubernetes smoke job; the Kubernetes report marks those fields false accordingly.

## Measured HTTP exercise

[Raw load report](artifacts/load-report.json), from the same passing Compose run:

| Measurement | Result |
|---|---:|
| Requested generator rate | 20 requests/s |
| Submitted / accepted | 120 / 120 |
| Errors / duplicates | 0 / 0 |
| Duration | 5.956 s |
| Observed acceptance rate | 20.15 responses/s |
| HTTP latency p50 / p95 / p99 | 5.91 / 21.83 / 63.24 ms |

This is a **short functional load exercise on a GitHub-hosted Ubuntu runner**, rate-limited by the client. Compose capped the API at 768 MB, Spark at 2 GB, MongoDB at 768 MB and Kafka at 1 GB. It measures HTTP acceptance, not completed risk analysis, maximum sustainable throughput or a production SLO. The small difference from the scheduled 20 requests/s reflects a finite sample whose first request is submitted immediately.

## Browser and runtime screenshot

Chromium checks cover sample-mode labeling, severity filtering, keyboard alert access, reasoned reviews, authenticated scenario payloads, unique event IDs, escaped untrusted text, mobile width and offline state.

The [README screenshot](assets/dashboard.png) was separately captured against the actual running Compose API after the recovery scenario and 120-event load. It shows **129 accepted transactions, 13 signals and zero pending outbox records**. Sample data is disabled. These are real outputs for synthetic inputs, not fixture counters or fraud-accuracy measurements.

## Local environment and limits

Local Java validation used Windows 11, Microsoft OpenJDK 17.0.16 and Maven 3.9.11. Both modules compiled and packaged. Windows lacked Hadoop NativeIO support for the file-stream checkpoint test, and Docker Desktop failed during its own inference-manager startup before containers could launch. Full runtime verification therefore ran successfully in Linux CI; no local Docker success is claimed.

Not established by the above results: native multi-node Spark execution, high availability under node/storage loss, production capacity, end-to-end latency percentiles, fraud accuracy, financial benefit or a security/compliance audit. Those require separate workloads and deployment designs.
