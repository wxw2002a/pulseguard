# PulseGuard automated test report

Artifact assessment: **PASSED**. Generated 2026-09-13T06:46:47.428657+00:00.

This report summarizes observed artifact evidence. The final quality gate must also require successful upstream jobs; report generation cannot turn a failed or cancelled job into a pass.

## Java and Spark tests

Observed **57 test cases: 57 passed, 0 failed, 0 errors, 0 skipped**.

| Suite | Module / phase | Passed | Failed | Errors | Skipped |
|---|---|---:|---:|---:|---:|
| io.pulseguard.api.IngestionIT | api / integration | 5 | 0 | 0 | 0 |
| io.pulseguard.api.config.KafkaWireContractTest | api / unit / Spark | 1 | 0 | 0 | 0 |
| io.pulseguard.api.investigation.InvestigationServiceTest | api / unit / Spark | 5 | 0 | 0 | 0 |
| io.pulseguard.api.outbox.OutboxPublisherTest | api / unit / Spark | 3 | 0 | 0 | 0 |
| io.pulseguard.api.outbox.OutboxStoreTest | api / unit / Spark | 3 | 0 | 0 | 0 |
| io.pulseguard.api.transaction.TransactionMappingTest | api / unit / Spark | 2 | 0 | 0 | 0 |
| io.pulseguard.api.transaction.TransactionServiceTest | api / unit / Spark | 5 | 0 | 0 | 0 |
| io.pulseguard.api.web.ApiContractTest | api / unit / Spark | 9 | 0 | 0 | 0 |
| io.pulseguard.streaming.RiskRulesTest | streaming / unit / Spark | 4 | 0 | 0 | 0 |
| io.pulseguard.streaming.StreamTransformsTest | streaming / unit / Spark | 3 | 0 | 0 | 0 |
| io.pulseguard.streaming.TransactionParserTest | streaming / unit / Spark | 17 | 0 | 0 | 0 |

JUnit suite files are counted once; Failsafe summaries are not added to those totals. Skipped tests are shown explicitly and are not counted as passes.

## Compose pipeline and recovery

Reported status: **passed**. Scenario run ID: `be3ad4a579d3`. Completed: 2026-09-13T06:45:21.681689+00:00.

| Check | Observed result |
|---|---|
| Exact HTTP replay is idempotent | PASS |
| Conflicting transaction IDs are rejected | PASS |
| Original transaction evidence is linked | PASS |
| Spark checkpoint recovery processes new events | PASS |
| Replay preserves analyst review and audit history | PASS |
| Malformed input retains a deterministic dead-letter identity | PASS |
| The durable outbox recovers after a Kafka outage | PASS |

Rules observed: HIGH_VALUE, VELOCITY, CARD_TESTING.
- Accepted unique scenario events: **9**.
- Checked window: unique transactions: **6**.
- Checked window: amount in minor units: **3000**.
- Scenario elapsed seconds: **41.879**.

These are recorded scenario checks, not production capacity or fraud-accuracy measurements.

## Kubernetes smoke test

Reported status: **passed**. Scenario run ID: `71e8e5b21542`. Completed: 2026-09-13T06:46:36.377130+00:00.

| Check | Observed result |
|---|---|
| Exact HTTP replay is idempotent | PASS |
| Conflicting transaction IDs are rejected | PASS |
| Original transaction evidence is linked | PASS |
| Spark checkpoint recovery processes new events | NOT CHECKED |
| Replay preserves analyst review and audit history | NOT CHECKED |
| Malformed input retains a deterministic dead-letter identity | NOT CHECKED |
| The durable outbox recovers after a Kafka outage | NOT CHECKED |

Rules observed: HIGH_VALUE, VELOCITY, CARD_TESTING.
- Accepted unique scenario events: **7**.
- Checked window: unique transactions: **6**.
- Checked window: amount in minor units: **3000**.
- Scenario elapsed seconds: **9.32**.

These are recorded scenario checks, not production capacity or fraud-accuracy measurements.
Kubernetes evidence covers the one-node development stack. Recovery checks marked NOT CHECKED were not performed in that scenario; this does not prove multi-node Spark, HA, or HPA scaling.

## Synthetic HTTP acceptance exercise

The CI exercise is **client-rate-limited**. Observed HTTP acknowledgement rates are **not maximum throughput**, and request latency is **not end-to-end Spark latency**. The current source report does not record the configured target rate.

Measured: 2026-09-13T06:45:27.783181+00:00. Scenario run ID: `0db3e3ca4c`.

| Measurement | Observed value |
|---|---:|
| Submitted requests | 120 |
| HTTP 202 responses | 120 |
| Duplicate acknowledgements | 0 |
| Elapsed seconds | 5.956 |
| Accepted responses / second | 20.15 |
| HTTP request latency p50 (ms) | 6.23 |
| HTTP request latency p95 (ms) | 18.89 |
| HTTP request latency p99 (ms) | 44.13 |
| HTTP request latency max (ms) | 48.95 |

HTTP statuses: 202 = 120.

## k6 performance regression

**6/6 thresholds passed.** Configured load: **20 events/s for 30s**; **601** accepted events, target **600**.

HTTP ingestion request latency: p95 **9.39 ms**, p99 **18.61 ms**.

This is a rate-limited HTTP regression test, not a maximum-throughput measurement or an end-to-end Spark latency result.

Recorded scope: Rate-limited HTTP ingestion regression; not maximum capacity or end-to-end Spark latency.

Recorded source commit: `7376d10c3360a690efec2298a2724b0b0165d53a`.

| k6 criterion | Result |
|---|---|
| checks: rate==1 | PASS |
| http_req_failed: rate==0 | PASS |
| accepted_events: count&gt;=600 | PASS |
| http_req_duration{endpoint:ingest}: p(95)&lt;500 | PASS |
| http_req_duration{endpoint:ingest}: p(99)&lt;1000 | PASS |
| dropped_iterations: count==0 | PASS |

## Dashboard checks

Observed 2 explicit success markers (not a JUnit test-case count):

- PASS: preview labeling, filter, keyboard details, review, live API, HTML escaping, scenario auth/IDs, mobile, offline state — `dashboard-test-reports/dashboard-tests.log`
- PASS: Pages build/subpath, verified source/capture, snapshot evidence/history, filters, read-only controls, isolated sample edits, mobile, zero live API requests — `dashboard-test-reports/dashboard-tests.log`

A success marker does not establish the final process exit status; the upstream dashboard job remains authoritative.

## JaCoCo coverage

Coverage is measured from the attached execution data. No arbitrary minimum percentage is enforced; unexecuted production code remains in the denominator.

| Report | Covered lines | Missed lines | Line coverage |
|---|---:|---:|---:|
| jacoco-coverage/api/target/site/jacoco/jacoco.xml | 222 | 39 | 85.06% |
| jacoco-coverage/streaming/target/site/jacoco/jacoco.xml | 128 | 104 | 55.17% |

## Artifact provenance

Counts and assertions above are derived from the supplied files. A scenario run ID inside an e2e/load report is a generated fixture identifier, not a GitHub Actions run ID.

Report-generation workflow: [GitHub Actions run 34743432685](https://github.com/wxw2002a/pulseguard/actions/runs/34743432685).

Report-generation commit: `7376d10c3360a690efec2298a2724b0b0165d53a`.

Embedded dashboard snapshot metadata: source commit `7376d10c3360a690efec2298a2724b0b0165d53a`, run `https://github.com/wxw2002a/pulseguard/actions/runs/34743432685`, captured 2026-09-13T06:46:29.263Z.

Workflow context alone does not authenticate an arbitrary local download. Download artifacts from one intended workflow run into an empty directory and retain the source run URL. SHA-256 values identify the exact report bytes used here.

| Source path relative to artifact directory | SHA-256 |
|---|---|
| dashboard-test-reports/dashboard-tests.log | `a121a61029737adbeba2560e2ed229665f650ab64e127319fbac71cbf5f15dd9` |
| jacoco-coverage/api/target/site/jacoco/jacoco.xml | `d93e43f58af2f84d17f1f31b066581b904305489601eaa3330f504c04adc95c7` |
| jacoco-coverage/streaming/target/site/jacoco/jacoco.xml | `c08ed3f7e0356a6316d258e24fd5a1449718818d713158736485aed2e2d078d0` |
| java-test-reports/api/target/failsafe-reports/TEST-io.pulseguard.api.IngestionIT.xml | `4d7eeb50133fb5ab1b6e16d567af1fb3e709827abc521c6b68b4e409afc82e62` |
| java-test-reports/api/target/failsafe-reports/failsafe-summary.xml | `a81d75e6f1b44d866827ca77b54e23e883ce942020df3ae3313b9a33a8c77c9e` |
| java-test-reports/api/target/surefire-reports/TEST-io.pulseguard.api.config.KafkaWireContractTest.xml | `0aeb0bb913458fce273bce87f579dc56bdb3f7bb8acbf22655c7c69a1f63ccd1` |
| java-test-reports/api/target/surefire-reports/TEST-io.pulseguard.api.investigation.InvestigationServiceTest.xml | `c4adc26688ce285e1c0e4af6f17b1585d435320aff7c88c7cc5c22a19b99bfc1` |
| java-test-reports/api/target/surefire-reports/TEST-io.pulseguard.api.outbox.OutboxPublisherTest.xml | `061873146c0e7cd99966c2b0d5fb76155be8089fdcf9147e72871a04f57a074e` |
| java-test-reports/api/target/surefire-reports/TEST-io.pulseguard.api.outbox.OutboxStoreTest.xml | `1e5de4d5fb66229242914d0b8abfc677f81500e679504e6d683bd9ac2f12e055` |
| java-test-reports/api/target/surefire-reports/TEST-io.pulseguard.api.transaction.TransactionMappingTest.xml | `cc2dc797835f5d3e9a2480fc77081a84c13a84ddf2a81159fd02a78dba3c2d6e` |
| java-test-reports/api/target/surefire-reports/TEST-io.pulseguard.api.transaction.TransactionServiceTest.xml | `246661408a73ee52340ac915c800b1987224eb5478b49b02ac99ece705c8d8c0` |
| java-test-reports/api/target/surefire-reports/TEST-io.pulseguard.api.web.ApiContractTest.xml | `eb33cde8c071db58607e67f2ac9a6a8f11cce835209d91154c30af21e2b381c8` |
| java-test-reports/streaming/target/surefire-reports/TEST-io.pulseguard.streaming.RiskRulesTest.xml | `21a80d73feb94465c1a6a97ea1c571aae6bb373bd8dc665ee8b657c628615a18` |
| java-test-reports/streaming/target/surefire-reports/TEST-io.pulseguard.streaming.StreamTransformsTest.xml | `afe814066f497ff5a69a08317beebf48c34f1b8ff720ad145edd3db5ef8f8520` |
| java-test-reports/streaming/target/surefire-reports/TEST-io.pulseguard.streaming.TransactionParserTest.xml | `ed93040b90d174cfa50df0e97466a774cdb3ea25b11e678b7734572e3356a810` |
| kubernetes-evidence/kubernetes-e2e-report.json | `abcf40c73dbff3fbc2e16e01c1d6f0e62575a1cf35bf5a7e397d0afb680cd166` |
| pipeline-evidence/dashboard-snapshot.json | `6cd260ad87217bcaf8de78af531f7342e86bab1c84472d0ff415d1542cc0edca` |
| pipeline-evidence/e2e-report.json | `eded9f2bd72ed30ac9142bf25d5b0db521721567d428d7ec1c79dd6cb97cb386` |
| pipeline-evidence/k6-summary.json | `758aad38942b223bcba7a8b3aef0f4c0dd1b505e92611ede75d9ce738dfe40b5` |
| pipeline-evidence/load-report.json | `8755d36d3ce85fcc5d2601f6043ff2ffde54240a9c10568dd118e0f4410f4cd1` |
