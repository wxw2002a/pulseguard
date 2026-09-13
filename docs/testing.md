# Automated testing

PulseGuard uses JUnit 5, Mockito, Spring MockMvc, Testcontainers, JaCoCo, Playwright and Grafana k6. GitHub Actions executes the toolchain against actual application processes, containers and a Kubernetes cluster. The repository retains machine-readable results and a consolidated report for each verification run.

## Test layers

| Tool | Checks | Evidence |
|---|---|---|
| JUnit 5, Mockito and Spring MockMvc | Java rule boundaries, validation, immutable IDs, conflicts, outbox ownership, API errors and review behavior | Surefire XML and text reports |
| Testcontainers with JUnit 5 | Real MongoDB and Kafka publication, concurrent ingestion, lease recovery, evidence boundaries and concurrent review capacity | Failsafe XML and container diagnostics |
| Spark with JUnit 5 | Actual Structured Streaming queries, exact window deduplication, checkpoint restore and late-event eviction | Surefire XML; Spark test logs |
| JaCoCo | Instrumented execution of application classes by the Java test suites | Per-module HTML, XML and execution data |
| Playwright with Chromium | Browser navigation, filtering, keyboard access, review actions, escaped input, mobile layout, offline behavior, hosted snapshot isolation and asset paths | Browser log and screenshots |
| OpenAPI Spec Validator and JSON Schema | API document validity and event-contract acceptance/rejection examples | Contract validation log |
| Docker Compose plus end-to-end assertions | Real HTTP → Mongo outbox → Kafka → Spark → Mongo projections; malformed quarantine, duplicate replay, review preservation, Spark restart and Kafka outage/recovery | JSON scenario report, container state and full logs |
| Grafana k6 | Controlled HTTP ingestion rate, accepted event identity, error rate, dropped iterations and latency thresholds | Raw JSON metrics and JUnit threshold results |
| kind and kubectl | Actual Kubernetes deployment, readiness, persistent volumes and a complete ingestion/investigation scenario | Scenario report, resource snapshots, events and pod logs |

Mockito is used where unit tests need controlled dependencies. The integration and deployment stages run the real services. Missing container dependencies cause those stages to fail rather than silently skip.

## Triggers and quality gate

Every push to `main`, every pull request and manual execution starts the `verify` workflow. Five jobs run independently: contracts, dashboard, Java, Compose pipeline and Kubernetes. The Kubernetes workflow is reusable and also supports a standalone manual run.

The final **Quality gate** downloads the current run's artifacts, generates an English Markdown report and requires all five jobs to succeed. A failed, cancelled or skipped required job cannot produce a passing gate. Missing required evidence and observed failed tests also fail report validation. Diagnostics upload even when a test fails; temporary Compose resources and the kind cluster are cleaned up in `always()` steps.

For a successful `main` push, the separate Pages workflow publishes the exact verified source and its captured synthetic results. It then uses Playwright against the actual public URL to check the expected commit, recorded data, original evidence, review history, assets and desktop/mobile layouts. Publication failures and post-deployment browser failures are visible in that workflow. A post-deployment failure reports the problem; it does not automatically roll back a deployment that has already completed.

Pull requests execute tests without publishing a website. No external cloud account, paid test service or production payment credential is required.

## k6 regression budgets

The CI scenario sends **20 events/second for 30 seconds** against the already running Compose API. It requires:

- At least 600 uniquely accepted events, with HTTP 202 and the matching transaction ID.
- Zero failed HTTP requests, failed response checks and dropped scheduled iterations.
- Ingestion HTTP latency below **500 ms at p95** and **1,000 ms at p99**.

These are regression budgets for the shared CI runner. They are not an advertised capacity limit, production SLA or end-to-end Spark latency target. The test stops the build with a nonzero exit status when a threshold fails. Actual measurements remain in the run artifact.

`PG_RATE`, `PG_DURATION_SECONDS`, `PG_P95_MS` and `PG_P99_MS` can configure a separate local experiment; the CI workflow uses the checked-in defaults. Authentication uses `INGEST_API_KEY` or the local development key. No real payments are made.

## Run the tools locally

Java 17 is required for host-side Maven execution. Real Spark tests are intended for Linux; integration tests require a working Docker daemon.

```bash
# Unit/MVC tests and per-module coverage reports
./mvnw -B -ntp -Pcoverage verify

# Full Java toolchain: real Kafka/Mongo containers and Spark execution
./mvnw -B -ntp -Pintegration,spark-tests,coverage verify

# API and event contracts
python -m pip install -r tests/contracts/requirements.txt
python scripts/validate_contracts.py

# Browser behavior for the API workspace and hosted snapshot mode
npm ci --prefix tests/dashboard
npx --prefix tests/dashboard playwright install chromium
npm test --prefix tests/dashboard

# Actual pipeline and recovery
docker compose up -d --build
python scripts/e2e.py --with-recovery --timeout 300

# k6 installed from the official release, against the local running stack
mkdir -p artifacts
k6 run tests/performance/ingestion.k6.js
```

On Windows, use `mvnw.cmd` for Maven. Prefer an ASCII checkout path for host-side coverage: the observed Windows Surefire launch misencoded a non-ASCII JaCoCo output path. The full GitHub Actions workflow runs on Linux and does not require Docker Desktop to work on the machine used to browse the repository.

Coverage HTML is generated at `services/api/target/site/jacoco/index.html` and `services/streaming/target/site/jacoco/index.html`. Reports show the observed coverage; the project does not claim an unmeasured percentage or exclude uncovered production code to improve the number.

## Read and reproduce reports

In **Actions → verify → the run**, open **Quality gate** for the consolidated job summary. Download `consolidated-test-report`, `java-test-reports`, `jacoco-coverage`, `dashboard-test-reports`, `contract-test-reports`, `pipeline-evidence` and `kubernetes-evidence` for the underlying results. The Pages workflow retains `published-site-evidence` with a JSON report and desktop/mobile screenshots.

```bash
gh run download RUN_ID --repo wxw2002a/pulseguard --dir artifacts/test-results
python scripts/test_report.py --artifacts artifacts/test-results --output artifacts/test-report.md
```

Use a fresh download directory for each run so results from different commits are not mixed. The script reports incomplete artifacts as missing, not passing. Historical measured results are documented separately in [the verification record](verification.md).

Tool behavior and configuration references: [JUnit](https://junit.org/junit5/docs/current/user-guide/), [Testcontainers](https://java.testcontainers.org/), [Playwright](https://playwright.dev/docs/intro), [JaCoCo Maven integration](https://www.jacoco.org/jacoco/trunk/doc/maven.html), and [k6 thresholds](https://grafana.com/docs/k6/latest/using-k6/thresholds/).
