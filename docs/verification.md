# Verification record

This file records observed results, separately from the deployment reference and future capacity work.

## Local environment

- Windows 11, Microsoft OpenJDK 17.0.16, Maven 3.9.11.
- Both Java modules compile and package. The current API's 28 unit/MVC tests and streaming module's 21 unit tests pass locally.
- Real local Spark parsing and batch aggregation execute on the JVM. The file-stream checkpoint test requires Linux/Hadoop native support and was blocked on Windows by `NativeIO$Windows.access0`.
- Compose/Kustomize/configuration rendering and Kafka connector dependency resolution were checked.
- Docker Desktop failed during its own inference-manager startup, before the project containers could launch. No local full-stack success is claimed.

## Continuous integration

The [verify workflow](https://github.com/wxw2002a/pulseguard/actions/workflows/ci.yml) contains independent jobs:

1. Java unit/MVC tests, real local Spark event-time/checkpoint tests, and Testcontainers MongoDB/Kafka integration tests.
2. Build and start the entire Compose stack, exercise the API/rules and original evidence, inject Kafka duplicates/invalid records, and restart Spark against retained checkpoints. Require both new event alerts and window updates after restart, preserve review history, then stop/recover Kafka to verify the durable outbox. Produce a measured HTTP load report and capture the dashboard against the running API.
3. Chromium checks for the dashboard's live/preview distinction, filters, review actions, authenticated synthetic scenarios, escaping, mobile layout and failure states.

Each run uploads Maven test reports and pipeline logs/reports. Consult the linked run for its current status; a workflow definition alone is not evidence that its jobs have passed. A pinned passing-run reference will be added after the first successful execution.

The separate manually dispatched `kubernetes-smoke` workflow creates an actual one-node kind cluster, loads the application images, applies the Kustomize development stack, waits for rollout/readiness and exercises the same HTTP/Kafka/Spark flow. This validates the single-pod development configuration when it passes; it does not validate native multi-node Spark execution.

## Scope that is not established by these tests

- Native multi-node Kubernetes Spark execution, high availability or failure of a real storage node.
- Production throughput, end-to-end latency percentiles or a maximum supported account cardinality.
- Fraud classification accuracy or financial benefit.
- Compliance, a security audit, or safe public deployment with real transaction data.

The dashboard screenshot explicitly uses synthetic sample data. Its counters and graph are not measured runtime evidence.
