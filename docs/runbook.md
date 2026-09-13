# Operations and demonstration runbook

## Start, inspect, stop

```bash
docker compose up -d --build
docker compose ps -a
docker compose logs --tail 100 api streaming
python scripts/e2e.py --timeout 300
docker compose stop
```

`stop` preserves containers and named volumes. `docker compose down` removes this project's containers/network and retains its named volumes. **`docker compose down -v` deletes this project's Kafka, MongoDB and checkpoint data**; use only for an intentionally disposable reset. CI uses it only on its ephemeral runner.

The first image build needs Maven Central and container registry access. Runtime Kafka connector dependencies are baked into the Spark image, so a running stream does not download Maven packages.

## Demonstrate repeatable acceptance

```bash
python scripts/load_generator.py --count 120 --rate 10 --save-events artifacts/events.jsonl
python scripts/load_generator.py --replay artifacts/events.jsonl --rate 10 --report artifacts/replay-report.json
```

The second command retains the original IDs and timestamps. Its response count may be 120, but unique transaction count must not increase. Editing an amount while retaining an existing transaction ID should produce HTTP 409. Use a new generated run for fresh events.

## Demonstrate stream recovery

```bash
python scripts/e2e.py --with-recovery --timeout 300
```

The test creates isolated synthetic accounts, observes all three rules, checks an exact six-event/3,000-minor-unit window, injects a duplicate directly into Kafka, restarts Spark with the same checkpoint, then requires a new alert **and** a fresh window update from the restarted process. It confirms the original duplicate did not change counts or create extra alerts. It writes `artifacts/e2e-report.json`.

Ordinary restarts retain checkpoint and projection volumes. Do not erase checkpoints to fix a transient failure. If a schema/query change requires rebuilding state, create new checkpoint directories and a fresh projection database, replay retained data, compare counts and switch readers once the rebuild is complete.

## Broker outage

Only run these operations against this project's disposable local Compose stack:

```bash
docker compose stop kafka
# The durable outbox can accept records through the direct API while MongoDB is available.
python scripts/load_generator.py --count 10 --rate 2
docker compose start kafka
```

Outbox retries are bounded by a capped backoff. Watch `/api/v1/overview` and `pulseguard_outbox_retries_total`; `pendingDelivery` should eventually return to zero. The API readiness check includes Kafka, so it reports unavailable during the outage. Kubernetes removes unready API pods from service routing; direct Compose HTTP acceptance and Kubernetes routing availability have different behavior. Spark may terminate if Kafka is unavailable long enough; its restart policy and retained checkpoint provide recovery.

## Monitoring

```bash
docker compose --profile monitoring up -d
```

The provisioned Grafana dashboard covers HTTP rate/latency, JVM memory and outbox counters. Spark progress logs expose input/processing rate, batch duration, watermark and state rows; inspect the Spark UI for task details. A healthy API alone does not establish that streaming is advancing. For this demo, the e2e new-event assertion is the strongest proof of progress.

Important operational signals to add for a real environment: oldest pending outbox age, Kafka retention headroom, per-partition lag, watermark lag, state memory and stalled-query alerts. These should be measured and implemented before claiming a complete production monitoring system.

## Configuration

| Variable | Default in Compose | Use |
|---|---|---|
| `INGEST_API_KEY` | `local-dev-key` | POST ingestion and PATCH review authentication |
| `API_PORT` | `8080` | Loopback API port |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:19092` | Internal Kafka listener |
| `MONGODB_URI` | `mongodb://mongodb:27017/pulseguard` | Database connection |
| `CHECKPOINT_DIR` | `/var/lib/pulseguard/checkpoints` | Persistent streaming state |
| `GRAFANA_ADMIN_PASSWORD` | `local-grafana-only` | Development Grafana account |

Copy `.env.example` to `.env` for overrides; `.env` is ignored by Git. A custom transaction topic must be created and configured consistently in both applications. Internal hostnames are meaningful inside the Compose network; the host Kafka listener is `localhost:9092`.

## Docker Desktop fails before containers start

During initial local development on Windows, Docker Desktop 4.45.0 failed before starting its Linux engine with:

```text
initializing Inference manager ... dockerInference ... bind:
Only one usage of each socket address ... is normally permitted.
```

This is a Docker Desktop startup failure rather than a PulseGuard container failure. Do not reset to factory defaults as a routine workaround: that can erase existing Docker data. Quit the error dialog, capture the local backend logs, and resolve Desktop independently or run the repository in a working Linux Docker environment. [Docker Desktop troubleshooting](https://docs.docker.com/desktop/troubleshoot/overview/) describes supported diagnostic steps. The project can still compile and run pure Java tests on Windows; GitHub Actions provides a clean Linux container environment.

The real file-stream checkpoint test also needs a supported Hadoop filesystem runtime. Windows may fail with `NativeIO$Windows.access0` when Hadoop native binaries are absent. Run `-Pspark-tests` on Linux/CI rather than downloading unverified native DLLs. Default Java unit tests do not require those binaries.

## Evidence before performance claims

Record the Git commit, resource limits, CPU/RAM, topology, input cardinality, partition count and warm-up period. Retain raw JSON reports and describe whether latency measures HTTP acceptance or completed risk analysis. The provided generator reports the former. It deliberately does not supply invented TPS, percentile or fraud-accuracy targets.
