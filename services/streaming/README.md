# PulseGuard streaming engine

Java 17 / Spark Structured Streaming 3.5.8 consumes `transactions.v1` and writes MongoDB `alerts`, `windows`, and `dead_letters`. Start it through the repository Docker Compose setup; the shaded application JAR includes MongoDB's sync driver, while the Spark image supplies Spark, Scala, Jackson, and the Kafka connector.

```bash
mvn -pl services/streaming -am test
# A real Spark runtime, file streaming source and persistent state-store tests (Linux):
mvn -pl services/streaming -am -Pspark-tests test
```

The `spark-tests` suite verifies actual SQL execution, cross-batch duplicate delivery, recovery from the same checkpoint, currency separation, and late-window eviction. Ordinary tests cover malformed records, exact integer money, rule thresholds, deterministic IDs and insert-only Mongo updates. The Spark suite is opt-in because Hadoop's filesystem implementation on Windows normally requires native helpers. It does not require Kafka or MongoDB; the repository end-to-end smoke test covers the services together.

## Rules and output

| Rule | Trigger | Severity / score |
|---|---|---|
| `HIGH_VALUE` | One transaction >= 500,000 minor units | HIGH / 90 |
| `VELOCITY` | >= 5 distinct transactions per account, currency and UTC minute | MEDIUM / 75 |
| `CARD_TESTING` | >= 5 distinct transactions <= 1,000 minor units in that same window | HIGH / 85 |

These are explainable demonstration heuristics, not a trained fraud model or calibrated probabilities. Thresholds apply independently in each supported currency; the engine performs no foreign exchange conversion. CARD_TESTING means a suspicious burst of small payments, not confirmed card abuse.

`HIGH_VALUE:<transactionId>` identifies an event alert. `<rule>:<accountId>:<currency>:<windowStartEpochMillis>` identifies a window alert. All alerts contain `rule`, `severity`, `score`, `accountId`, `currency`, `eventTime`, `createdAt`, `status: OPEN`, and a `reasons` list. Event alerts add `transactionId` and `amountMinor`; window alerts add `windowStart`, `windowEnd`, `transactionCount`, and `totalAmountMinor`. Alert evidence captures the first qualifying observation. Analyst changes to `status` and other review fields survive replays because the entire alert uses `$setOnInsert`.

Window documents use `<accountId>:<currency>:<windowStartEpochMillis>` as `_id`. They hold `accountId`, `currency`, `windowStart`, `windowEnd`, `transactionCount`, `totalAmountMinor`, `highValueCount`, `smallAmountCount`, and `updatedAt`. Times are BSON dates in UTC and money is BSON int64 in minor units. Invalid records create one dead letter per Kafka `topic:partition:offset`, including the validation reason and a payload excerpt limited to 2,048 characters. Payloads are never logged by this application. Apply retention and access controls to dead letters because excerpts may still contain sensitive data.

## Delivery and state guarantees

Two independent queries read the source with separate `events-v1` and `windows-v1` checkpoints. Event validation, event alerts, and dead letters are stateless. Window aggregation uses one stateful operator: a one-minute event-time window with a two-minute watermark and `collect_set(struct(transactionId, amountMinor))`.

The producer contract guarantees that each transaction ID has one immutable payload. The API rejects conflicting reuse of an ID. Under that contract, the set removes duplicates both within and across micro-batches, including outbox retries. A producer that bypasses the API must enforce the same contract; reusing an ID with different amount/account/currency/time is unsupported and can corrupt analytics. Kafka ACLs should restrict writes to authorized producers.

The set retains one ID and amount per distinct transaction per open account/currency window. State size therefore grows with distinct activity and watermark lag; this is an explicit exact-deduplication memory tradeoff. The watermark eventually removes old windows, but is driven by incoming event time, not wall-clock time. Idle sources do not automatically finalize every window. Extremely busy accounts need load testing and a reviewed capacity limit or another deduplication/state strategy.

Update mode exposes current window snapshots every five-second micro-batch. Kafka/outbox delivery and Spark `foreachBatch` effects are **at least once**. Mongo writes converge through deterministic upserts: windows replace absolute values rather than incrementing counters, and alerts/dead letters insert once. A failed Mongo operation fails its Spark task. If task retries are exhausted, the query exits; the runtime must restart the process with its checkpoint so the uncommitted micro-batch is replayed. Writes are chunked to 500 operations using one Mongo client per nonempty executor partition and majority write concern. No production sink collects records onto the driver.

This is not an atomic transaction spanning Kafka, the Spark checkpoint, multiple Mongo collections or the two queries. A failure can temporarily expose a window before its alert, or an event alert before its window. Restart with the same persistent checkpoint to converge. Never delete a checkpoint against existing output and describe it as a safe resume: historical rebuilds can temporarily regress snapshots and retain previously raised alerts. For a deliberate rebuild, use isolated output collections/database and checkpoint paths, validate results, then switch readers.

Events arriving after a window is older than Spark's watermark can be dropped from **window** analytics; stateless event rules still evaluate them. These valid late events do not enter `dead_letters`. Spark's `streaming_progress` JSON logs include watermark/state metrics and `numRowsDroppedByWatermark`. Events more than 30 seconds ahead of their Kafka record timestamp are invalid; that guard is smaller than the two-minute watermark delay and reduces accidental watermark poisoning. Use broker `LogAppendTime` and restrict producer access if Kafka timestamps must be trusted against hostile producers.

## Runtime configuration

| Environment | Default |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `TRANSACTION_TOPIC` | `transactions.v1` (must match the API and broker topic provisioning) |
| `MONGODB_URI` | `mongodb://localhost:27017/pulseguard` |
| `CHECKPOINT_DIR` | `/var/lib/pulseguard/checkpoints` |
| `MAX_OFFSETS_PER_TRIGGER` | `5000` per query |
| `SPARK_LOG_LEVEL` | `WARN` |

`spark-submit --master` controls the execution target; a missing Spark master defaults to `local[2]`. Configure shared durable checkpoint storage when using remote executors. Kafka starts at `earliest` only for a new checkpoint; subsequent starts restore committed offsets. `failOnDataLoss=true` surfaces expired/missing Kafka offsets instead of silently skipping data. Only one instance may own each checkpoint directory.

Further reading: [Spark Structured Streaming 3.5.8](https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html), [Spark Kafka integration](https://spark.apache.org/docs/3.5.8/structured-streaming-kafka-integration.html), [MongoDB Java bulk writes](https://www.mongodb.com/docs/drivers/java/sync/v5.5/crud/bulk/).
