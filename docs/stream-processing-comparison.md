# Stream Processing Comparison: Kafka Streams, Flink, and Spark (for this project)

This repo makes two different stream-processing choices for two similarly-shaped jobs: `flink`
consumes `playground.https-sessions` and writes to Cassandra + MongoDB using **Apache Flink**;
`dynamo` consumes the same topic and writes to DynamoDB using **Kafka Streams**. Neither uses
**Spark Structured Streaming**, which this article covers anyway because it's the third option
you'll actually be choosing between in most real decisions. This is the article for "which one do
I reach for," grounded in what actually broke and worked while building the two jobs in this repo.

## 1. What each one fundamentally is

| | Kafka Streams | Apache Flink | Spark Structured Streaming |
|---|---|---|---|
| Deployment model | a **library** — your app *is* the cluster node | a **cluster runtime** — job submitted to a Flink cluster (or embedded, see below) | a **cluster runtime** — job submitted to a Spark cluster |
| Minimum infra beyond Kafka | none — just your JVM app | a JobManager + TaskManager(s), or an embedded `MiniCluster` | a driver + executors (standalone, YARN, or Kubernetes) |
| Scaling unit | Kafka partitions, via consumer-group rebalancing | operator parallelism, independent of source partitioning in principle | Kafka partitions, similar to Kafka Streams |
| Processing model | true per-record streaming | true per-record streaming | **micro-batch** (or "continuous" experimental mode, rarely used) |
| State backend | local RocksDB + changelog topic | local RocksDB (or heap) + checkpoint to durable storage | in-memory, checkpointed to durable storage |
| This repo's usage | `dynamo` module | `flink` module | not used |

The deployment-model row is the one that actually drives the decision in practice. Kafka Streams
being "just a library" is why the `dynamo` module has zero additional infrastructure in
`docker-compose.yml` beyond the Kafka broker itself — no JobManager, no cluster to run. Flink needs
somewhere to run its cluster; this repo sidesteps that with an embedded approach (§2).

## 2. How this repo actually runs Flink — and why that's not how you'd run it in production

`HttpsSessionFlinkJob` calls `StreamExecutionEnvironment.getExecutionEnvironment()` inside a
Spring Boot `CommandLineRunner`. With no cluster configured, Flink starts an **embedded
`MiniCluster`** inside the same JVM as the Spring Boot app — see `docs/flink-runtime.md` for the
full detail on serialization and parallelism implications of this choice. It's a legitimate way to
run Flink for local development and this kind of playground, but production Flink deployments run
a real cluster (standalone, Kubernetes operator, or a managed service) with the job submitted to
it, precisely so the job's lifecycle is decoupled from any one JVM's lifecycle and so TaskManagers
can be added independently of the submitting application.

Kafka Streams has no equivalent "embedded vs. real cluster" distinction — the library mode *is*
the production mode. Scaling out means running more instances of the same JAR, all in the same
`application.id` consumer group.

## 3. State: what "stateful" costs you in each

Neither job in this repo is stateful in the Flink/Spark sense (no windowing, no joins across
records) — both are effectively per-record transformations, so this section is about what you'd be
signing up for if you added, say, a windowed aggregate.

| | Kafka Streams | Flink | Spark Structured Streaming |
|---|---|---|---|
| Local state store | RocksDB (or in-memory), per task | RocksDB (or heap), per operator subtask | in-memory, spills to disk under pressure |
| Fault tolerance | changelog topic (Kafka itself is the durable log) | periodic checkpoints to durable storage (S3, HDFS, etc.) | checkpoints to durable storage |
| Rebalance/rescale cost | state store must be rebuilt from changelog on a new node — can be slow for large state | Flink can redistribute state via key-groups during rescaling with less full-rebuild cost | executor loss triggers stage recompute from the last checkpoint |
| Exactly-once scope | Kafka-to-Kafka only (`exactly_once_v2`) — does **not** extend to external side effects like a DynamoDB write | end-to-end with `TwoPhaseCommittingSink`-capable sinks (Iceberg's connector is an example of this pattern, see `docs/delivery-semantics.md` §4/§7) | end-to-end for idempotent/transactional sinks Spark supports natively |

The `dynamo` module's design directly demonstrates the Kafka Streams exactly-once caveat: even with
`exactly_once_v2` enabled, a `DynamoSessionAggregateWriter.update()` call inside `.foreach()` is a
plain side effect Kafka Streams knows nothing about — a crash between the DynamoDB write and the
offset commit is an ordinary at-least-once redelivery from DynamoDB's point of view, which is
exactly why the aggregate table's `ADD` counters aren't idempotent (`docs/dynamodb-tutorial.md`
§2). Flink's `TwoPhaseCommittingSink` interface (used by Kafka's own transactional sink, and by
Iceberg's Connect sink via a different but analogous mechanism) is the pattern that actually closes
this gap — neither of this repo's Flink sinks (`CassandraHttpsSessionSink`,
`MongoHttpsSessionSink`) implements it, so the `flink` module has the same class of gap as `dynamo`,
just for a different reason (no checkpointing at all — `docs/delivery-semantics.md` §2).

## 4. Failure handling: the gotcha this repo actually hit

Building the `dynamo` module surfaced a genuinely non-obvious Kafka Streams behavior worth stating
plainly: **if the source topic doesn't exist yet when the Streams client starts consuming, the
default behavior is a full client shutdown (`SHUTDOWN_CLIENT`), not a wait-and-retry.**

```
org.apache.kafka.streams.errors.MissingSourceTopicException: Missing source topics. [playground.https-sessions]
...
Encountered the following exception during processing and the registered exception handler
opted to SHUTDOWN_CLIENT. The streams client is going to shut down now.
```

This is a sharp edge specific to Kafka Streams' topology-validation-on-rebalance design — a plain
`KafkaConsumer.subscribe()` would just keep polling and pick the topic up once it exists. The
practical fix in this repo: make sure something has produced to `playground.https-sessions` (or
otherwise create the topic) before starting `./gradlew :dynamo:bootRun` — see
`docs/dynamodb-tutorial.md` §6 and `DEPLOYMENT.md` §10. This is exactly the class of failure
`HttpsSessionDynamoConfig` guards against by registering a `StreamsUncaughtExceptionHandler` set to
`REPLACE_THREAD` (instead of the default `SHUTDOWN_CLIENT`) — the stream thread restarts instead of
killing the whole client, though topic-existence checks in deployment tooling are still the better
fix for this specific gotcha.

Flink and Spark don't have a direct equivalent — both are typically deployed with the topic
already provisioned as part of the job's declared source configuration, and neither treats a
missing topic as an unrecoverable client-level failure by default (Flink's `KafkaSource` retries
metadata fetches; it doesn't shut down the whole job).

## 5. Operational shape: what you're actually signing up to run

| | Kafka Streams | Flink | Spark Structured Streaming |
|---|---|---|---|
| Deploy artifact | a normal app (JAR, container, whatever you already deploy) | a job submitted to a cluster you also operate | a job submitted to a cluster you also operate |
| Scaling operation | start/stop instances, like any stateless-ish service | resize the cluster and/or job parallelism, often needs a savepoint/restart | resize the cluster; streaming jobs typically need a restart to change parallelism |
| Monitoring surface | JMX metrics like any Kafka client | Flink Web UI, checkpoint/backpressure metrics | Spark UI, micro-batch duration/backlog metrics |
| Team fit | teams already comfortable operating Kafka-consuming services | teams that need complex event-time/windowing semantics and already run Flink | teams already running Spark for batch, extending into streaming |
| Latency floor | lowest — true per-record | low — true per-record, some overhead from network/serialization | higher — bounded below by micro-batch interval, even at its fastest ("continuous" mode is immature/limited) |

## 6. When to reach for each, in production

**Kafka Streams — when the job is "consume Kafka, do something per-record or with modest local
state, done."** No new cluster to operate, deploys like any other service, lowest latency. The
`dynamo` module is exactly this shape: read an event, write it to two places, no windowing, no
joins across streams. Wrong fit the moment you need complex event-time semantics, large
joins/windows that outgrow what a partition's local state store can hold, or true exactly-once
into an arbitrary external system (you'd be building the two-phase-commit machinery yourself).

**Flink — when you need rich stream semantics:** event-time processing with watermarks, complex
windowing, CEP (complex event processing), or end-to-end exactly-once into a sink that supports
`TwoPhaseCommittingSink`. The `flink` module in this repo barely uses any of this — it's a
one-record-in-one-record-out pipeline, which is arguably more than Flink is needed for here (it's
included in this repo specifically to demonstrate the embedded-cluster pattern and the custom
`Sink2` writer contract; see `docs/flink-runtime.md` and `docs/delivery-semantics.md` §4). Wrong
fit if your team doesn't want to operate a Flink cluster (or the embedded-cluster pattern shown
here) and your job doesn't actually need Flink's advanced semantics.

**Spark Structured Streaming — when streaming is one part of a broader Spark-centric platform**,
especially where the same team/codebase also does batch ETL and wants one unified engine and API
for both. The micro-batch model is the right trade when sub-second latency isn't required and the
operational win of "one engine for batch and streaming" outweighs Flink's lower latency and richer
streaming-native semantics. Wrong fit for genuinely low-latency requirements — the micro-batch
floor is a hard architectural constraint, not a tuning knob.

## 7. The decision this repo actually made, explained

Given the `dynamo` module's requirements — consume one topic, write per-record to two DynamoDB
tables, no windowing or joins — Kafka Streams was the minimal-infrastructure choice: no new
cluster, no embedded-runtime workaround like Flink needed (`docs/flink-runtime.md`), and a
deployment shape (`./gradlew :dynamo:bootRun`, a plain Spring Boot app) consistent with how `kafka`
and `flink` already run in this repo. Had the requirement instead been "join `https-sessions`
against a second stream within a time window" or "guarantee true exactly-once into DynamoDB,"
Flink would have been the better-justified choice — Kafka Streams could still do the join, but
you'd be reaching for its lower-level Processor API rather than the DSL's `foreach`, and you'd
still need to hand-build the two-phase-commit logic for the DynamoDB side that
`TwoPhaseCommittingSink` gives you for free in Flink.
