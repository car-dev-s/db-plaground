# Delivery Semantics & Failure Modes (for this project)

One Kafka topic fans out into four stores over **three completely independent consumers** — a
Flink job (Cassandra + MongoDB), a Kafka Connect worker (Iceberg), and a Kafka Streams topology
(DynamoDB, in the `dynamo` module). They do not share offsets, transactions, or failure handling.
This article is about what each one actually guarantees when something crashes, and why the four
stores can legitimately disagree afterwards.

If you only read one section, read §5.

## 1. The three paths, side by side

| | Flink path (`flink` module) | Kafka Connect path (`kafka-connect`) | Kafka Streams path (`dynamo` module) |
|---|---|---|---|
| Consumer group | `playground-flink-sink` | `connect-https-sessions-iceberg-sink` (managed by Connect) | `playground-dynamo-streams` |
| Offset storage | *none* — see §2 | `_connect-offsets` topic | Kafka consumer-group offsets, committed on `commit.interval.ms` |
| Restart behaviour | replays the **whole topic** from `earliest` | resumes from committed offsets | resumes from committed offsets |
| Commit protocol | none — per-record writes | two-phase commit over a control topic | none — per-record writes, offset commit decoupled from the DynamoDB write |
| Effective guarantee | **at-least-once at best**, no crash recovery | **exactly-once** into the Iceberg table | **at-least-once into DynamoDB** — Kafka-internal `exactly_once_v2` doesn't extend to the external write, see §10 |
| Write visibility | immediate | batched, every `iceberg.control.commit.interval-ms` (10s here) | immediate |

None of the three paths know the others exist. There is no distributed transaction spanning
Cassandra, MongoDB, Iceberg, and DynamoDB — and there is no practical way to build one across four
engines this different. Accepting per-store guarantees and reconciling afterwards (see
`docs/cross-store-consistency.md`) is the normal industry answer, not a shortcut taken here.

## 2. The Flink job has no checkpointing — what that actually means

`HttpsSessionFlinkJob` never calls `env.enableCheckpointing(...)`. Checkpointing is Flink's
entire fault-tolerance mechanism: it periodically snapshots every operator's state *and* the
Kafka source's read offsets into a coordinated, consistent barrier-aligned snapshot. Without it:

- **Kafka offsets are never committed back to the broker.** Flink's `KafkaSource` commits
  offsets *on checkpoint*, not on record consumption — that is deliberate, because committing
  earlier would break exactly-once. No checkpoints means no commits, ever.
- **There is no recovery point.** If a sink writer throws, Flink restarts the job from the
  configured starting offsets, not from where it got to.
- **`setGroupId("playground-flink-sink")` is nearly decorative.** In a Flink `KafkaSource` the
  group id is used for metrics and (optionally) offset commits — Flink does *not* use Kafka's
  consumer-group rebalance protocol to assign partitions. Flink's own `SplitEnumerator` assigns
  splits to subtasks. You cannot scale this job by starting a second instance in the same group;
  you would get two independent full readers.

Combined with the next point, this is the single biggest correctness gap in the pipeline.

## 3. `OffsetsInitializer.earliest()` vs `committedOffsets(...)`

```java
.setStartingOffsets(OffsetsInitializer.earliest())
```

`earliest()` is **unconditional**. It does not consult committed offsets; it always starts at the
beginning of every partition. Every `./gradlew :flink:bootRun` re-reads the entire topic and
re-writes every record into Cassandra and MongoDB.

The offset-aware variant is:

```java
.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
```

which means "resume from the committed offset, or start from earliest if there is none" — the
behaviour most people assume `earliest()` has. Note this is only useful *together* with
checkpointing (§2); without checkpoints there are never any committed offsets to resume from, so
it degrades back to a full replay anyway.

Contrast with the Connect side, where `consumer.override.auto.offset.reset: earliest` is exactly
this "only if no committed offset" semantic — the same word meaning two different things in two
tools is a genuinely common source of confusion.

## 4. The `Sink2` writer contract, and why the empty `flush()` is (barely) OK

Both sinks implement Flink 2.x's `org.apache.flink.api.connector.sink2.Sink`, whose writer has
three lifecycle methods:

| Method | Contract |
|---|---|
| `write(element, context)` | accept one record; may buffer |
| `flush(boolean endOfInput)` | make all previously accepted records durable — called before every checkpoint and at end of input |
| `close()` | release resources |

`CassandraHttpsSessionSink` and `MongoHttpsSessionSink` both implement `flush()` as a no-op. That
is only correct because both `write()` implementations are **fully synchronous**:
`session.execute(bound)` and `collection.insertOne(document)` block until the server acknowledges.
Nothing is ever buffered, so there is nothing to flush.

That correctness is also the performance problem: one network round-trip per record, no batching,
no pipelining. The idiomatic high-throughput version buffers and uses the async API:

```java
// sketch — Cassandra
private final List<CompletionStage<AsyncResultSet>> inFlight = new ArrayList<>();

public void write(HttpsSessionEvent event, Context context) {
    inFlight.add(session.executeAsync(bind(event)));
    if (inFlight.size() >= BATCH) { awaitAll(); }
}

public void flush(boolean endOfInput) { awaitAll(); }   // now this method earns its keep
```

The moment you make `write()` asynchronous, an empty `flush()` becomes a data-loss bug: Flink
would checkpoint offsets past records still in flight. This is the classic mistake when writing a
custom sink — the contract is "flush makes it durable", and an empty implementation silently
converts at-least-once into *at-most*-once.

`TwoPhaseCommittingSink` (the interface behind Kafka's transactional sink and the Iceberg sink)
is the next tier up: it adds `PrecommitningSinkWriter#prepareCommit` plus a `Committer`, letting
the sink stage writes at checkpoint N and commit them only once the checkpoint globally succeeds.
Neither store here supports the staged-write primitive that would need.

## 5. Replay is idempotent for Cassandra, and *not* for MongoDB

This is the practical consequence of §2 and §3, and the most useful thing in this article.

**Cassandra self-heals.** Every CQL `INSERT` is an upsert — there is no "duplicate key" error in
Cassandra, by design. The table's primary key is `((source_ip), timestamp)`, and the sink writes
every column, so re-inserting the same record writes identical values over identical values. Row
count after ten replays is the same as after one.

**MongoDB accumulates.** `collection.insertOne(document)` supplies no `_id`, so the driver
generates a fresh `ObjectId` per call. The same session record inserted ten times becomes ten
distinct documents. Row count after ten replays is 10x.

So the symptom you will actually observe: run the producer once, then run the Flink job three
times, then compare counts.

```sql
SELECT count(*) FROM cassandra.playground.https_sessions;  -- 100
SELECT count(*) FROM mongodb.playground.https_sessions;    -- 300
```

Cassandra looks "right" here, but note that it is right *for the wrong reason* — the same upsert
behaviour that makes replay harmless also silently swallows genuinely distinct events that
collide on `(source_ip, timestamp)`. See `docs/cross-store-consistency.md` §3; the two effects
are the same mechanism viewed from opposite sides.

The fix for MongoDB is a **deterministic `_id`** derived from the record's natural key, turning
insert into an idempotent upsert:

```java
Document document = new Document()
        .append("_id", event.getSourceIp() + "|" + event.getTimestamp())
        // ... remaining fields
collection.replaceOne(
        Filters.eq("_id", document.get("_id")),
        document,
        new ReplaceOptions().upsert(true));
```

This is the general pattern for making any non-transactional sink replay-safe: *derive the
primary key from the data, never from the write*. It is why Cassandra and Iceberg-with-upsert
tolerate at-least-once delivery, and why append-only sinks without a natural key never can.

## 6. Two sinks on one stream = partial-failure divergence

```java
sessions.sinkTo(new CassandraHttpsSessionSink(...));
sessions.sinkTo(new MongoHttpsSessionSink(...));
```

Calling `sinkTo` twice on the same `DataStream` does **not** duplicate the Kafka read — the source
is read once and its output is broadcast to both sink operators in the job graph. That part is
efficient and correct.

What it does not give you is atomicity. Each record is handed to both writers independently, and
there is no rollback:

- Cassandra write succeeds, MongoDB write throws → the record exists in one store and not the
  other, and the job fails and restarts.
- On restart (§3) the whole topic replays → Cassandra converges (upsert), MongoDB now double-writes
  everything it had already written before the failure.

If MongoDB is down at startup, `MongoClients.create(uri)` will *not* fail immediately — the driver
is lazy and connects in the background — so the failure surfaces on the first `insertOne`, after
the Cassandra sink has already been writing happily.

## 7. The Iceberg path's exactly-once, for contrast

The Iceberg sink connector is the one path here with a real commit protocol, and it is worth
understanding because it shows what the Flink path is missing:

1. Each sink task writes Parquet data files to MinIO but does **not** register them in the catalog.
2. Tasks report their written files, plus their consumed Kafka offsets, over a dedicated Kafka
   **control topic** (`control-iceberg`).
3. A coordinator collects reports from all tasks for a commit interval, then performs one atomic
   Iceberg commit that appends all data files *and* records the offsets in the snapshot metadata.
4. On restart, tasks resume from the offsets stored in the Iceberg snapshot — data files written
   but never committed are orphaned, not double-counted.

That is a textbook two-phase commit: the Kafka offsets and the table contents advance in the same
atomic step, which is precisely what makes exactly-once possible. The costs are visible in the
config: a 10-second commit interval means **query visibility lags writes by up to 10 seconds**,
and every commit produces new small files (see `docs/iceberg-tutorial.md` §4).

`DEPLOYMENT.md` §6 documents the operational fragility of this coordinator during live
reconfiguration — worth reading alongside this.

## 8. Things a senior dev should know going in

**"At-least-once" is only a useful guarantee if your sink is idempotent.** Otherwise it is just a
promise to corrupt your data eventually. Half of the design work in a streaming sink is choosing a
key that makes replay a no-op.

**`acks: all` on a single-broker cluster is a placebo.** `kafka/src/main/resources/application.yml`
sets `acks: all`, but the topic has `ReplicationFactor: 1` and `min.insync.replicas=1` — "all
in-sync replicas" is one node. The setting is correct-by-habit and costs nothing here, but it buys
zero durability until there is more than one broker.

**The topic has one partition.** Confirm with:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic playground.https-sessions
```

That caps *every* consumer's parallelism at 1, regardless of Flink's parallelism setting or
Connect's `tasks.max`. See `docs/flink-runtime.md` §3.

**Failure of the whole job is often better than failure of one record.** Neither sink here catches
exceptions, which means a single bad record kills the job. That is the right default — silently
swallowing write errors is how you get stores that disagree with no signal. When you do need to
tolerate bad records, route them to a dead-letter sink explicitly rather than logging and
continuing.

## 9. Experiments worth running

```bash
# 1. Prove replay is non-idempotent for MongoDB and idempotent for Cassandra.
./gradlew :kafka:bootRun          # produce 100 records
./gradlew :flink:bootRun          # ctrl-C once it idles
./gradlew :flink:bootRun          # again
docker compose exec trino trino --execute \
  "SELECT count(*) FROM cassandra.playground.https_sessions"   # 100
docker compose exec trino trino --execute \
  "SELECT count(*) FROM mongodb.playground.https_sessions"     # 200

# 2. Prove Flink never commits offsets: the group has no committed offsets at all.
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group playground-flink-sink

# 3. Compare with the Connect consumer group, which does commit.
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list
```

Then enable checkpointing in `HttpsSessionFlinkJob` and repeat experiment 2:

```java
env.enableCheckpointing(5000);   // 5s, at-least-once is the default alignment mode
```

Offsets start appearing for the group — and combined with `committedOffsets(...)` from §3, the
restart replay disappears.

## 10. The `dynamo` module: Kafka Streams' exactly-once doesn't cover external writes

`HttpsSessionDynamoTopology.writeToBothTables` runs inside a `KStream.foreach`, calling the AWS
SDK's `DynamoDbAsyncClient` synchronously (`.join()`) for each of two tables. Kafka Streams *does*
support `processing.guarantee=exactly_once_v2`, which gives you atomic, exactly-once semantics for
state stores and Kafka-to-Kafka writes — but that guarantee stops at Kafka's own boundary. A
`.foreach()` side effect like a DynamoDB `UpdateItem`/`PutItem` call is invisible to that
transactional machinery entirely. A crash between the DynamoDB write succeeding and the consumer
offset committing is an ordinary at-least-once redelivery from DynamoDB's point of view — same
record, processed again.

This is *why* `https_session_aggregates`' `ADD`-based counters aren't idempotent
(`docs/dynamodb-tutorial.md` §2): the topology has no way to detect "I already wrote this," because
nothing about Kafka Streams' exactly-once story covers that write. Compare with the Iceberg path's
real two-phase commit (§7) — closing this gap for DynamoDB would mean building the equivalent
staged-write/commit protocol yourself, which is exactly the kind of thing Flink's
`TwoPhaseCommittingSink` interface gives you for free (see `docs/stream-processing-comparison.md`
§3) and Kafka Streams' plain DSL does not.

**A second, unrelated failure mode surfaced while building this module**: Kafka Streams treats a
missing source topic during rebalance as fatal (`MissingSourceTopicException`), and by default
shuts the whole client down rather than retrying — see `docs/stream-processing-comparison.md` §4
for the full trace and the practical workaround (make sure the topic exists before starting
`./gradlew :dynamo:bootRun`).
