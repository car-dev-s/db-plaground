# DynamoDB Tutorial (for this project)

DynamoDB is a **managed, partition-key-hashed key-value/document store** with no notion of
schema, joins, or ad-hoc query beyond its declared keys and indexes. In this repo it runs as
`amazon/dynamodb-local` (port 8000, data bind-mounted to `${MOUNT_ROOT}/dynamodb`) — a single-JVM
emulator with no partitioning, no WCU/RCU accounting, and no throttling, so (like Cassandra's
single-node setup) the operational characteristics that actually define DynamoDB in production are
invisible here. The data model decisions below only make sense in light of what real DynamoDB
does with them.

Unlike the other three stores, DynamoDB is fed by its own **fully isolated `dynamo` module**
(Kafka Streams, not Flink or Kafka Connect) — see `docs/stream-processing-comparison.md` for why
that module exists as a separate technology choice rather than a third Flink sink.

## 1. Two tables, two different write shapes

`HttpsSessionDynamoTopology` fans every event out to two independent DynamoDB writes:

```java
void writeToBothTables(String key, HttpsSessionEvent event) {
    CompletableFuture<Void> aggregateWrite = start(() -> aggregateWriter.update(event));
    CompletableFuture<Void> eventWrite = start(() -> eventWriter.put(event));

    RuntimeException aggregateFailure = await(aggregateWrite, "aggregate", event.getSourceIp());
    RuntimeException eventFailure = await(eventWrite, "event", event.getSourceIp());

    if (aggregateFailure != null || eventFailure != null) {
        throw new HttpsSessionDynamoWriteException(event.getSourceIp(), aggregateFailure, eventFailure);
    }
}
```

| Table | Partition key | Sort key | Write | Semantics |
|---|---|---|---|---|
| `https_session_aggregates` | `sourceIp` | *(none)* | `UpdateItem` | merge/upsert — running per-IP aggregate |
| `https_session_events` | `sourceIp` | `timestampIso` | `PutItem` | whole-item replace — one row per event |

Both writes are started concurrently and attempted independently — a failure in one
never skips the other, and neither has to wait on the other to start
(`HttpsSessionDynamoTopology.start`/`await`, catches per-writer). This is deliberate:
the two tables answer different questions ("what does source X look like right now"
vs "show me every event"), and there's no reason a failure answering one should block
answering the other.

Events with a missing/blank `sourceIp` or `timestampIso` — both required key
attributes — are filtered out before either write is attempted, and logged as a
warning.

## 2. `https_session_aggregates`: atomic counters via `UpdateItem`

```java
.updateExpression(
    "ADD eventCount :one, totalBytesSent :bytesSent, totalBytesReceived :bytesReceived "
  + "SET lastSeen = :lastSeen, lastDomain = :domain, lastMethod = :method, lastStatusCode = :statusCode")
```

`ADD` is DynamoDB's atomic increment — no read-modify-write race, no optimistic-lock retry loop
needed, unlike almost every other store's "increment a counter" pattern. This is one of DynamoDB's
genuine strengths: single-item atomic counters are a first-class primitive, not a workaround.

**The `ADD` is not idempotent.** A replayed/duplicated event (Kafka's at-least-once delivery, or a
Kafka Streams thread restart after a partial failure — see `docs/delivery-semantics.md`) increments
`eventCount` and the byte totals a second time. The `SET` fields (`lastSeen`, `lastDomain`, etc.)
are last-write-wins and don't have this problem — re-applying the same value is harmless — but the
counters genuinely double-count. This is the same class of gotcha as MongoDB's non-idempotent
`insertOne` in `docs/delivery-semantics.md` §5, with a different mechanism: MongoDB accumulates
extra *rows*, this table accumulates an inflated *count inside one row*, which is arguably harder
to notice because the row count itself never looks wrong.

## 3. `https_session_events`: why it needs a compound key at all

Unlike Cassandra's `((source_ip), timestamp)` or MongoDB's generated `_id`, this table's key is
`sourceIp` (partition) + `timestampIso` (sort) — not because that's DynamoDB's default shape, but
because a *single* partition key of `sourceIp` alone would mean every event from the same source IP
overwrites the previous one on `PutItem` (DynamoDB items are uniquely identified by their full key,
partition + sort together). Adding `timestampIso` as a sort key is what turns this into an
append-only event log rather than a single mutable row per source.

Contrast this with the aggregate table, which deliberately has *no* sort key — there, "one item per
partition key" is exactly the point.

## 4. The hot-partition-key problem, and why this repo can't show it to you

`https_session_aggregates` has no sort key, so **every event for a given `sourceIp` writes to the
same DynamoDB item, forever.** On real DynamoDB this is a scaling ceiling, not just a performance
detail: per-partition-key throughput on-demand tables is capped around 1,000 WCU/sec sustained
(roughly 3,000 burst), *per key*, regardless of overall table capacity. A single high-volume source
IP throttles on this table while `https_session_events` — spread across `timestampIso` sort
keys — keeps accepting writes fine, because DynamoDB partitions storage (and throughput) by full
key, not just by table.

On-demand capacity mode adds a further wrinkle worth knowing before relying on it: it scales to
roughly double the prior observed peak, not instantly to any rate, so a cold-start burst or a
sudden traffic spike can throttle briefly even with good key cardinality across the table as a
whole.

**None of this is visible against `dynamodb-local`.** It's a single-JVM process with no partition
model, no WCU/RCU accounting, and no per-key throttling — load-testing against it only exercises
correctness, not capacity behavior. If you need to validate throughput/hot-key characteristics,
that has to happen against real DynamoDB (or at minimum a load generator paired with CloudWatch's
per-table/per-key throttle metrics), not this repo's local emulator.

## 5. Why there's no Trino query for this table

`docs/trino-tutorial.md` and `docs/query-federation.md` cover `iceberg`, `cassandra`, and
`mongodb` as catalogs queryable side by side. DynamoDB is deliberately absent from that list —
Trino has no built-in DynamoDB connector, unlike the other three engines. Querying DynamoDB data
alongside the rest of this pipeline would require either exporting to Iceberg/S3 (e.g. via DynamoDB
Streams + a Lambda/Glue export job) or a third-party connector, neither of which this repo sets up.
This is a real architectural asymmetry worth knowing before assuming "every store in this repo is
queryable from one place" — three are; DynamoDB isn't.

## 6. Useful commands

```bash
# List tables
docker compose exec dynamodb-local-init aws dynamodb list-tables --endpoint-url http://dynamodb-local:8000

# Scan a table (fine at playground volume; a full-table scan is expensive on real DynamoDB)
docker compose exec dynamodb-local-init aws dynamodb scan \
  --endpoint-url http://dynamodb-local:8000 --table-name https_session_aggregates

docker compose exec dynamodb-local-init aws dynamodb scan \
  --endpoint-url http://dynamodb-local:8000 --table-name https_session_events

# Point lookup on the aggregate table
docker compose exec dynamodb-local-init aws dynamodb get-item \
  --endpoint-url http://dynamodb-local:8000 --table-name https_session_aggregates \
  --key '{"sourceIp": {"S": "10.90.109.45"}}'

# Run the Kafka Streams sink locally (needs playground.https-sessions to already exist — see
# docs/stream-processing-comparison.md's Kafka Streams gotcha)
./gradlew :dynamo:bootRun

# Reset local DynamoDB entirely (drops the emulator's own state, not just table contents)
docker compose down dynamodb-local && docker compose up -d dynamodb-local dynamodb-local-init
```
