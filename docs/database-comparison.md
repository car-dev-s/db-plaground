# Database Comparison: Iceberg, Cassandra, MongoDB, DynamoDB (for this project)

This repo writes the same `https_sessions` event into four different storage engines. That's not
an endorsement of running four databases in production — it's what makes the engines' differences
concrete instead of theoretical. Each per-store tutorial (`docs/cassandra-tutorial.md`,
`docs/mongodb-tutorial.md`, `docs/iceberg-tutorial.md`, `docs/dynamodb-tutorial.md`) covers one
engine in depth; this article is the side-by-side.

## 1. What each one actually is

| | Iceberg (+ MinIO/S3) | Cassandra | MongoDB | DynamoDB |
|---|---|---|---|---|
| Category | table format over object storage | wide-column store | document store | key-value / document store |
| Write path here | Kafka Connect sink (batched commits) | Flink sink (sync, per-record) | Flink sink (sync, per-record) | Kafka Streams (per-record, dual write) |
| Schema | declared in table metadata | declared per-table (CQL) | none (schema-on-read) | none, except declared key attributes |
| Query surface | SQL via Trino | CQL, or SQL via Trino | Mongo query language, or SQL via Trino | AWS SDK / CLI only — **no Trino catalog in this repo** |
| Consistency model (managed/prod) | snapshot isolation, serializable per-table commits | tunable per-request (`ONE`/`QUORUM`/`ALL`) | tunable per-operation (write/read concern) | eventually consistent by default, strongly consistent reads opt-in |
| Scaling axis | object storage + stateless compute | partition key hash, leaderless | shard key hash (or replica set) | partition key hash, fully managed |
| This repo's instance | single-node MinIO + REST catalog | single Cassandra node | single MongoDB node | `dynamodb-local`, single JVM, no partition model |

## 2. Identity and key design, side by side

This is the axis that determines whether replay/duplicate delivery is safe — see
`docs/delivery-semantics.md` and `docs/cross-store-consistency.md` §4 for the full trace.

| Store | Key | Derived from | Duplicate behavior |
|---|---|---|---|
| Cassandra | `(source_ip, timestamp)` | the data | upsert — silently replaces (and can silently collide, see `cross-store-consistency.md` §3) |
| MongoDB | generated `ObjectId` | the write | append — accumulates duplicates |
| Iceberg | none (append-only files) | n/a | append — accumulates duplicates; dedup is a query/compaction-time concern |
| DynamoDB (`https_session_events`) | `(sourceIp, timestampIso)` | the data | replace — same key overwrites, same as Cassandra |
| DynamoDB (`https_session_aggregates`) | `sourceIp` | the data | **merges** — `ADD` counters double-count on replay, `SET` fields are idempotent (`docs/dynamodb-tutorial.md` §2) |

The general pattern holds across all four: **a data-derived key makes replay safe (or at least
detectable); a generated/write-derived key makes replay accumulate silently.** DynamoDB's aggregate
table is the interesting middle case — data-derived key, but the *operation* (`ADD`) is what breaks
idempotence, not the key itself.

## 3. Query capability: the real differentiator

| Capability | Iceberg | Cassandra | MongoDB | DynamoDB |
|---|---|---|---|---|
| Ad-hoc filtering on any column | yes (full scan, no key needed) | only on partition/clustering key without `ALLOW FILTERING` | yes, but unindexed = full collection scan | only via `Query` on key attributes, or `Scan` (full table, no filter pushdown to storage) |
| Secondary indexes | n/a (columnar, scan any column) | yes, but rarely a good idea (not covered here) | yes, on-demand | yes, but **provisioned separately** (GSI/LSI) — not free like MongoDB's `createIndex` |
| Joins | in Trino, across catalogs | in Trino only | in Trino only | **not queryable from Trino at all** — see `docs/dynamodb-tutorial.md` §5 |
| Aggregate pushdown | `count(*)` from snapshot metadata | none | none | none — every aggregate is a client-side reduce over `Scan`/`Query` results |
| Time travel / snapshots | yes, native | no | no | no (DynamoDB Streams + PITR exist but aren't the same primitive) |

DynamoDB is the outlier here in the opposite direction from Iceberg: Iceberg is built for
"query anything, pay at read time"; DynamoDB is built for "query only what you designed a key or
index for, and it will be fast and cheap — everything else requires a full `Scan`." Cassandra and
MongoDB sit between the two, each with their own version of "the query pattern is a design-time
decision, not a runtime one."

## 4. Operational model: who runs it, and how it fails

| | Iceberg | Cassandra | MongoDB | DynamoDB |
|---|---|---|---|---|
| Who manages the servers | you (or a lakehouse platform) | you | you (or Atlas) | AWS, fully managed |
| Capacity planning | storage/compute decoupled — scale independently | nodes + replication factor | nodes + shards | provisioned or on-demand WCU/RCU, per table *and per key* |
| Failure mode under overload | slow queries (more files to scan) | write/read timeouts past consistency level's replica count | write/read timeouts, or `w:0` silently drops | `ProvisionedThroughputExceededException` (provisioned) or transient 5xx/throttle (on-demand) — see `docs/dynamodb-tutorial.md` §4 |
| Hot-key/hot-partition risk | none (files are independent) | wide partitions (`docs/cassandra-tutorial.md` §4) | possible with a bad shard key | **explicit per-key throughput ceiling**, independent of overall table capacity |
| Multi-region | via catalog + replicated object storage | native (multi-DC) | native (Atlas global clusters) | native (Global Tables), but eventually consistent across regions |

The throughline: **Cassandra and DynamoDB share the same conceptual hot-partition-key risk** — both
hash a key to decide physical placement, and both cap throughput per key rather than only per
table/cluster. DynamoDB's version is more visible (an explicit exception/throttle) where
Cassandra's shows up as gradually degrading node performance from an oversized partition
(`docs/cassandra-tutorial.md` §4). MongoDB's equivalent failure (a bad shard key) has the same
root cause but a different failure signature (uneven shard load, not a hard per-key cap).

## 5. When to reach for each, in production

**Iceberg (or another open table format) — for analytical/historical workloads.** Anything you'll
query with ad-hoc SQL, aggregate over time ranges, or need to reprocess/backfill. The cost is write
latency (batched commits, not real-time) and the operational overhead of a catalog + compaction.
Wrong fit for point lookups by a known key at low latency.

**Cassandra — for high-write-throughput, known-access-pattern operational data**, especially
multi-region active-active writes with tunable consistency. The access pattern must be decided at
table-design time (`PRIMARY KEY` shape); if you don't know your queries yet, this is the wrong
store. Wrong fit for ad-hoc analytics or anything needing joins/aggregation outside a partition.

**MongoDB — for evolving/heterogeneous document shapes where schema-on-read genuinely helps**, and
where secondary-index flexibility matters more than raw write throughput. The cost is the
self-describing-BSON overhead at scale (`docs/cross-store-consistency.md` §8) and no compile-time
schema safety. Wrong fit if you already know your schema and want it enforced, or if you need
Cassandra-grade linear write scaling.

**DynamoDB — for known-key-shape, latency-sensitive operational data at AWS-native scale, with
zero operational burden.** The single-digit-millisecond latency and fully managed scaling are real
advantages over self-hosting Cassandra for an equivalent workload. The cost is the least flexible
query surface of the four (design your access patterns into keys/GSIs up front, because there is no
`ALLOW FILTERING` escape hatch, only `Scan`) and, as this repo demonstrates directly, **no
first-class way to join it with anything else** — plan a CDC/export path (DynamoDB Streams → S3 or
a warehouse) from day one if cross-store analytics will ever matter.

## 6. What this repo intentionally does not show

Every store here runs single-node/single-JVM with no real load. That erases the entire axis these
engines actually compete on: **behavior under partition, contention, and scale.** Consistency-level
trade-offs (Cassandra), shard-key hot-spotting (MongoDB), and per-key throughput ceilings
(DynamoDB) are all invisible at this data volume and topology — see each tutorial's gotchas section
for what to go looking for once you're running any of these for real.
