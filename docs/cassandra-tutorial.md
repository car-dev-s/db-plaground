# Apache Cassandra Tutorial (for this project)

Cassandra is a **wide-column, partitioned, leaderless distributed database**. In this repo it
runs single-node (`cassandra:5`, port 9042, data bind-mounted to `D:\work\docker\mount\cassandra`),
so none of its distributed-consistency machinery is actually exercised — but the data model
decisions below only make sense in light of *why* Cassandra is shaped the way it is.

## 1. Core mental model

A Cassandra table's primary key has two parts, and they do different jobs:

```
PRIMARY KEY ((partition_key), clustering_key, ...)
```

- **Partition key** — decides *which node(s)* own a row (via a hash of the key), and groups all
  rows sharing that key into one physically co-located partition on disk. All reads/writes for a
  partition key hit the same replica set — this is Cassandra's single most important scaling
  lever: pick a partition key that spreads writes evenly and that your read queries can supply
  exactly (Cassandra can't efficiently query *across* partitions without a full scan).
- **Clustering key** — sorts rows *within* a partition on disk, in the order declared. Range
  queries (`WHERE clustering_key > ?`) are only efficient when scoped to a single partition.

This project's table:

```sql
CREATE TABLE playground.https_sessions (
    source_ip        text,
    timestamp         timestamp,
    ... ,
    PRIMARY KEY ((source_ip), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

`source_ip` is the partition key, `timestamp` the clustering key, ordered descending — so
`SELECT * FROM https_sessions WHERE source_ip = ? LIMIT 10` cheaply returns the 10 most recent
sessions from that IP without scanning the whole partition. This optimizes for "look up recent
activity from a given source" — the natural access pattern for session/log data — at the cost of
"give me all sessions in the last hour across every source," which would require a full-cluster
scan (or a separate table with a time-bucketed partition key, not implemented here).

## 2. Why `((source_ip), timestamp)` over a generated UUID key

A sole generated UUID key (one row per partition) would spread writes maximally evenly, but
every read-by-source-ip becomes a full scan with `ALLOW FILTERING` — fine for playground data
volumes, actively harmful at any real scale. Partitioning by `source_ip` trades some potential
write skew (a source_ip that logs constantly gets a large, hot partition) for the query pattern
this project actually needs. Worth knowing if `source_ip` cardinality/skew ever becomes a real
concern: that's the point where a compound/synthetic partition key (e.g. bucketed by day) would
be the next design step.

## 3. Consistency levels (why this doesn't matter much here, but matters in general)

Cassandra has no single leader — every read/write specifies a **consistency level** (how many
replicas must ack) independent of the write path itself. Common levels: `ONE` (fastest, least
safe), `QUORUM` (majority of replicas), `ALL` (every replica, slowest). With
`replication_factor: 1` (this project's `cassandra/init.cql`), there's only one copy of the data
per partition, so consistency level is moot — every read/write effectively talks to the one
replica that has it. This is *the* thing that changes the moment this setup grows past one node:
replication factor and consistency level become real decisions with real trade-offs (availability
vs. consistency during node failure/partition — Cassandra's whole reason for being).

## 4. Gotchas

**Wide partitions.** A partition key that receives unbounded writes over time (like
`source_ip` here, if a single IP never stops generating sessions) grows the partition
indefinitely. Very large partitions (traditionally >100MB, modern Cassandra tolerates more but
it's still a smell) slow down reads/compaction and can eventually cause node instability. Not a
concern at playground data volumes; worth knowing for anything long-running.

**Tombstones.** Deletes in Cassandra don't remove data immediately — they write a tombstone
marker that must be read past on every subsequent read until compaction physically removes it.
Deleting rows out of a hot partition repeatedly (rather than truncating the whole table) is a
classic Cassandra performance foot-gun. This project's data is insert-only, so this doesn't come
up in normal use — only relevant if you start scripting per-row deletes.

**`ALLOW FILTERING`.** Any query that doesn't fully specify the partition key (and, for range
predicates, a prefix of the clustering key) gets rejected unless you add `ALLOW FILTERING` —
which usually means "this will scan every partition." Treat it as a query-design smell, not a
flag to reach for.

## 5. How `cqlsh` and the Trino `cassandra` catalog relate

Both talk to the exact same underlying table — `cqlsh` via Cassandra's native CQL protocol
directly, Trino's `cassandra` connector (`trino/catalog/cassandra.properties`) via the same
protocol under the hood, translating SQL into CQL-equivalent operations. There's no
synchronization step and no second copy of data: a row inserted by the Flink sink is visible from
both `cqlsh` and `SELECT * FROM cassandra.playground.https_sessions` in Trino/SQLPad immediately
(read-your-writes with `replication_factor: 1`, no cross-replica consistency window to worry
about).

## 6. Useful commands

```bash
# Open a cqlsh shell in the running container
docker exec -it playground-cassandra cqlsh

# Inspect the schema
docker exec -it playground-cassandra cqlsh -e "DESCRIBE KEYSPACE playground;"

# Query recent rows
docker exec -it playground-cassandra cqlsh -e "SELECT * FROM playground.https_sessions LIMIT 10;"

# Single-node cluster health
docker exec -it playground-cassandra nodetool status

# Reset data without dropping the table
docker exec -it playground-cassandra cqlsh -e "TRUNCATE playground.https_sessions;"

# Run the Flink sink locally (writes into this table, and into MongoDB — see mongodb-tutorial.md)
./gradlew :flink:bootRun

# Query through Trino/SQLPad instead of cqlsh
docker compose exec trino trino --execute "SELECT * FROM cassandra.playground.https_sessions LIMIT 10;"
```
