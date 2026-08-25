# Query Federation: Pushdown and Join Strategy in Trino (for this project)

`docs/trino-tutorial.md` covers how to *use* Trino here. This article is about what Trino is
actually doing when you write a join across `iceberg`, `cassandra`, and `mongodb` — and why the
same query can be instant or catastrophic depending on which side of the join a predicate lands.

## 1. Trino does not push joins down. Ever.

The mental model that matters:

> A connector can be asked to return **rows from one table**, optionally with some filtering,
> projection, and limiting already applied. Everything else — joins, aggregations, sorts,
> window functions — happens in Trino's own workers, on rows pulled over the network.

There is no such thing as a cross-catalog join executed by a storage engine. When you write

```sql
SELECT c.source_ip, c.domain, m.bytes_sent
FROM cassandra.playground.https_sessions c
JOIN mongodb.playground.https_sessions m
  ON c.source_ip = m.sourceIp
```

Trino opens splits against Cassandra, opens splits against MongoDB, streams both result sets into
worker memory, and hash-joins them there. The interesting question is never "how fast is the
join" — it is **how many rows each connector was forced to return**.

Everything below is about shrinking that number.

## 2. What each connector can push down

| Capability | `iceberg` | `cassandra` | `mongodb` |
|---|---|---|---|
| Column projection | yes (Parquet reads only the needed columns) | yes | yes |
| Predicate pushdown | **full** — any predicate becomes file/row-group filtering | **only on key columns** (see §3) | partial — simple comparisons become BSON query filters |
| Partition pruning | yes, via table partition spec | n/a (token-range splits only) | n/a |
| Min/max statistics skipping | yes, per Parquet row group | no | no |
| `LIMIT` pushdown | yes | yes | yes |
| Aggregate pushdown | `count(*)` answered from snapshot metadata | no | no |
| Table statistics for the optimizer | yes (row counts, NDV, min/max) | **no** | **no** |

That last row is the one that quietly ruins query plans — see §5.

The asymmetry is not arbitrary. Iceberg is a *table format designed for query engines*: its
metadata tree exists precisely so an engine can skip files without opening them. Cassandra and
MongoDB are operational stores whose APIs were designed for point access from applications, and a
federating engine can only use what their wire protocols expose.

## 3. Cassandra pushdown is governed by the primary key, not by the query

This is the single sharpest edge in this project. The table is:

```cql
PRIMARY KEY ((source_ip), timestamp)   -- partition key: source_ip, clustering key: timestamp
```

Cassandra can only efficiently serve a query if the predicate lets it locate partitions:

```sql
-- pushed down: equality on the partition key. Trino asks Cassandra for one partition.
WHERE c.source_ip = '10.90.109.45'

-- pushed down: partition key + range on the clustering key, in key order.
WHERE c.source_ip = '10.90.109.45' AND c.timestamp > TIMESTAMP '2026-08-24 12:00:00'

-- NOT pushed down: no partition key predicate. Trino full-scans the table over token-range
-- splits and filters in its own workers.
WHERE c.domain = 'api.example.com'

-- NOT pushed down: clustering key without the partition key. Same full scan.
WHERE c.timestamp > TIMESTAMP '2026-08-24 12:00:00'
```

The failure mode is that **nothing errors**. `cqlsh` would reject the last two with
`InvalidRequest ... use ALLOW FILTERING`; Trino cheerfully executes them by scanning the entire
table and filtering itself. At playground volumes you will not notice. At real volumes this is
the difference between 5 milliseconds and reading the whole cluster.

The practical rule for a federated join against Cassandra: **the join key should be the partition
key, and it should be the build side coming from the smaller table**, or you should filter
Cassandra down by `source_ip` first.

## 4. MongoDB pushdown is real but shallow

Trino's MongoDB connector translates simple predicates into a native BSON filter document, so
`WHERE m.statusCode = 500` genuinely becomes `{statusCode: 500}` on the MongoDB side. But:

- There are **no indexes in this project** beyond the default `_id` index
  (`docs/mongodb-tutorial.md` §3), so a pushed-down filter still causes a full collection scan
  inside MongoDB. Pushdown moved the work, it did not remove it.
- The connector infers column types by sampling documents. In a schema-on-read store, a field
  that is a `long` in most documents and a `string` in one will produce type surprises that
  appear only when the sample happens to include the odd document.
- Field names must match MongoDB's camelCase (`sourceIp`), not Cassandra's snake_case
  (`source_ip`) — a permanent source of typos in cross-catalog queries. This is a genuine schema
  divergence between the two sinks, not something Trino introduces.

Adding the one index that mirrors the Cassandra key makes MongoDB pushdown actually pay off:

```javascript
db.https_sessions.createIndex({ sourceIp: 1, timestamp: -1 })
```

## 5. No statistics means no cost-based optimizer

Trino's optimizer has two major cost-based powers: choosing **join order** and choosing **join
distribution**. Both need row counts. The Cassandra and MongoDB connectors report none.

Consequence: for any join involving those catalogs, Trino falls back to essentially the order you
wrote, and you are the optimizer. Two things follow.

**Join order is your responsibility.** Put the small, well-filtered relation on the build side.
Trino builds a hash table from the *right* input of a join and streams the left, so:

```sql
-- good: the heavily filtered Cassandra partition is the build side
FROM mongodb.playground.https_sessions m
JOIN cassandra.playground.https_sessions c
  ON m.sourceIp = c.source_ip
WHERE c.source_ip = '10.90.109.45'
```

**Join distribution is worth setting explicitly.** `BROADCAST` sends the whole build side to every
worker (great when it is small); `PARTITIONED` shuffles both sides by the join key (necessary when
both are large).

```sql
SET SESSION join_distribution_type = 'BROADCAST';
```

You can also force ordering off entirely if you trust your own SQL:

```sql
SET SESSION join_reordering_strategy = 'NONE';
```

For Iceberg-only queries, leave both on `AUTOMATIC` — there the statistics exist and the optimizer
beats hand-tuning.

## 6. Reading the plan

Guessing is unnecessary; Trino will tell you exactly what it pushed down.

```sql
EXPLAIN SELECT * FROM cassandra.playground.https_sessions WHERE source_ip = '10.90.109.45';
```

Look at the `TableScan` node. The distinction to read for:

- **`constraint=` / a filter listed inside the scan node** → pushed into the connector.
- **A separate `ScanFilterProject` or `Filter` node above the scan** → Trino is filtering rows the
  connector already returned. The connector read everything.

`EXPLAIN ANALYZE` runs the query and annotates each stage with actual row counts and wall time —
the fastest way to find the stage that returned ten million rows to produce twelve:

```sql
EXPLAIN ANALYZE
SELECT c.domain, count(*)
FROM cassandra.playground.https_sessions c
JOIN mongodb.playground.https_sessions m ON c.source_ip = m.sourceIp
GROUP BY c.domain;
```

The web UI at http://localhost:8082 shows the same information as a live stage graph, which is
easier to read for multi-stage queries. Both are far more informative than timing the query.

## 7. Federation is for exploration; materialization is for repetition

The honest positioning of a federated query engine: it is superb for *ad-hoc questions across
systems you would otherwise have to join by hand in a script*, and it is a poor substitute for
loading data into an analytical store. Every federated query re-pays the full extraction cost.

Trino makes the transition trivial — `CREATE TABLE AS` across catalogs is the entire ETL:

```sql
CREATE TABLE iceberg.playground.sessions_from_cassandra AS
SELECT * FROM cassandra.playground.https_sessions;
```

Now the data has statistics, column-level min/max, partition pruning, and time travel. This is the
pattern the whole architecture in this repo is a miniature of: operational stores serve the
application, and an analytical table format serves the analysis, with a query engine bridging the
two.

Note the direction is one-way in practice. Trino *can* write to Cassandra and MongoDB, but there
are **no cross-catalog transactions** — a multi-catalog `INSERT` is not atomic, and there is no
rollback if the second catalog fails. Treat Iceberg as the only write target you can reason about.

## 8. Reconciliation queries worth keeping

```sql
-- 1. Do the three stores agree on volume? (See docs/cross-store-consistency.md for why they may not.)
SELECT 'iceberg'   AS store, count(*) AS rows FROM iceberg.playground.https_sessions
UNION ALL SELECT 'cassandra', count(*) FROM cassandra.playground.https_sessions
UNION ALL SELECT 'mongodb',   count(*) FROM mongodb.playground.https_sessions;

-- 2. Which source_ips exist in Cassandra but never made it to MongoDB?
SELECT c.source_ip
FROM cassandra.playground.https_sessions c
LEFT JOIN mongodb.playground.https_sessions m ON c.source_ip = m.sourceIp
WHERE m.sourceIp IS NULL;

-- 3. Distinct-key comparison — cheaper than a full anti-join, and it isolates
--    "records lost" from "records duplicated".
SELECT
  (SELECT count(DISTINCT source_ip) FROM cassandra.playground.https_sessions) AS cassandra_ips,
  (SELECT count(DISTINCT sourceIp)  FROM mongodb.playground.https_sessions)   AS mongodb_ips;

-- 4. Iceberg count(*) served entirely from snapshot metadata — no data files are read.
--    Compare its EXPLAIN with the Cassandra one to see the difference in scan cost.
EXPLAIN SELECT count(*) FROM iceberg.playground.https_sessions;
```

## 9. Things a senior dev should know going in

**A federated query is only as fast as its worst connector.** Trino's own execution is rarely the
bottleneck; the connector that was forced into a full scan is.

**Pushdown failures are silent.** No warning, no error — just a slow query. `EXPLAIN` is not an
optimization tool here, it is a correctness tool.

**Catalog config is a server-side file, not a client concern.** `trino/catalog/*.properties` is
read at startup; adding a catalog means restarting the Trino container. There is no `CREATE
CATALOG` in this setup.

**Schema drift between stores is permanent friction.** `source_ip` vs `sourceIp` exists because
two independent sinks each picked their store's idiomatic convention. That was a reasonable local
decision and a costly global one — if you own both sinks, pick one naming convention and enforce
it at the event schema level.
