# Trino Tutorial (for this project)

Trino is a distributed SQL query engine — it has no storage of its own. It executes SQL against
**connectors** (Iceberg, Postgres, Kafka, Hive, etc.), each mapped to a "catalog" name, and can even
join across catalogs in a single query. In this repo, Trino talks to the `iceberg` catalog defined
in `trino/catalog/iceberg.properties`, which points at the Iceberg REST catalog (`iceberg-rest`,
metadata) and MinIO (`minio`, data files). See `DEPLOYMENT.md` section 5 for how that catalog file
is built.

Trino is running at **http://localhost:8082** (mapped from the container's internal 8080 — don't
confuse it with Kafka UI, which owns host port 8080 in this compose file).

## 1. Core mental model

- **Catalog → Schema → Table**, three-part naming: `iceberg.playground.https_sessions`.
  - *Catalog* = a connector instance + config (one `.properties` file = one catalog).
  - *Schema* = what other engines call a "database" (Iceberg REST calls it a "namespace").
  - *Table* = the usual.
- **Trino is stateless and ephemeral per query.** There's no persistent session state beyond a
  single query's execution — it plans, fans out to workers, executes, streams results, discards.
  Nothing is written back except through the connector (e.g., `INSERT`/`CREATE TABLE AS`).
- **Everything is a `SELECT`-shaped pipeline.** Even DDL like `CREATE TABLE ... AS SELECT` and
  `INSERT INTO ... SELECT` reuses the same distributed execution engine as a read query.
- **Pushdown matters.** Trino tries to push filters/projections/aggregations down into the
  connector so it reads less data. For Iceberg this means: partition pruning, min/max stats
  pruning via manifest files, and (for Parquet) column pruning — read `EXPLAIN` output to confirm
  this is happening for a given query rather than assuming it.
- **No local durable state between restarts.** Restarting the `trino` container loses nothing
  persistent-worthy — schemas/tables live in the Iceberg REST catalog and MinIO, not in Trino.

## 2. Querying via the CLI

Fastest way, using the CLI baked into the `trino` image:

```bash
docker compose exec trino trino
```

This drops you into a REPL already pointed at `localhost:8080` inside the container. From there:

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.playground;
DESCRIBE iceberg.playground.https_sessions;

SELECT count(*) FROM iceberg.playground.https_sessions;

SELECT sourceip, destinationip, method, statuscode, bytessent
FROM iceberg.playground.https_sessions
LIMIT 10;
```

Useful CLI-only commands (not SQL, no trailing `;`):
- `quit` / `exit` — leave the REPL.
- `USE iceberg.playground;` — set default catalog/schema so you can write `SELECT * FROM
  https_sessions` instead of the fully qualified name.

Non-interactive / scripted usage (handy for CI or quick checks from your shell without entering
the container's REPL):

```bash
docker compose exec trino trino --execute "SELECT count(*) FROM iceberg.playground.https_sessions"
```

Standalone `trino-cli` (if you want a native client on your host instead of `docker compose exec`):
download the executable jar from the [Trino releases page](https://trino.io/docs/current/client/cli.html)
and run `java -jar trino-cli-*-executable.jar --server http://localhost:8082 --catalog iceberg
--schema playground`.

## 3. Querying via the Web UI

Open **http://localhost:8082**. The built-in UI is a **monitoring console first, query tool
second**:

- Cluster/worker overview, running/finished query list, per-query timeline and resource usage.
- A basic query editor under the "Query" tab where you can paste SQL and see results — but no
  autocomplete, no saved queries, no multi-tab editing.
- Click into any query (running or historical) to see its live/final execution plan, stage-by-stage
  timing, and — critically for tuning — how much data each stage actually read vs. estimated.

For serious day-to-day query authoring, most teams pair the CLI (or a JDBC client — DBeaver,
DataGrip) with the Web UI purely for **observability**: confirming a query didn't blow up on a
missing partition filter, checking peak memory, spotting a skewed stage.

JDBC connection string for any generic SQL client:

```
jdbc:trino://localhost:8082/iceberg/playground
```

Driver: `io.trino:trino-jdbc` (Maven Central). No auth configured in this setup (`user` can be
anything, no password).

## 4. Things a senior dev should know going in

**`EXPLAIN` and `EXPLAIN ANALYZE` before trusting a query's cost.**
`EXPLAIN` shows the planned distributed execution graph without running it; `EXPLAIN ANALYZE`
actually runs it and annotates the plan with real row counts/timings per stage. Use this to check
whether a `WHERE` clause is actually pruning Iceberg partitions/files or whether Trino is scanning
the whole table.

```sql
EXPLAIN SELECT * FROM iceberg.playground.https_sessions WHERE statuscode = 500;
```

**Trino ≠ a transactional database.**
No row-level locking, no long-running transactions across statements. Each statement is
independent. For Iceberg specifically, Trino does support atomic single-statement
`INSERT`/`UPDATE`/`DELETE`/`MERGE` (Iceberg's snapshot model makes this safe), but there's no
`BEGIN ... COMMIT` spanning multiple statements the way you'd expect in Postgres.

**Query results are only as fresh as the connector's metadata view — watch for the "read your own
write" gap.**
This bit us already in this project (see `DEPLOYMENT.md`, "stale metadata cache" gotcha): querying
immediately after a Kafka Connect commit can show `0` rows for one query, then correct itself.
Trino caches Iceberg table metadata for performance; if you need a guaranteed-fresh read after a
known write, re-run the query, or reduce `iceberg.metadata-cache-ttl` in the catalog properties for
latency-sensitive interactive use (not recommended for high-QPS production catalogs — that cache
exists to protect the REST catalog from load).

**Memory limits are per-query and per-cluster, and undersized joins fail loudly, not silently.**
`SELECT` with a big unpartitioned join can hit `Query exceeded per-node user memory limit` or
`Query exceeded distributed user memory limit`. In production Trino deployments, tune
`query.max-memory` / `query.max-memory-per-node`; for exploratory work, prefer filtering before
joining and check `EXPLAIN` for a broadcast join where a partitioned join was intended (`SET
SESSION join_distribution_type = 'PARTITIONED'` to force it).

**Time travel and snapshot inspection are native for Iceberg tables** — this is one of the biggest
practical wins over querying a plain object store or a non-versioned warehouse:

```sql
-- see all snapshots and when they were committed
SELECT * FROM iceberg.playground."https_sessions$snapshots";

-- see current file layout / sizes (useful for spotting small-file problems from frequent Kafka Connect commits)
SELECT * FROM iceberg.playground."https_sessions$files";

-- query the table as of a specific snapshot
SELECT * FROM iceberg.playground.https_sessions FOR VERSION AS OF 1234567890123456789;
```

The `"table$metadata_table"` syntax (note the double quotes — required because `$` isn't valid in
an unquoted identifier) is Trino's Iceberg connector exposing Iceberg's internal metadata tables
directly as queryable relations: `$snapshots`, `$files`, `$manifests`, `$history`,
`$partitions`. Reach for these before assuming something's wrong with the data — they're the
fastest way to answer "did the write actually land" without leaving SQL.

**`CREATE TABLE ... AS SELECT` (CTAS) is the idiomatic way to materialize a derived Iceberg table**
— no separate DDL + backfill dance needed:

```sql
CREATE TABLE iceberg.playground.https_sessions_5xx AS
SELECT * FROM iceberg.playground.https_sessions WHERE statuscode >= 500;
```

**Session properties are per-connection tuning knobs, not global config** — `SET SESSION
<property> = <value>` only affects your current CLI/JDBC session. Useful ones: `join_distribution_type`,
`task_concurrency`. `SHOW SESSION;` lists what's tunable and current values.

**Iceberg-connector-specific properties live on the table, not just the catalog** — e.g. you can
set `partitioning`, `format` (Parquet/ORC/Avro), `sorted_by` at `CREATE TABLE` time:

```sql
CREATE TABLE iceberg.playground.example (
    id BIGINT,
    event_time TIMESTAMP(6)
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(event_time)']
);
```

Getting partitioning right up front matters far more in Iceberg/Trino than in a typical RDBMS —
partition pruning is the main lever Trino has to avoid scanning entire tables, and Iceberg's
hidden partitioning (`day(event_time)`, `bucket(id, 16)`, etc.) means you don't need to duplicate
the partition column in every filter — Trino translates `WHERE event_time > ...` into the
right partition prune automatically.

## 5. Quick reference — checking pipeline health from Trino

```sql
-- row count sanity check
SELECT count(*) FROM iceberg.playground.https_sessions;

-- confirm recent commits from the Kafka Connect sink are landing
SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added_records
FROM iceberg.playground."https_sessions$snapshots"
ORDER BY committed_at DESC
LIMIT 5;

-- spot a small-file problem (frequent low-interval commits from Kafka Connect create many small files)
SELECT count(*) AS file_count, sum(file_size_in_bytes) AS total_bytes
FROM iceberg.playground."https_sessions$files";
```

If `file_count` is high relative to `total_bytes`, that's the small-files symptom flagged in the
project's memory notes (frequent commits from Kafka Connect + Iceberg's OCC model) — the fix is
either raising `iceberg.control.commit.interval-ms` on the connector or running Iceberg's table
maintenance procedures:

```sql
ALTER TABLE iceberg.playground.https_sessions EXECUTE optimize;
```

This compacts small files into larger ones. Run it periodically in any pipeline with frequent
small commits, not just this playground.

## 6. Next: what Trino is actually doing

This article covers using Trino here. Two follow-ons go a level deeper:

- `docs/query-federation.md` — which predicates each connector can push down (and the silent
  full scans that happen when they can't), why the missing Cassandra/MongoDB statistics disable
  the cost-based optimizer, and how to read `EXPLAIN` to tell the difference.
- `docs/cross-store-consistency.md` — why joining the three catalogs on their native `timestamp`
  columns returns nothing, and which field to join on instead.
