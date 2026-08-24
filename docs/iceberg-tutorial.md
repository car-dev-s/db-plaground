# Apache Iceberg Tutorial (for this project)

Apache Iceberg is a **table format**, not a database or storage engine. It defines how a set of
data files (Parquet/ORC/Avro, in this project's case Parquet-by-default via Trino) plus metadata
files together represent a versioned, ACID-compliant table — while the actual bytes sit in ordinary
object storage. This is what lets Kafka Connect (writer) and Trino (reader) — two completely
different engines — safely operate on the same table concurrently without either one owning the
data.

In this repo: table data lives in **MinIO** (`s3://warehouse/`, S3-compatible), table metadata is
tracked by the **Iceberg REST catalog** (`iceberg-rest`, port 8181), and both the Kafka Connect sink
connector and Trino's `iceberg` catalog point at that same REST catalog.

## 1. Core mental model

Iceberg's central idea: **a table is a mutable pointer to an immutable tree of metadata.**

```
catalog entry (mutable pointer)
   → metadata.json (current schema, partition spec, snapshot list)
      → snapshot (a specific version of the table)
         → manifest list
            → manifest files (list which data files belong to this snapshot, with column stats)
               → data files (actual Parquet/ORC/Avro files, immutable once written)
```

- **Nothing is ever mutated in place.** A write produces new data files and a new metadata.json;
  the catalog's pointer is atomically swapped from the old metadata.json to the new one. Readers
  either see the fully-old or fully-new state — never a half-written table.
- **A "snapshot" is a complete, independently-queryable version of the table.** This is what
  backs time-travel queries and `optimize`/rollback operations.
- **The catalog's only job is atomically swapping that one pointer** (compare-and-swap on
  "current metadata location"). This is the entire mechanism behind Iceberg's ACID guarantees —
  see section 3.

## 2. Why a REST catalog (vs. Hive Metastore / Glue / JDBC catalog)

Iceberg supports pluggable catalog backends. This project uses the **REST catalog**
(`tabulario/iceberg-rest`) specifically because:

- It's a thin, stateless HTTP API in front of the CAS operation — no Hive Metastore/Thrift, no
  separate RDBMS for a JDBC catalog. Minimal moving parts for a local playground.
- Any Iceberg-compatible engine that speaks the REST catalog protocol works against it identically
  — Trino and the Kafka Connect sink connector both just point at `http://iceberg-rest:8181` with no
  engine-specific catalog logic.
- It's become the de facto standard interop layer across the Iceberg ecosystem (Snowflake, Databricks,
  Spark, Trino, Flink, Kafka Connect can all read/write the same tables through it).

Trade-off worth knowing: the REST catalog spec doesn't standardize *storage* of the catalog's own
state — this specific image keeps it in-memory unless configured otherwise, which is fine for a
throwaway dev setup but means catalog state (schema history/pointer) doesn't survive that
container being recreated, distinct from the actual table data files sitting durably in MinIO.

## 3. ACID guarantees — how they actually work

- **Atomicity**: every write (batch insert, delete, schema change) produces a new snapshot as one
  atomic catalog pointer swap. Partial writes are impossible from a reader's perspective — you see
  the old snapshot or the new one, never a mix.
- **Consistency**: schema and partition spec are versioned alongside data; readers always see a
  self-consistent schema-to-data mapping for whatever snapshot they're reading.
- **Isolation**: achieved via **optimistic concurrency control (OCC)** at the catalog level, not
  locks. A writer reads the current metadata pointer, builds its new metadata based on it, then
  attempts a compare-and-swap: "set pointer to my-new-metadata.json, but only if it's still
  pointing at what I read." If another writer won the race first, the CAS fails and the loser
  **retries**: re-read the new current state, recompute, try again.
- **Durability**: once a snapshot's metadata.json is committed and the catalog pointer swapped,
  it's durable per the underlying object store's guarantees (MinIO here, S3 in production).

**What happens under concurrent write load** (directly relevant to this pipeline, since Kafka
Connect commits on a timer and Trino can write concurrently via `INSERT`/`MERGE`):

- No corruption, no deadlocks — OCC failures are just failed CAS attempts, always retried.
- Under sustained concurrent writers to the *same table*, retry rate climbs non-linearly as
  contention increases — every failed writer has to re-read state and recompute, which itself adds
  catalog load, which increases the odds of the next writer also losing its race.
- Each writer has a bounded retry count; **exhausting retries surfaces as a commit failure**, not
  silent data loss — you'll see this as an explicit error in the writer (e.g. Kafka Connect task
  logs), not as missing rows.
- **Practical mitigations** (tune whichever applies to your bottleneck):
  - Increase commit interval (fewer, larger commits per writer = less contention) — this project's
    `iceberg.control.commit.interval-ms` on the sink connector is exactly this knob.
  - Reduce concurrent writers per table (route to disjoint tables/partitions instead of N writers
    racing on one table).
  - Partition so concurrent writers target disjoint partitions where the catalog/engine can avoid
    contention (partition-level commit conflict detection isn't universal across engines, but
    disjoint physical writes still reduces the *practical* chance of overlapping stat ranges
    triggering unnecessary retries).

## 4. The small-files problem (you will hit this with Kafka Connect)

Every commit — even a five-row batch — creates at least one new data file and one new manifest
entry. A connector committing every 10 seconds (as configured here for fast local iteration)
produces far more, far smaller files than the same data written in hourly batches. Symptoms:

- Query planning slows down (more manifest entries to read/prune).
- Object storage gets many small objects, which is inefficient for most engines' scan patterns.
- No correctness issue — just a performance/cost one.

Check it directly in Trino:

```sql
SELECT count(*) AS file_count, sum(file_size_in_bytes) AS total_bytes,
       avg(file_size_in_bytes) AS avg_file_size
FROM iceberg.playground."https_sessions$files";
```

Fix with Iceberg's built-in compaction, callable from Trino:

```sql
ALTER TABLE iceberg.playground.https_sessions EXECUTE optimize;
```

This rewrites small files into fewer, larger ones and commits the result as a new snapshot — same
CAS mechanism as any other write, so it's safe to run against a live table. In production you'd
schedule this periodically (e.g. via a maintenance job), not just run it ad hoc.

## 5. Schema evolution

Iceberg tracks schema **by field ID**, not by column position or name matching — this is what makes
add/drop/rename/reorder column operations safe and metadata-only (no data file rewrite required):

```sql
ALTER TABLE iceberg.playground.https_sessions ADD COLUMN user_agent VARCHAR;
ALTER TABLE iceberg.playground.https_sessions RENAME COLUMN statuscode TO status_code;
```

Old data files remain valid after a schema change — a column added later simply reads as `NULL`
for rows written before the change. This project's sink connector config sets
`iceberg.tables.evolve-schema-enabled: true`, meaning **the connector itself** can widen the table
schema automatically when it sees a new field in incoming JSON — useful for iterating on a producer
schema without manual DDL, but worth knowing it's an automatic, silent schema change in production
terms — a typo'd field name in a producer becomes a permanent new column, not an error.

## 6. Time travel and metadata introspection

Every snapshot is independently queryable. From Trino:

```sql
-- list every snapshot with its operation type and commit time
SELECT snapshot_id, committed_at, operation FROM iceberg.playground."https_sessions$snapshots";

-- query the table as of a specific snapshot
SELECT * FROM iceberg.playground.https_sessions FOR VERSION AS OF <snapshot_id>;

-- query as of a timestamp
SELECT * FROM iceberg.playground.https_sessions FOR TIMESTAMP AS OF TIMESTAMP '2026-08-22 23:00:00';

-- roll the table back to a prior snapshot (creates a new snapshot pointing at old state — non-destructive)
ALTER TABLE iceberg.playground.https_sessions EXECUTE rollback_to_snapshot('<snapshot_id>');
```

Other metadata tables worth knowing (all queried the same `"table$metadata_name"` way):
`$history` (full lineage of snapshot changes, including which was replaced by which),
`$manifests`, `$partitions` (per-partition row/file counts — fast way to check for skew), `$refs`
(named branches/tags, if used).

## 7. Things a senior dev should know going in

**Partition spec changes don't rewrite old data.** Changing `PARTITIONED BY` on an existing Iceberg
table only affects *new* writes — old files keep their original layout. This is a deliberate
design choice (avoids an expensive full-table rewrite) but means a table can have data physically
laid out under two different partition schemes simultaneously; Iceberg's query planner handles this
transparently, but it matters if you're reasoning about file layout for external tooling.

**Hidden/implicit partitioning is a real ergonomic win over Hive-style partitioning** — you don't
need `WHERE day_partition = '2026-08-22'` as a separate derived column; `PARTITIONED BY
(day(event_time))` lets you filter on the actual timestamp column and Iceberg does the partition
mapping internally. Don't manually maintain a redundant partition column out of Hive-era habit.

**Catalog choice affects concurrency characteristics, not just "where metadata lives."** A
REST catalog's CAS behavior, a Hive Metastore's Thrift-based locking, and a plain JDBC catalog's
database transaction all have different contention/latency profiles under concurrent commits — this
matters directly if this playground's write pattern (frequent small commits from Kafka Connect)
were ever pointed at a different catalog backend.

**Data files are immutable — deletes and updates are metadata operations, not in-place edits.**
Iceberg v2 tables implement row-level `DELETE`/`UPDATE`/`MERGE` via delete files (either
position-based or equality-based) that get reconciled against data files at read time, not by
rewriting the original Parquet files. This is why deletes are cheap and instantaneous but a table
with many uncompacted deletes can slow down reads until `optimize` runs — same underlying mechanism
as the small-files problem in section 4.

**The catalog is a single point of coordination, not a single point of failure for reads.** If the
REST catalog is briefly unavailable, in-flight reads against already-resolved snapshots are
unaffected (the data files themselves are in MinIO/S3, independent of the catalog), but no new
table resolution (opening a table for the first time in a session, or committing a new write) can
happen until it's back.

**Don't confuse "connector-level" catalog config with "Trino-level" catalog config** — this project
has the *same* logical REST catalog referenced from two different places with two different config
dialects: `trino/catalog/iceberg.properties` (`iceberg.rest-catalog.uri`, Trino's own key names) and
`kafka-connect/iceberg-sink-connector.json` (`iceberg.catalog.uri`, the sink connector's own key
names). They must point at the same catalog/warehouse for both engines to see the same tables, but
there's no shared config file enforcing that — a typo'd URI in one silently creates a
second, disconnected view of "the same" table space.
