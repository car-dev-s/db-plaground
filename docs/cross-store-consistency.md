# Cross-Store Consistency: Why the Three Stores Disagree (for this project)

One producer emits one record. Three storage engines receive it. They end up holding **three
different timestamps and three different notions of identity** — and none of that is a bug in any
one component. This article traces a single field through the whole pipeline and shows where the
information is lost, because that trace is the most transferable thing in this repo.

Companion reading: `docs/delivery-semantics.md` explains disagreements caused by *replay and
failure*. This article explains disagreements caused by *representation*, which are permanent and
happen even when nothing goes wrong.

## 1. One `Instant`, five representations

`HttpsSessionProducer.randomSession()` calls `Instant.now()` **once** and writes it into two
fields of the same record:

```java
Instant now = Instant.now();
// ...
now,             // -> timestamp     (java.time.Instant)
now.toString()   // -> timestampIso  (String)
```

Here is what each hop does to it:

| Stage | Type | Precision | Example |
|---|---|---|---|
| `Instant.now()` (JDK 21, Windows) | `Instant` | 100 ns ticks | `2026-08-24T12:58:23.472492600Z` |
| Kafka message (Jackson JSON) | JSON number | full, as fractional epoch-seconds | `1787498303.4724926` |
| Flink → Cassandra `timestamp` | CQL `timestamp` | **milliseconds** | `2026-08-24 12:58:23.472000+0000` |
| Flink → MongoDB `timestamp` | BSON `Date` | **milliseconds** | `ISODate('2026-08-24T12:58:23.472Z')` |
| Connect → Iceberg `timestamp` | Iceberg `timestamp` | **seconds** | `2026-08-24 12:58:23.000000` |
| any store, `timestamp_iso` / `timestampIso` | `text` / `string` | full, lossless | `2026-08-24T12:58:23.472492600Z` |

You can verify the first two truncations directly against the running stack:

```bash
docker compose exec cassandra cqlsh -e \
  "SELECT source_ip, timestamp, timestamp_iso FROM playground.https_sessions LIMIT 3;"

docker compose exec mongodb mongosh --quiet --eval \
  'db.getSiblingDB("playground").https_sessions.find({},{sourceIp:1,timestamp:1,timestampIso:1}).limit(3).toArray()'
```

Real output from this project — note `.472000` and `.472Z` against `.472492600Z`:

```
 source_ip      | timestamp                       | timestamp_iso
 10.232.248.183 | 2026-08-24 12:58:23.472000+0000 | 2026-08-24T12:58:23.472492600Z
```

### Why each loss happens

**Cassandra and MongoDB truncate to milliseconds because their timestamp types *are*
milliseconds.** CQL `timestamp` and BSON `Date` are both a signed 64-bit count of milliseconds
since the epoch. The DataStax driver's `Instant` binding and `Date.from(instant)` both truncate
silently — there is no lossy-conversion warning, and no wider type to switch to. (Cassandra does
have `time`/`date` types, but neither is a full timestamp; storing nanosecond precision means
storing a second column, which is exactly what `timestamp_iso` is doing.)

**Iceberg truncates to whole seconds because of an SMT chain, not because of Iceberg.** Iceberg's
`timestamp` type is microsecond-precision — easily the most precise of the three. The loss comes
from the workaround documented in `DEPLOYMENT.md` §6: with `schemas.enable: false`, the connector
infers column types from raw JSON shapes, and a fractional epoch-second number is indistinguishable
from any other decimal, so it infers `double`. The fix casts it to `int64` first:

```json
"transforms.castTimestamp.spec": "timestamp:int64",
"transforms.toTimestamp.unix.precision": "seconds"
```

`Cast$Value` to `int64` truncates the fraction. Everything after the decimal point is gone before
`TimestampConverter` ever sees the value. The pipeline traded 1000x precision for a correct column
*type* — a reasonable trade made explicit, but worth knowing it was made.

The structural fix is to stop shipping schemaless JSON: with a schema registry (Avro/Protobuf) or
`schemas.enable: true`, the timestamp arrives as a declared logical type and the entire SMT chain
disappears.

## 2. `timestampIso` is the only field that survives intact — and it is not sortable

Because it is carried as text everywhere, `timestampIso` is byte-for-byte identical in Cassandra,
MongoDB, and Iceberg. That makes `(sourceIp, timestampIso)` **the only reliable cross-store join
key in this project**. Joining on the native `timestamp` columns silently produces wrong results:
a Cassandra row at `.472000` will never match its own Iceberg row at `.000000`.

But do not reach for `timestampIso` as an ordering key. `Instant.toString()` uses
`DateTimeFormatter.ISO_INSTANT`, which emits **zero, three, six, or nine** fractional digits,
trimming trailing zero groups. Variable-length fractions break lexicographic ordering:

| Instant | `toString()` | |
|---|---|---|
| `…23.420000000Z` | `2026-08-24T12:58:23.420Z` | |
| `…23.420492600Z` | `2026-08-24T12:58:23.420492600Z` | |

String-compare those: after `420`, the first has `Z` (0x5A) and the second has `4` (0x34), so the
*earlier* instant sorts *later*. The degenerate case is worse — an `Instant` landing exactly on a
second renders as `2026-08-24T12:58:23Z`, where `Z` compares against `.` (0x2E) and sorts after
every sub-second timestamp in that second.

So the two fields have exactly complementary properties, and the correct usage is to use both:

| Field | Precise | Sortable / range-queryable | Use it for |
|---|---|---|---|
| `timestamp` | no | yes | `ORDER BY`, `BETWEEN`, partitioning, clustering |
| `timestampIso` | yes | **no** | exact identity, cross-store joins, audit |

If you wanted one field with both properties, the fix is a fixed-width rendering — either always
nine digits (`DateTimeFormatter.ofPattern` with `nnnnnnnnn`, or `ISO_INSTANT` on an
`Instant.truncatedTo(ChronoUnit.NANOS)` padded manually) or an integer epoch-nanos column. That is
a one-line producer change with a large payoff, and it is the change worth making first.

## 3. Millisecond truncation is also a *silent row-loss* mechanism in Cassandra

The Cassandra table's primary key is:

```cql
PRIMARY KEY ((source_ip), timestamp)
```

The clustering key is the **truncated** millisecond value. Two genuinely distinct sessions from the
same `source_ip` within the same millisecond therefore collapse to the same primary key — and
because every CQL `INSERT` is an upsert, the second one **silently overwrites the first**. No
error, no duplicate-key exception, no warning. Cassandra has no mechanism to tell you this
happened.

This is the same property that makes replay harmless (`docs/delivery-semantics.md` §5) viewed from
the other side. Idempotence and lossiness are not two behaviours; they are one behaviour, and which
one you get depends entirely on whether records that share a key are actually the same record.

How likely is a collision here? `HttpsSessionProducer.loadSessions()` runs a tight loop, so many of
the 100 records genuinely share a millisecond — but `randomPrivateIp()` draws from a ~16M address
space, so a same-millisecond *and* same-IP pair is rare at this volume. Raise `session-count`, or
narrow the IP space, and the counts start diverging:

```sql
-- Cassandra reports fewer rows than MongoDB, and it is not a delivery failure.
SELECT count(*) FROM cassandra.playground.https_sessions;
SELECT count(*) FROM mongodb.playground.https_sessions;
```

**Diagnosing it is the interesting part**, since the collided rows leave no trace in Cassandra. The
`timestamp_iso` column is what makes it detectable: it survived at full precision, so two records
that collided on `(source_ip, timestamp)` would have had different `timestamp_iso` values — and
only the survivor's is present. Compare against a store that did not collapse them:

```sql
-- Records MongoDB has that Cassandra lost to key collision.
SELECT m.sourceIp, m.timestampIso
FROM mongodb.playground.https_sessions m
LEFT JOIN cassandra.playground.https_sessions c
  ON m.sourceIp = c.source_ip AND m.timestampIso = c.timestamp_iso
WHERE c.source_ip IS NULL;
```

The real lesson: **a primary key built from a value your storage engine will truncate is not the
key you think it is.** Either key on something guaranteed unique (a session UUID), or store the
key column at a precision the engine preserves.

## 4. Three different identity models

Timestamps are the vivid example, but identity diverges just as much:

| Store | Identity of a record | Duplicate behaviour |
|---|---|---|
| Cassandra | `(source_ip, timestamp)` — derived from the data | upsert: silently replaces |
| MongoDB | generated `ObjectId` — derived from the *write* | append: accumulates duplicates |
| Iceberg | none; append-only files | append: accumulates duplicates |
| DynamoDB `https_session_events` | `(sourceIp, timestampIso)` — derived from the data | replace: silently overwrites, same shape as Cassandra |
| DynamoDB `https_session_aggregates` | `sourceIp` — derived from the data | **merges**: `SET` fields idempotent, `ADD` counters double-count — see `docs/dynamodb-tutorial.md` §2 |

Cassandra and the DynamoDB event table both have a data-derived key, which is why both converge
(in the identity sense) under replay. The MongoDB `_id` is generated per `insertOne` call —
visible in the sample output above as `ObjectId('6a8c40753e030c5f845dffab')` — so it carries no
information about the session at all. `docs/delivery-semantics.md` §5 has the deterministic-`_id`
fix. The DynamoDB aggregate table is the odd one out: a data-derived key *and* still not safe under
replay, because the non-idempotence lives in the `ADD` operation, not the key — a reminder that key
design and operation design are two separate decisions that both have to be gotten right.

Iceberg's append-only model is not a deficiency; it is what makes its snapshot isolation and time
travel work. Deduplication in that world is a query-time or compaction-time concern
(`MERGE INTO`, or `row_number()` over the natural key), not a write-time one.

## 5. Naming diverges too, and it is permanent

| Concept | Kafka JSON | Cassandra | MongoDB | Iceberg |
|---|---|---|---|---|
| source IP | `sourceIp` | `source_ip` | `sourceIp` | `sourceIp` |
| ISO timestamp | `timestampIso` | `timestamp_iso` | `timestampIso` | `timestampIso` |

Cassandra is snake_case because `cassandra/init.cql` declares it that way (CQL folds unquoted
identifiers to lowercase, so camelCase would need permanent double-quoting — snake_case was the
right call). MongoDB and Iceberg both inherit the Java field names, since neither has an explicit
schema definition step.

The cost lands entirely on the query author: every cross-catalog join in this project has to switch
convention mid-`ON`-clause. If you control the event schema, the cheap fix is to emit snake_case on
the wire and let every store inherit it.

## 6. A consistency checklist

Run these three in order; each one isolates a different failure class.

```sql
-- 1. Volume. Divergence here = delivery/replay (see docs/delivery-semantics.md),
--    or key collision (§3 above).
SELECT 'cassandra' AS store, count(*) AS rows FROM cassandra.playground.https_sessions
UNION ALL SELECT 'mongodb', count(*) FROM mongodb.playground.https_sessions
UNION ALL SELECT 'iceberg', count(*) FROM iceberg.playground.https_sessions;

-- 2. Identity. Divergence here = records present in one store, absent in another.
--    Join on timestampIso, never on the native timestamp columns (§2).
SELECT count(*) AS matched
FROM cassandra.playground.https_sessions c
JOIN mongodb.playground.https_sessions m
  ON c.source_ip = m.sourceIp AND c.timestamp_iso = m.timestampIso;

-- 3. Fidelity. Confirm the precision loss is exactly where §1 says it is.
SELECT source_ip, timestamp, timestamp_iso,
       date_diff('millisecond', timestamp, from_iso8601_timestamp(timestamp_iso)) AS lost_ms
FROM cassandra.playground.https_sessions
LIMIT 5;
```

## 7. Things a senior dev should know going in

**Precision loss is the default, not the exception.** Every hop between a Java type, a wire
format, and a storage engine's type system is an opportunity to truncate, and almost none of them
warn. The only defence is to test a round-trip with a value that has digits in every position.

**Carry a lossless copy of anything you might need to reconcile on.** `timestampIso` looks
redundant right up until it is the only way to prove what happened — as in §3, where it is the sole
evidence that a row was overwritten.

**`count(*)` is a weak consistency check.** Equal counts can hide a lost row and a duplicated row
cancelling out. Compare *key sets*, not cardinalities, when it matters.

**Cassandra will not help you audit this.** `SELECT count(DISTINCT source_ip)` is not valid CQL,
and any aggregation without a partition key returns a `Aggregation query used without partition
key` warning while scanning the cluster. Run reconciliation queries through Trino, which can
express them — that is a large part of what the `cassandra` catalog is for here.

**Schemaless JSON on the wire pushes type decisions downstream, where they are harder.** The whole
`castTimestamp`/`toTimestamp` SMT chain, and the second-precision Iceberg column it produces, exist
solely because the type was not declared at the source. Schema-on-write costs a registry;
schema-on-read costs a workaround in every consumer.

## 8. BSON's self-describing cost vs. a known schema

MongoDB's flexibility in §4 and §5 has a performance price that is worth naming explicitly, because
it is the same root cause as the timestamp problem in §1: **no declared schema means type/shape
decisions happen at read time instead of write time.**

**Every document repeats its own field names.** Unlike Cassandra (schema declared once in
`cassandra/init.cql`, each row stores only values) or Iceberg (schema in the table metadata, columns
stored once), BSON stores `sourceIp`, `timestampIso`, etc. as literal bytes *inside every single
document*. Across the collection that is pure repeated overhead — more bytes on disk, more bytes to
read off disk, for the same information a fixed-schema store stores once.

**Unindexed queries pay a parsing tax.** Cassandra and Iceberg both know a column's byte offset (or
which file/column-chunk to read) from metadata alone. MongoDB has no such offset table — to find
`sourceIp` in a document, the engine walks the BSON byte stream field by field. `createIndex` erases
this difference for the indexed field (an index lookup is a B-tree, same as any other store), but a
`find()` on an unindexed field means a full collection scan that re-parses every document's BSON
structure, not just its bytes.

**No write-time validation shifts the cost to every reader.** Without a `$jsonSchema` validator,
MongoDB will happily accept a document where `timestampIso` is missing or is a number instead of a
string. Cassandra's CQL schema and Iceberg's declared schema both reject that at write time. This
project doesn't hit it because every write goes through the same Flink sink, but it is why the
`LEFT JOIN ... WHERE c.source_ip IS NULL` pattern in §3 has to defensively handle absent fields —
nothing upstream guarantees the field exists.

**Where this project would actually feel it:** the reconciliation queries in §6 run through Trino
against all three stores. The Cassandra and Iceberg legs benefit from connector-level pushdown using
known column metadata; the MongoDB leg either uses the index on `sourceIp` (if queried) or falls back
to a collection scan that decodes each document's self-describing structure. At 100 rows this is
invisible. At production volume, an unindexed cross-store reconciliation query against MongoDB is
the leg most likely to dominate the query's total latency.

The tradeoff is real in both directions, not just a MongoDB weakness: the schema-on-write stores paid
their cost earlier, in this project, as the SMT chain in §1 and the `init.cql` column declarations —
work that had to happen before the first row was ever written. BSON deferred that cost to every query
instead of paying it once at design time.
