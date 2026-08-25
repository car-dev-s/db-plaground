# MongoDB Tutorial (for this project)

MongoDB is a **document database** — no fixed schema per collection, no CREATE TABLE step. In
this repo it runs single-node (`mongo:7`, port 27017, data bind-mounted to
`D:\work\docker\mount\mongodb`), and the `playground` database / `https_sessions` collection are
created implicitly by the Flink sink's first `insertOne` call — there's no init script, unlike
Cassandra's `cassandra/init.cql`.

## 1. Core mental model

A MongoDB **document** is a BSON (binary JSON) object; a **collection** is a named group of
documents, analogous to a table but without an enforced column set — two documents in the same
collection can have entirely different fields. This project's documents all share the same shape
by convention (mirroring `HttpsSessionEvent`), but MongoDB itself doesn't enforce that; nothing
stops a future write with an extra or missing field from landing in the same collection.

Every document gets an `_id` field automatically (an `ObjectId`) unless you supply your own —
this project doesn't set one explicitly, so MongoDB assigns a generated `ObjectId` per session
record, distinct from any of the session's own fields (`sourceIp`, `timestamp`, etc.).

## 2. `Instant` → BSON `Date`, and why `timestampIso` still exists

BSON has a native `Date` type (`Date` in the Java driver, `ISODate(...)` in `mongosh` output) —
millisecond-precision UTC. The `MongoHttpsSessionSink` maps `HttpsSessionEvent.timestamp`
(`java.time.Instant`) via `Date.from(instant)`, so `timestamp` is stored as a real, queryable,
sortable/range-able native datetime — no epoch-number workaround needed here, unlike the Iceberg
sink path (see `docs/iceberg-tutorial.md` and this project's Kafka Connect
`castTimestamp`/`toTimestamp` SMT chain, which exists specifically because the Iceberg connector
*doesn't* have this problem solved for it).

`timestampIso` (a plain string) is still written alongside it, for consistency with the Iceberg
table's schema — not because MongoDB needs it. One real trade-off worth knowing: `Date` truncates
to millisecond precision, while `timestampIso` (produced from the same `Instant` before it's
serialized) preserves the original sub-millisecond precision as text. If you ever need
microsecond-level ordering guarantees, `timestampIso` is the field with more precision, at the
cost of not being natively sortable/range-queryable the way `timestamp` is.

Sharper than that, and worth flagging: `timestampIso` is not just *less convenient* to sort, it
sorts **incorrectly**. `Instant.toString()` emits zero, three, six, or nine fractional digits,
trimming trailing zero groups, so `...23.420Z` string-compares as *greater than* `...23.420492600Z`.
Use `timestamp` for ordering and ranges, and `timestampIso` only for exact identity — it is the
one field that survives every hop losslessly, which makes it the right cross-store join key.
See `docs/cross-store-consistency.md` §2.

## 3. Gotchas

**No schema enforcement means no schema *validation* by default.** A typo'd field name in a
future producer change (`sourceip` instead of `sourceIp`) doesn't error — it just silently
creates a new field, and existing documents don't get it retroactively. Unlike Iceberg's
`iceberg.tables.evolve-schema-enabled` (an explicit, logged schema change), this happens with zero
signal. MongoDB supports optional JSON Schema validators per collection if this ever becomes a
real concern; this project doesn't use one, matching its playground scope.

**The generated `_id` makes every replay a duplicate.** Because `insertOne` supplies no `_id`, the
driver mints a fresh `ObjectId` per call — identity comes from the *write*, not from the data. The
Flink job restarts from `earliest` on every run, so running it twice gives you two copies of every
session, while Cassandra (whose key *is* data-derived) converges. This is the most visible
inconsistency in the whole project; the deterministic-`_id` fix is in
`docs/delivery-semantics.md` §5.

**No indexes are created here beyond the default `_id` index.** Every query in this project's
manual verification steps (`find()`, `countDocuments()`) does a full collection scan. Fine at
playground data volumes; the first index you'd add for a `source_ip`-scoped query pattern (mirroring
Cassandra's partition key) would be `db.https_sessions.createIndex({ sourceIp: 1, timestamp: -1 })`.

**Write concern defaults matter more than they look.** The driver's default write concern
(`w: 1`, acknowledged by the primary) is what `insertOne` uses here — fine for a single-node
setup where there's only one node to acknowledge. On a real replica set, the same call could
return before data is durable on a majority of nodes unless write concern is explicitly raised.

## 4. How `mongosh` and the Trino `mongodb` catalog relate

Both read the same underlying collection. `mongosh` talks MongoDB's native wire protocol
directly; Trino's `mongodb` connector (`trino/catalog/mongodb.properties`) does the same under the
hood, then infers a SQL-shaped view of the collection's documents (sampling document shapes to
build a schema, since MongoDB itself has none). A document inserted by the Flink sink is visible
from both `mongosh` and `SELECT * FROM mongodb.playground.https_sessions` in Trino/SQLPad
immediately — no replication lag to account for on a single node.

## 5. Useful commands

```bash
# Open a mongosh shell against the playground database
docker exec -it playground-mongodb mongosh playground

# Inspect recent documents
docker exec -it playground-mongodb mongosh playground --eval "db.https_sessions.find().limit(10)"

# Count documents
docker exec -it playground-mongodb mongosh playground --eval "db.https_sessions.countDocuments()"

# Reset data without dropping the collection
docker exec -it playground-mongodb mongosh playground --eval "db.https_sessions.deleteMany({})"

# Run the Flink sink locally (writes into this collection, and into Cassandra — see cassandra-tutorial.md)
./gradlew :flink:bootRun

# Query through Trino/SQLPad instead of mongosh
docker compose exec trino trino --execute "SELECT * FROM mongodb.playground.https_sessions LIMIT 10;"
```
