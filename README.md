  # db-playground

A local, Docker-based playground for exploring a streaming data pipeline that fans one Kafka
topic out into four very different storage engines — Apache Iceberg (via Kafka Connect),
Cassandra and MongoDB (via a Flink job), and DynamoDB (via a fully isolated Kafka Streams
module) — three of them queryable together through Trino.

```
                                   +-> Kafka Connect (Iceberg sink) -> Iceberg (MinIO + REST catalog)
producer (Spring Boot) -> Kafka  -|
                                   +-> Flink job -> Cassandra
                                   |               -> MongoDB
                                   +-> Kafka Streams (dynamo) -> DynamoDB (2 tables)

Trino (+ SQLPad) queries Iceberg, Cassandra, and MongoDB side by side.
DynamoDB is not queryable via Trino — see docs/dynamodb-tutorial.md §5.
```

Everything runs in Docker except the three Spring Boot / Flink / Kafka Streams Gradle modules
(`kafka`, `flink`, `dynamo`), which run locally against the containerized services.

## Modules

| Module | What it does |
|---|---|
| `kafka` | Spring Boot app. On startup, produces mock HTTPS session records to `playground.https-sessions`; also includes a basic consumer. |
| `flink` | Spring Boot app wrapping a Flink job. Consumes `playground.https-sessions` as a second, independent consumer group and writes each record to both Cassandra and MongoDB via custom `sink2` `Sink`/`SinkWriter` implementations. |
| `dynamo` | Spring Boot app wrapping a Kafka Streams topology. Consumes `playground.https-sessions` as a third, independent consumer group and writes each record to two DynamoDB tables — an `UpdateItem` aggregate and a `PutItem` event log — fully isolated from the `kafka`/`flink` modules (no shared code). See `docs/stream-processing-comparison.md` for why this module uses Kafka Streams instead of Flink. |
| `kafka-connect` | Custom Kafka Connect image (`kafka-connect/Dockerfile`) with the Iceberg sink connector baked in; sinks the same topic into an Iceberg table. |

## Services (`docker-compose.yml`)

| Service | URL | Purpose |
|---|---|---|
| Kafka | `localhost:9092` | Broker (KRaft, single node) |
| Kafka UI | http://localhost:8080 | Browse topics/messages |
| Kafka Connect | http://localhost:8083 | REST API for managing connectors |
| MinIO API / Console | http://localhost:9000 / http://localhost:9001 | S3-compatible storage for Iceberg (`admin` / `password`) |
| Iceberg REST catalog | http://localhost:8181 | Table metadata for Iceberg |
| Cassandra | `localhost:9042` | Wide-column store, direct Flink sink target |
| MongoDB | `localhost:27017` | Document store, direct Flink sink target |
| DynamoDB Local | `localhost:8000` | Key-value store, direct Kafka Streams (`dynamo`) sink target — not exposed to Trino |
| Trino | http://localhost:8082 | SQL across the `iceberg`, `cassandra`, and `mongodb` catalogs |
| SQLPad | http://localhost:3000 | Browser SQL client pre-wired to Trino (`admin@playground.local` / `admin`) |

## Quick start

1. Set `MOUNT_ROOT` in a repo-root `.env` file, e.g.:
   ```
   MOUNT_ROOT=D:/work/docker/mount
   ```
   (required — every bind-mounted service fails fast without it; see `.gitignore`, `.env` is not committed).
2. Build and start everything:
   ```bash
   docker compose build kafka-connect
   docker compose up -d
   ```
   This also registers the Iceberg sink connector and creates the Cassandra keyspace/table automatically.
3. Produce mock data (publishes on startup):
   ```bash
   ./gradlew :kafka:bootRun
   ```
4. Run the Cassandra/MongoDB sink job (runs locally, not containerized):
   ```bash
   ./gradlew :flink:bootRun
   ```
4a. Run the DynamoDB sink module (runs locally; needs step 3 to have run first so the topic exists
   — see `docs/stream-processing-comparison.md` §4):
   ```bash
   ./gradlew :dynamo:bootRun
   ```
5. Query everything from Trino:
   ```bash
   docker compose exec trino trino
   ```
   ```sql
   SELECT count(*) FROM iceberg.playground.https_sessions;
   SELECT count(*) FROM cassandra.playground.https_sessions;
   SELECT count(*) FROM mongodb.playground.https_sessions;
   ```

See `DEPLOYMENT.md` for the full step-by-step build (every non-default config value and why it's
there) and checklist.

## Docs

- `DEPLOYMENT.md` — from-scratch deployment guide and full checklist.
- `docs/kafka-connect-tutorial.md` — Kafka Connect + the Iceberg sink connector.
- `docs/iceberg-tutorial.md` — Iceberg table format and REST catalog.
- `docs/cassandra-tutorial.md` — Cassandra data model and gotchas.
- `docs/mongodb-tutorial.md` — MongoDB data model and gotchas.
- `docs/dynamodb-tutorial.md` — DynamoDB data model (two tables, two write shapes), the
  hot-partition-key problem, and why it isn't queryable from Trino.
- `docs/trino-tutorial.md` — querying across the `iceberg`, `cassandra`, and `mongodb` catalogs
  with Trino.
- `kafka-connect/README.md` — narrower guide focused on the Kafka Connect + Iceberg slice.

Deeper, cross-cutting articles — these are about the pipeline as a whole rather than one component:

- `docs/delivery-semantics.md` — what each path guarantees when something crashes: checkpointing,
  offset handling, the `Sink2` contract, and why replay is idempotent for Cassandra but not MongoDB.
- `docs/cross-store-consistency.md` — one `Instant` becomes five representations; timestamp
  precision loss, key collisions, and which field to reconcile on.
- `docs/query-federation.md` — connector pushdown, the missing statistics that disable Trino's
  cost-based optimizer, and how to read a federated query plan.
- `docs/flink-runtime.md` — how the embedded Flink MiniCluster runs inside Spring Boot, parallelism
  vs. partition count, job-graph serialization, and the two build workarounds in `flink/build.gradle`.
- `docs/database-comparison.md` — Iceberg, Cassandra, MongoDB, and DynamoDB side by side: identity
  models, query capability, operational failure modes, and when to reach for each in production.
- `docs/stream-processing-comparison.md` — Kafka Streams vs. Flink vs. Spark Structured Streaming:
  deployment model, state/exactly-once semantics, and why this repo picked Kafka Streams for the
  `dynamo` module instead of a third Flink sink.

## License

[MIT](LICENSE)
