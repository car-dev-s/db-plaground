# db-playground

A local, Docker-based playground for exploring a streaming data pipeline that fans one Kafka
topic out into three very different storage engines — Apache Iceberg (via Kafka Connect),
Cassandra, and MongoDB (both via a Flink job) — queryable together through Trino.

```
                                   +-> Kafka Connect (Iceberg sink) -> Iceberg (MinIO + REST catalog)
producer (Spring Boot) -> Kafka  -|
                                   +-> Flink job -> Cassandra
                                                  -> MongoDB

Trino (+ SQLPad) queries Iceberg, Cassandra, and MongoDB side by side.
```

Everything runs in Docker except the two Spring Boot / Flink Gradle modules (`kafka`, `flink`),
which run locally against the containerized services.

## Modules

| Module | What it does |
|---|---|
| `kafka` | Spring Boot app. On startup, produces mock HTTPS session records to `playground.https-sessions`; also includes a basic consumer. |
| `flink` | Spring Boot app wrapping a Flink job. Consumes `playground.https-sessions` as a second, independent consumer group and writes each record to both Cassandra and MongoDB via custom `sink2` `Sink`/`SinkWriter` implementations. |
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
- `docs/trino-tutorial.md` — querying across all three stores with Trino.
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

## License

[MIT](LICENSE)
