# Kafka Connect + Iceberg (local)

Adds a local pipeline for sinking Kafka topics into Apache Iceberg tables, backed by MinIO (S3-compatible storage) and an Iceberg REST catalog. Everything runs in Docker — no external accounts needed.

## What was added

- **`kafka-connect`** — Kafka Connect worker (`confluentinc/cp-kafka-connect:7.7.1`) built via `kafka-connect/Dockerfile`, with the Iceberg Sink Connector (`io.tabular.iceberg.connect.IcebergSinkConnector`, from the `databricks/iceberg-kafka-connect` runtime distribution) baked in. Connects to the existing `kafka` broker on its internal listener (`kafka:29092`).
- **`minio`** — S3-compatible object store that holds the Iceberg warehouse's data and metadata files.
- **`minio-init`** — one-shot container that creates the `warehouse` bucket in MinIO on startup.
- **`iceberg-rest`** — Iceberg REST catalog (`tabulario/iceberg-rest`), tracks table schemas/snapshots, backed by the `warehouse` bucket in MinIO.
- **`kafka-connect-init`** — one-shot container that waits for Kafka Connect and the Iceberg REST catalog, creates the `playground` namespace, and registers the Iceberg sink connector on startup.
- **`trino`** — SQL query engine (`trinodb/trino`) with an `iceberg` catalog (`trino/catalog/iceberg.properties`) pointed at `iceberg-rest` and MinIO, so Iceberg tables can be queried with plain SQL.

## Building the Kafka Connect image

`docker compose up -d` (below) builds it automatically on first run, but to build or rebuild it on its own:

```bash
docker compose build kafka-connect
```

This runs `kafka-connect/Dockerfile`, which starts from `confluentinc/cp-kafka-connect:7.7.1` and downloads the Iceberg sink plugin jar into it. Rebuild after changing the Dockerfile (e.g. bumping `ICEBERG_CONNECT_VERSION`) or to pick up a new base image.

## Spinning it up

```bash
docker compose up -d
```

This builds the `kafka-connect` image on first run. Services and ports:

| Service | URL | Purpose |
|---|---|---|
| Kafka | `localhost:9092` | Broker (external listener) |
| Kafka UI | http://localhost:8080 | Browse topics/messages |
| Kafka Connect | http://localhost:8083 | REST API for managing connectors |
| MinIO API | http://localhost:9000 | S3 endpoint (`admin` / `password`) |
| MinIO Console | http://localhost:9001 | Browse the `warehouse` bucket |
| Iceberg REST catalog | http://localhost:8181 | Table catalog metadata |
| Trino | http://localhost:8082 | SQL query engine for Iceberg tables |

Check Connect is up and the Iceberg plugin loaded:

```bash
curl http://localhost:8083/connector-plugins | grep -i iceberg
```

## Using the Iceberg REST catalog

The `iceberg-rest` service exposes the [Iceberg REST catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) directly, and is also what the `kafka-connect` sink connector and `trino` both talk to for table metadata. You normally don't call it by hand, but it's useful for checking what exists:

```bash
# list namespaces
curl http://localhost:8181/v1/namespaces

# create a namespace (e.g. to hold the sink connector's tables)
curl -X POST http://localhost:8181/v1/namespaces \
  -H 'Content-Type: application/json' \
  -d '{"namespace": ["playground"]}'

# list tables in a namespace
curl http://localhost:8181/v1/namespaces/playground/tables
```

The easier way to work with the data day-to-day is through **Trino**, which has an `iceberg` catalog already wired up to this same REST catalog + MinIO warehouse:

```bash
docker compose exec trino trino
```

```sql
SHOW SCHEMAS FROM iceberg;
CREATE SCHEMA IF NOT EXISTS iceberg.playground;
SHOW TABLES FROM iceberg.playground;
SELECT * FROM iceberg.playground.<table_name> LIMIT 10;
```

Any table the Iceberg sink connector creates in the `playground` namespace/schema will show up here — the REST catalog and Trino's `iceberg` catalog are looking at the same metadata.

## Registering the sink connector

The connector config lives at `kafka-connect/iceberg-sink-connector-config.json` and sinks the `playground.https-sessions` topic into an Iceberg table `playground.https_sessions`. Registration is automatic: the `kafka-connect-init` service waits for Kafka Connect and the Iceberg REST catalog to be ready, creates the `playground` namespace, and `PUT`s the connector config on every `docker compose up`. The `PUT` is idempotent, so re-running it (e.g. after a restart) just reapplies the same config.

Check it's running:

```bash
curl http://localhost:8083/connectors/https-sessions-iceberg-sink/status
```

To register it by hand (e.g. under a different name, or from `kafka-connect/iceberg-sink-connector.json` which includes the `name` wrapper for the `POST` form):

```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @kafka-connect/iceberg-sink-connector.json
```

The table itself is created automatically (`iceberg.tables.auto-create-enabled`) the first time the connector sees a record, with its schema inferred from the JSON.

### Gotchas hit while wiring this up

- **Plugin jar layout**: Kafka Connect only treats a directory as an isolated plugin if the jar(s) live in their own subdirectory under a `plugin.path` entry — a jar dropped directly at the top level is silently ignored. The Dockerfile extracts the runtime distribution into its own folder for this reason.
- **No Maven Central artifact**: `iceberg-kafka-connect-runtime` isn't published to Maven Central. It ships as a `.zip` on the connector's GitHub releases (originally `tabular-io/iceberg-kafka-connect`, now `databricks/iceberg-kafka-connect`).
- **Key converter**: the HTTPS session producer keys messages with a plain string (`sourceIp:sourcePort`), not JSON — the connector's `key.converter` has to be `StringConverter`, not `JsonConverter`, or every record fails deserialization.
- **Transaction state topic**: the connector uses a transactional producer for exactly-once commits, which needs the `__transaction_state` topic. Its default replication factor (3) can't be satisfied with this single-broker setup, so the broker sets `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` / `..._MIN_ISR` to `1`.
- **Offset reset**: a brand-new sink task has no committed offsets and Kafka's client default (`latest`) means it skips any messages already sitting on the topic. `consumer.override.auto.offset.reset: earliest` makes it pick up existing data instead of only new messages.
- **Commit interval**: the connector batches writes and only commits a snapshot every `iceberg.control.commit.interval-ms` (default 5 minutes). It's set to 10s here so data shows up quickly for local testing — raise it for anything resembling production.
- **`Instant` fields infer as `double`, not `timestamp`**: with `value.converter.schemas.enable: false`, the connector infers each column's type from the raw JSON value's shape. Spring's `JsonSerializer` writes a Java `Instant` as fractional epoch-seconds (e.g. `1787498606.0625362`), which is indistinguishable from any other decimal number, so it lands as Iceberg `double`. An ISO-8601 string field doesn't fix this either — this connector doesn't sniff strings for timestamp patterns, so it just becomes `varchar`. The fix has to happen in the pipeline itself: an SMT chain converts `HttpsSessionMock.timestamp` before it reaches Iceberg —
  ```json
  "transforms": "castTimestamp,toTimestamp",
  "transforms.castTimestamp.type": "org.apache.kafka.connect.transforms.Cast$Value",
  "transforms.castTimestamp.spec": "timestamp:int64",
  "transforms.toTimestamp.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
  "transforms.toTimestamp.field": "timestamp",
  "transforms.toTimestamp.target.type": "Timestamp",
  "transforms.toTimestamp.unix.precision": "seconds"
  ```
  `TimestampConverter`'s `unix` source format only accepts a `Long`, not a fractional `Double`, so `Cast$Value` truncates to whole seconds first — this loses sub-second precision. `HttpsSessionMock` also carries a separate `timestampIso` string field (full nanosecond precision, `Instant.toString()`) for comparison; it stays `varchar` in Iceberg since nothing converts it. Changing the connector's `timestamp` type on an existing table requires dropping and recreating it (`double` → `timestamp` isn't a compatible schema evolution).
