# Deploying Kafka → Kafka Connect → Iceberg (on MinIO) → Trino, from scratch

This is a step-by-step guide to building the whole local pipeline yourself, starting from
plain Docker images. It lists every config value that had to be changed from the image's
default, and every gotcha that will bite you if skipped. If you just want to run the
already-built setup in this repo, see `docker compose up -d` and
`kafka-connect/README.md` instead — this doc explains *why* it's built the way it is.

Pipeline shape:

```
producer (Spring Kafka) -> Kafka topic -> Kafka Connect (Iceberg sink connector)
   -> Iceberg table (metadata via REST catalog, data files in MinIO/S3) -> Trino (SQL)
```

All images are pinned to specific versions (not `:latest`) so the setup is reproducible.

## 1. Kafka broker

Image: `apache/kafka:4.3.1` (KRaft mode, no separate ZooKeeper needed).

Minimal config changes from a bare single-node KRaft broker:

```yaml
environment:
  KAFKA_NODE_ID: 1
  KAFKA_PROCESS_ROLES: broker,controller
  KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
  KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,INTERNAL://kafka:29092
  KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT
  KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

Why each of these differs from default:

- **Two listeners (`PLAINTEXT` + `INTERNAL`)**: apps on your host connect via `localhost:9092`,
  but containers (Kafka Connect, Kafka UI) must use the Docker network name `kafka:29092` —
  `localhost` inside a container means the container itself, not the host.
- **`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`**: default is 3. A single-broker cluster
  can never satisfy replication factor 3 — the topic creation would hang/fail.
- **`KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` / `..._MIN_ISR: 1`**: same problem,
  but for the `__transaction_state` topic. This one is easy to miss because it only
  matters once something uses a *transactional* producer — the Iceberg sink connector
  does, for exactly-once commits. Without this, the connector's producer hangs and
  eventually fails with `TimeoutException: Timeout expired while initializing
  transactional state`.

### Gotcha: don't casually restart just the `kafka` service

Kafka Connect's internal topics (`_connect-configs`, `_connect-offsets`, `_connect-status`)
must be created with `cleanup.policy=compact`. If the broker gets restarted/reset while
Connect isn't running and recreates these topics with defaults, Connect will crash on
startup with a `ConfigException` about cleanup policy. If that happens, don't try to
patch topic configs by hand — for disposable local dev, it's faster to `docker compose
down` (drops volumes/topics) and `docker compose up -d` for a clean slate.

## 2. Kafka Connect (with the Iceberg sink plugin)

Image: built from `confluentinc/cp-kafka-connect:7.7.1` — see `kafka-connect/Dockerfile`.

### 2a. Why a custom image is needed

The Iceberg Kafka Connect sink (`io.tabular.iceberg.connect.IcebergSinkConnector`) is
**not published to Maven Central**. It's distributed only as a `.zip` runtime bundle via
GitHub Releases, currently under `databricks/iceberg-kafka-connect` (the project moved
there from `tabular-io/iceberg-kafka-connect`). So the base Connect image has to have
the plugin baked in at build time:

```dockerfile
FROM confluentinc/cp-kafka-connect:7.7.1

ARG ICEBERG_CONNECT_VERSION=0.6.19
RUN curl -sSL -o /tmp/iceberg-kafka-connect-runtime.zip \
    "https://github.com/databricks/iceberg-kafka-connect/releases/download/v${ICEBERG_CONNECT_VERSION}/iceberg-kafka-connect-runtime-${ICEBERG_CONNECT_VERSION}.zip" && \
    python3 -m zipfile -e /tmp/iceberg-kafka-connect-runtime.zip /usr/share/java/ && \
    rm /tmp/iceberg-kafka-connect-runtime.zip
```

Gotchas here:

- **Plugin must land in its own subdirectory.** Kafka Connect's plugin scanner only
  recognizes a directory under `plugin.path` as a plugin if its jars are isolated in a
  dedicated subfolder. A jar dropped loosely at the top level of `/usr/share/java` is
  silently ignored — the connector class just never shows up in
  `GET /connector-plugins`, with no error logged. The zip already extracts into its own
  `iceberg-kafka-connect-runtime-<version>/` folder, which is what makes this work.
- **No `unzip` in the base image.** `confluentinc/cp-kafka-connect` is RHEL8-based and
  doesn't ship `unzip`, but does have `python3`, so `python3 -m zipfile -e` is used
  instead of `unzip`.
- Pin `ICEBERG_CONNECT_VERSION` to a real release tag — check
  https://github.com/databricks/iceberg-kafka-connect/releases for current versions.

### 2b. Minimal Connect worker config

```yaml
environment:
  CONNECT_BOOTSTRAP_SERVERS: kafka:29092
  CONNECT_GROUP_ID: playground-connect
  CONNECT_CONFIG_STORAGE_TOPIC: _connect-configs
  CONNECT_OFFSET_STORAGE_TOPIC: _connect-offsets
  CONNECT_STATUS_STORAGE_TOPIC: _connect-status
  CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
  CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
  CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
  CONNECT_KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
  CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
  CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE: "false"
  CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
  CONNECT_REST_ADVERTISED_HOST_NAME: kafka-connect
  CONNECT_PLUGIN_PATH: /usr/share/java
```

Same replication-factor-1 story as the broker: all three `*_REPLICATION_FACTOR`
settings default to 3 and must be dropped to 1 for a single-broker cluster, or the
worker never finishes starting up.

`CONNECT_KEY_CONVERTER` / `CONNECT_VALUE_CONVERTER` here are just the **worker-level
defaults** — each connector config can (and, for this pipeline, does) override them
per-connector. See section 5.

Build it: `docker compose build kafka-connect`. Verify the plugin loaded:

```bash
curl http://localhost:8083/connector-plugins | grep -i iceberg
```

If nothing prints, the jar layout is wrong (see gotcha above) — check `docker compose
logs kafka-connect` for plugin scan errors and confirm the folder structure inside the
image with `docker compose exec kafka-connect ls -la /usr/share/java`.

## 3. MinIO (S3-compatible storage for Iceberg data files)

Image: `minio/minio:RELEASE.2025-09-07T16-13-09Z`.

```yaml
environment:
  MINIO_ROOT_USER: admin
  MINIO_ROOT_PASSWORD: password
command: server /data --console-address ":9001"
```

The only things that differ from a bare MinIO container: setting root credentials
(there is no sane default) and passing `--console-address` so the web console is
reachable on a separate port (9001) alongside the S3 API (9000).

Iceberg needs a bucket to act as its warehouse root. MinIO doesn't auto-create buckets,
so a one-shot `minio/mc` init container creates it on startup:

```yaml
minio-init:
  image: minio/mc:RELEASE.2025-08-13T08-35-41Z
  depends_on: [minio]
  entrypoint: >
    /bin/sh -c "
    until mc alias set local http://minio:9000 admin password; do sleep 1; done;
    mc mb --ignore-existing local/warehouse;
    "
```

The `until` loop is necessary because `mc alias set` will fail if it runs before MinIO
has finished starting — `depends_on` only waits for the container to start, not for the
service inside it to be ready.

## 4. Iceberg REST catalog

Image: `tabulario/iceberg-rest:1.6.0`. This is what tracks table schemas and snapshot
history, and is the one thing both Kafka Connect and Trino talk to for table metadata.

```yaml
environment:
  CATALOG_WAREHOUSE: s3://warehouse/
  CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
  CATALOG_S3_ENDPOINT: http://minio:9000
  CATALOG_S3_PATH__STYLE__ACCESS: "true"
  AWS_REGION: us-east-1
  AWS_ACCESS_KEY_ID: admin
  AWS_SECRET_ACCESS_KEY: password
depends_on: [minio, minio-init]
```

Gotchas:

- **`CATALOG_S3_PATH__STYLE__ACCESS: "true"` is required for MinIO.** Real AWS S3 uses
  virtual-hosted-style URLs (`bucket.s3.amazonaws.com`) by default; MinIO needs
  path-style (`minio:9000/bucket`) instead, or every request 404s.
  Double underscore (`__`) in the env var name maps to a literal dot in the property
  key (`s3.path-style-access`) — this is the REST catalog image's own naming
  convention, easy to typo.
- **`AWS_REGION` must still be set** even though MinIO doesn't care about regions —
  the AWS S3 SDK client refuses to initialize without one.
- REST catalogs don't auto-create namespaces. You must `POST /v1/namespaces` before
  any table can be created in it (see section 6).

## 5. Trino (SQL query engine)

Image: `trinodb/trino:465`.

Trino just needs a catalog file mounted in, pointing at the same REST catalog + MinIO:

`trino/catalog/iceberg.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
iceberg.rest-catalog.warehouse=s3://warehouse/
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=admin
s3.aws-secret-key=password
```

```yaml
trino:
  image: trinodb/trino:465
  ports: ["8082:8080"]
  volumes:
    - ./trino/catalog:/etc/trino/catalog
  depends_on: [iceberg-rest, minio]
```

`fs.native-s3.enabled` + `s3.path-style-access=true` is the same MinIO path-style
requirement as section 4, just in Trino's own config dialect. Any file under
`trino/catalog/*.properties` becomes an available catalog named after the filename —
`iceberg.properties` here becomes the `iceberg` catalog referenced in SQL as
`iceberg.<schema>.<table>`.

## 6. Registering the Iceberg sink connector

This is the step that actually wires Kafka → Iceberg together; everything above is
just infrastructure sitting idle until a connector is registered.

This is now automatic: a `kafka-connect-init` one-shot container waits for Kafka
Connect and the Iceberg REST catalog to be ready, creates the `playground` namespace
(REST catalogs don't auto-create these), and `PUT`s the connector config from
`kafka-connect/iceberg-sink-connector-config.json`. The `PUT` is idempotent, so it's
safe to run again after a restart. Check it came up:

```bash
curl http://localhost:8083/connectors/https-sessions-iceberg-sink/status
```

Full working config, for reference (`kafka-connect/iceberg-sink-connector.json`):

```json
{
  "name": "https-sessions-iceberg-sink",
  "config": {
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "1",
    "topics": "playground.https-sessions",
    "iceberg.tables": "playground.https_sessions",
    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.tables.evolve-schema-enabled": "true",
    "iceberg.control.commit.interval-ms": "10000",
    "iceberg.catalog": "iceberg",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "http://iceberg-rest:8181",
    "iceberg.catalog.warehouse": "s3://warehouse/",
    "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "iceberg.catalog.s3.endpoint": "http://minio:9000",
    "iceberg.catalog.s3.path-style-access": "true",
    "iceberg.catalog.client.region": "us-east-1",
    "iceberg.catalog.s3.access-key-id": "admin",
    "iceberg.catalog.s3.secret-access-key": "password",
    "consumer.override.auto.offset.reset": "earliest",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "transforms": "castTimestamp,toTimestamp",
    "transforms.castTimestamp.type": "org.apache.kafka.connect.transforms.Cast$Value",
    "transforms.castTimestamp.spec": "timestamp:int64",
    "transforms.toTimestamp.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
    "transforms.toTimestamp.field": "timestamp",
    "transforms.toTimestamp.target.type": "Timestamp",
    "transforms.toTimestamp.unix.precision": "seconds"
  }
}
```

To register it by hand instead (e.g. under a different name):

```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @kafka-connect/iceberg-sink-connector.json

curl http://localhost:8083/connectors/https-sessions-iceberg-sink/status
```

Every one of the non-obvious fields above exists to work around a gotcha discovered
while building this:

| Config | Why it's set this way |
|---|---|
| `key.converter: StringConverter` | The worker-level default is `JsonConverter` (section 2b), but this pipeline's Kafka message keys are plain strings (e.g. `"203.0.113.5:54321"`), not JSON. Leaving `JsonConverter` here fails every record with a `DataException`/`JsonParseException` on the key. Override it per-connector. |
| `value.converter.schemas.enable: false` | Values are plain JSON objects with no embedded Connect schema envelope; leaving schema mode on causes deserialization errors. |
| `consumer.override.auto.offset.reset: earliest` | A brand-new sink task has no committed consumer offsets. Kafka's client default is `latest`, so if any messages were already produced to the topic before the connector started, they're silently skipped forever. `earliest` makes the task pick up existing data. |
| `iceberg.control.commit.interval-ms: 10000` | Default is 5 minutes — fine for production, painfully slow for local testing/iteration. Lowered to 10s so writes show up quickly. |
| `iceberg.tables.auto-create-enabled: true` | Lets the connector create the destination table itself, with schema inferred from the first record's JSON, instead of requiring it to be pre-created via the REST catalog. |
| `iceberg.catalog.s3.path-style-access: true` | Same MinIO requirement as sections 4/5, but in the connector's own catalog-client config namespace. |
| `transforms: castTimestamp,toTimestamp` | Without schemas, the connector infers a column's type from the raw JSON value's shape. `HttpsSessionMock.timestamp` (a Java `Instant`) serializes as fractional epoch-seconds (e.g. `1787498606.0625362`), which is indistinguishable from any other decimal and infers as `double`. This SMT chain casts it to a `Long` (whole seconds, so sub-second precision is lost) then converts it to Connect's `Timestamp` logical type before the sink writes it, so Iceberg gets `timestamp` instead of `double`. |

### Gotcha: reconfiguring a live connector is fragile

The Iceberg sink connector splits work internally between a "coordinator" and
"worker/channel" thread per task, coordinated over a Kafka control topic
(`control-iceberg`). Repeatedly `PUT`-ing config changes into a running connector can
leave these internal threads in an inconsistent state — symptoms are the task staying
`RUNNING` but logs showing `Commit timeout reached... committed to 0 table(s)` forever,
with no error. If you hit persistent zero-row commits after reconfiguring:

```bash
curl -X DELETE http://localhost:8083/connectors/https-sessions-iceberg-sink
# also drop the table via the REST catalog if it was already created, and the
# playground.https-sessions / control-iceberg Kafka topics, for a truly clean slate
```

Then register the connector fresh with its final config, **wait for both the connector
and its task to report `RUNNING`** via the status endpoint before producing any data,
and only then run the producer.

This also applies to changing a column's *type* via `transforms` (see the
`timestamp`/`double` gotcha above): `iceberg.tables.evolve-schema-enabled` only allows
compatible widenings, not `double` → `timestamp`. Drop the table via Trino
(`DROP TABLE iceberg.playground.https_sessions`) so it gets recreated with the corrected
inferred type on the next record.

Also note: `PUT /connectors/{name}/config` expects just the flat config object (the
inner `"config": {...}` map), not the `{"name": ..., "config": {...}}` envelope used by
`POST /connectors` — sending the wrapped form returns an HTTP 500 with a confusing
Jackson deserialization error.

## 7. Producing data

This repo's Spring Boot `kafka` module has a `CommandLineRunner` gated behind the
`load-https-sessions` Spring profile that publishes mock HTTPS session records to the
`playground.https-sessions` topic (see `kafka/src/main/resources/application.yml` for
the topic name and record count). Run it with that profile active to feed the pipeline.

## 8. Querying with Trino

```bash
docker compose exec trino trino
```

```sql
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.playground;

SELECT count(*) FROM iceberg.playground.https_sessions;

SELECT sourceip, destinationip, method, statuscode, bytessent, timestamp, timestampiso
FROM iceberg.playground.https_sessions
LIMIT 10;
```

Any table the sink connector creates under the `playground` namespace shows up here
automatically — Trino's `iceberg` catalog and the connector both read/write through the
same REST catalog, so there's no separate registration step for Trino.

### Gotcha: stale metadata cache right after a write

If you query immediately after the first commit and see `0` rows even though the
connector logs confirm a snapshot was committed, it's usually Trino's own metadata
cache being one query behind — re-run the same query and it corrects itself. You can
also verify a commit landed directly against the REST catalog, bypassing Trino
entirely:

```bash
curl http://localhost:8181/v1/namespaces/playground/tables/https_sessions
# look for a non-null "current-snapshot-id" and check "summary.added-records" in the
# corresponding snapshot entry
```

## Full checklist, start to finish

1. `docker compose build kafka-connect`
2. `docker compose up -d` → `kafka-connect-init` creates the namespace and registers the connector automatically
3. `curl http://localhost:8083/connector-plugins | grep -i iceberg` → confirms plugin loaded
4. `curl http://localhost:8083/connectors/https-sessions-iceberg-sink/status` → wait for `RUNNING` on connector *and* task
5. Run the producer (`load-https-sessions` profile)
6. `docker compose exec trino trino` → `SELECT count(*) FROM iceberg.playground.https_sessions;`
