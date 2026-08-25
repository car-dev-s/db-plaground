# Kafka Connect Tutorial (for this project)

Kafka Connect is a framework for moving data in/out of Kafka without hand-writing
producer/consumer code — you configure **connectors** (source or sink) declaratively via JSON/REST,
and Connect's runtime (the "worker") handles the plumbing: offset tracking, retries, scaling via
tasks, and serialization.

In this repo: `kafka-connect` (built from `kafka-connect/Dockerfile`) runs a single Connect worker
with the Apache Iceberg sink connector baked in, sinking `playground.https-sessions` →
`iceberg.playground.https_sessions`. REST API at **http://localhost:8083**.

## 1. Core concepts

- **Worker** — the JVM process running Connect itself (`kafka-connect` container here). A worker
  can run many connectors. This setup runs one worker, standalone-style config but actually running
  in **distributed mode** (the mode that scales — see below), just with a cluster size of one.
- **Connector** — a logical job (e.g. "sink `playground.https-sessions` into Iceberg"), configured
  via a JSON blob posted to the REST API. A connector doesn't move data itself — it spawns tasks.
- **Task** — the actual unit of parallel work. `tasks.max` in a connector's config caps how many
  tasks it can run; Connect assigns topic partitions to tasks. More tasks = more parallelism, up to
  the partition count of the source topic.
- **Converter** — turns bytes on the wire into a structured `SinkRecord`/`SourceRecord` (and back
  for sources). Configured independently for keys and values, at both the **worker level** (default
  for all connectors) and **connector level** (per-connector override, wins if set). This project
  hit exactly this: the worker default is `JsonConverter` for both key and value, but the HTTPS
  session producer keys messages as plain strings — so the connector config overrides
  `key.converter` to `StringConverter` (see `kafka-connect/README.md` gotchas).
- **Offsets** — Connect tracks per-task progress in its own internal Kafka topics
  (`_connect-offsets` for source connectors' external-system offsets; sink connectors instead rely
  on normal Kafka consumer group offsets, since they're just consuming a topic).
- **Internal topics** — `_connect-configs`, `_connect-offsets`, `_connect-status`. These store the
  worker's own state (not your data) and **must** be created with `cleanup.policy=compact`. If they
  get recreated with defaults (e.g. after a raw broker reset), Connect refuses to start.

## 2. Standalone vs. Distributed mode

| | Standalone | Distributed |
|---|---|---|
| Config storage | Local file | Kafka topics (`_connect-configs` etc.) |
| Fault tolerance | None — one process | Workers can die/join; connectors rebalance across survivors |
| Scaling | Single JVM only | Add workers to the same `group.id` to scale out |
| Typical use | Local dev, quick tests | Anything resembling production |

This project runs the **distributed-mode worker image** (`confluentinc/cp-kafka-connect`, driven
entirely via REST, using `CONNECT_GROUP_ID`) even though there's only one worker container — that's
still "distributed mode with a cluster size of one." This matters because it means the connector
config is **not** a local file — it's stored in the `_connect-configs` Kafka topic, which is why
`docker compose down` (which drops volumes) wipes out registered connectors, while restarting the
container alone does not.

## 3. The REST API — this is how you actually operate Connect

Everything is done via HTTP against port 8083. No CLI tool ships with core Kafka Connect for this —
`curl` (or any HTTP client) is the interface.

```bash
# what connector plugins are available (i.e. did the Dockerfile's plugin install actually work)
curl http://localhost:8083/connector-plugins

# list registered connectors
curl http://localhost:8083/connectors

# register a new connector
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @kafka-connect/iceberg-sink-connector.json

# check connector + task status (the one you'll run constantly while debugging)
curl http://localhost:8083/connectors/https-sessions-iceberg-sink/status

# get current config
curl http://localhost:8083/connectors/https-sessions-iceberg-sink/config

# update config in place — NOTE: flat config object, NOT the {"name":...,"config":{...}} envelope
curl -X PUT http://localhost:8083/connectors/https-sessions-iceberg-sink/config \
  -H 'Content-Type: application/json' \
  -d '{...flat key/value config...}'

# pause / resume without deleting (keeps offsets)
curl -X PUT http://localhost:8083/connectors/https-sessions-iceberg-sink/pause
curl -X PUT http://localhost:8083/connectors/https-sessions-iceberg-sink/resume

# restart just a failed task (cheaper than restarting the whole connector)
curl -X POST http://localhost:8083/connectors/https-sessions-iceberg-sink/tasks/0/restart

# delete entirely
curl -X DELETE http://localhost:8083/connectors/https-sessions-iceberg-sink
```

The `POST /connectors` vs `PUT /connectors/{name}/config` envelope mismatch is a real trap: `POST`
wants `{"name": ..., "config": {...}}`, `PUT` wants just the inner config map. Sending the wrong
shape to `PUT` gives a confusing HTTP 500 with a Jackson deserialization stack trace rather than a
clear "wrong shape" error — noted the hard way in this project (`DEPLOYMENT.md` section 6).

## 4. Status semantics — what `RUNNING` actually tells you (and doesn't)

`GET /connectors/{name}/status` returns a **connector** state and a list of **task** states,
independently:

```json
{
  "name": "https-sessions-iceberg-sink",
  "connector": { "state": "RUNNING", "worker_id": "kafka-connect:8083" },
  "tasks": [
    { "id": 0, "state": "RUNNING", "worker_id": "kafka-connect:8083" }
  ]
}
```

- `connector.state == RUNNING` only means the connector instance itself started successfully — it
  says **nothing** about whether its tasks are healthy.
- Always check `tasks[].state` too. A task can be `FAILED` while the connector stays `RUNNING`.
- **`RUNNING` does not mean "making progress."** This project hit exactly that: the Iceberg sink
  task stayed `RUNNING` while silently committing zero rows after a live config change left its
  internal coordinator/worker threads in an inconsistent state (`DEPLOYMENT.md`, "reconfiguring a
  live connector is fragile"). `RUNNING` + zero throughput requires checking connector-specific
  logs/metrics, not just the status endpoint.
- `FAILED` tasks include a `trace` field with the exception — read it, don't just restart blindly.
  A restart without understanding the failure just reproduces it (unless it was transient, like a
  broker not being ready yet).

## 5. Debugging workflow (senior-level checklist)

1. **Plugin not found when registering?** → `curl .../connector-plugins | grep -i <name>`. If
   missing, the jar isn't discovered — check it's under its own subdirectory of `plugin.path`
   (Connect's plugin isolation scanner ignores loose jars at the top level — see
   `kafka-connect/Dockerfile` comments). Check `docker compose logs kafka-connect` for plugin scan
   errors.
2. **Connector registers but task immediately fails?** → `status` endpoint's `tasks[].trace`. Common
   causes: wrong converter for the actual message format (`DataException`/`JsonParseException`),
   missing target-system connectivity (wrong hostname — remember containers use Docker network
   names like `kafka:29092`, not `localhost`), or missing prerequisite state (e.g. this project's
   Iceberg REST catalog doesn't auto-create namespaces — the connector fails until you `POST
   /v1/namespaces` first).
3. **Task RUNNING but topic has no consumer lag movement / sink has zero writes?** → check
   `consumer.override.auto.offset.reset`. Default `latest` means a brand-new sink task silently
   skips everything already on the topic — looks identical to "nothing is happening" from the
   outside.
4. **Task RUNNING, offsets moving, but downstream system still shows nothing?** → connector-specific
   batching/commit interval. This project's Iceberg sink batches writes and only commits a Iceberg
   snapshot every `iceberg.control.commit.interval-ms` (default 5 minutes, lowered to 10s locally).
   Not every sink surfaces "in-flight, uncommitted" state via the REST status endpoint — you often
   have to know the connector's own semantics.
5. **Reconfiguring a running connector behaves strangely?** → Some connectors (this Iceberg one
   included) keep internal state (coordinator/worker threads, control topics) that doesn't cleanly
   reconcile with a live `PUT` config change. If you see persistent no-op commits after a
   reconfigure, the reliable fix is delete → clean up downstream state (drop the partially-created
   table, purge internal control topics) → re-register fresh → **wait for `RUNNING` on connector and
   task** → only then start producing.
6. **Broker-level cascading failures** — if the Kafka broker itself gets reset while Connect isn't
   running, its internal topics can get recreated with the wrong `cleanup.policy`, and Connect
   refuses to start with a `ConfigException`. Don't patch this by hand for local/disposable setups —
   `docker compose down && docker compose up -d` for a clean slate is faster and less error-prone.

## 6. Things a senior dev should know going in

**Exactly-once vs. at-least-once is a per-connector contract, not a Connect-wide guarantee.**
Connect's framework gives you at-least-once by default. Exactly-once semantics (EOS) for sink
connectors require the connector itself to use a transactional producer/idempotent write pattern —
the Iceberg sink does this, which is *why* it needs the `__transaction_state` topic and why that
topic's replication factor becomes a real constraint on single-broker dev setups (see
`DEPLOYMENT.md` section 1).

**Connector config validation happens at `PUT`/`POST` time, but only for known fields.** Typos in
config keys are frequently accepted silently (unknown property, ignored) rather than rejected —
always verify with `GET .../config` after registering that what's live matches what you intended,
and check `status` for actual behavior, not just a 201/200 response from the register call.

**Scaling is topic-partition-bound.** `tasks.max` is a ceiling, not a guarantee — Connect can't
create more active tasks than there are partitions to assign. A `tasks.max: 4` connector on a
1-partition topic still runs effectively 1 task.

**Worker-level converter defaults are a trap for multi-connector clusters.** If you add a second
connector to this same worker with a different message format, don't assume it inherits sane
defaults — always set `key.converter`/`value.converter` explicitly per-connector when the format
isn't uniform across all connectors on the worker.

**REST API has no auth in this setup** — fine for local dev, not something to carry into a shared
or production cluster without adding `CONNECT_REST_EXTENSION_CLASSES` / a reverse proxy with auth.

**Single Message Transforms (SMTs)** are Connect's lightweight per-record transformation mechanism
(field rename, mask, route to a different topic, timestamp conversion) — configured inline in the
connector JSON via `transforms`. This project's connector uses a two-step chain (`Cast$Value` then
`TimestampConverter$Value`) to fix the `timestamp` field's inferred type — see the "`Instant` fields
infer as `double`" gotcha in `kafka-connect/README.md`. SMTs avoid needing a separate
stream-processing job for simple record shaping, but they're limited to what the built-in transforms
support: `TimestampConverter`'s `unix` format only accepts a `Long`, not a fractional `Double`, which
is why a `Cast$Value` step (with precision loss) comes first here. That precision loss is not
cosmetic — it leaves the Iceberg table with second-granularity timestamps while the other two stores
hold milliseconds; `docs/cross-store-consistency.md` §1 traces the consequence.

**This connector is the only exactly-once path in the project.** The Iceberg sink stages Parquet
files without registering them, reports files *and* consumed offsets over the `control-iceberg`
topic, and lets a coordinator commit both atomically into one Iceberg snapshot — a genuine
two-phase commit, which is why offsets live in the table metadata rather than only in
`_connect-offsets`. The `iceberg.control.commit.interval-ms: 10000` setting is the visible cost:
writes are invisible to queries for up to 10 seconds. Contrast with the Flink path, which has no
commit protocol at all — `docs/delivery-semantics.md` §7 puts the two side by side.
