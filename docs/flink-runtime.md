# How the Flink Job Actually Runs (for this project)

There is no Flink service in `docker-compose.yml`, no JobManager, no TaskManager, and no
`flink run` command anywhere in this repo. The `flink` module is a **Spring Boot application that
starts an entire Flink cluster inside its own JVM**. That is unusual enough to be worth explaining
properly, because almost every piece of Flink documentation assumes the opposite.

## 1. `getExecutionEnvironment()` is a context detector

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```

This single call has three completely different behaviours depending on how the JVM was started:

| How the code was launched | What you get | Where the work runs |
|---|---|---|
| `flink run job.jar` (CLI) | `StreamContextEnvironment` | a remote cluster; `execute()` submits the JobGraph and returns |
| inside a JobManager (application mode) | `StreamContextEnvironment` | that cluster |
| **plain `java -jar` / `bootRun` / IDE (this project)** | `LocalStreamEnvironment` | a `MiniCluster` started in this JVM |

Flink detects the CLI cases through a thread-local `ExecutionEnvironmentFactory` that the client
installs before invoking your `main`. When nothing installed one — which is the case here — it
falls back to local mode, spins up a `MiniCluster` (a real JobManager and TaskManager, just as
threads instead of processes), and runs the job in-process.

So the answer to "what shell does Flink run on" is: **the same shell you ran Gradle in.** The
JobManager, the TaskManager, the Kafka source, and both sink writers are all threads inside the
`bootRun` process. Kill that terminal and the cluster is gone.

Everything the job talks to *is* containerized, which is why `flink/src/main/resources/application.yml`
uses host-side addresses (`localhost:9092`, `localhost:9042`, `mongodb://localhost:27017`) rather
than the compose service names (`kafka:29092`) that Trino and Kafka Connect use. That difference is
the clearest evidence of which side of the network boundary each component lives on.

## 2. `CommandLineRunner` + `env.execute()` — a blocking Spring bean

```java
@Component
public class HttpsSessionFlinkJob implements CommandLineRunner {
    public void run(String... args) throws Exception {
        // ... build the job graph ...
        env.execute("https-sessions-flink-sink");
    }
}
```

`env.execute()` on a `LocalStreamEnvironment` **blocks until the job finishes**. Because the Kafka
source is unbounded, it never finishes. Consequences worth internalising:

- `SpringApplication.run(...)` does not return until you kill the process. Any `CommandLineRunner`
  ordered after this one would never run.
- Spring's lifecycle and Flink's lifecycle are not integrated. Flink is not shutting down through
  `@PreDestroy`; a Ctrl-C tears down the JVM and the MiniCluster with it.
- Spring is doing exactly one useful thing here: binding `application.yml` into
  `HttpsSessionFlinkProperties` (a `record`, via `@ConfigurationPropertiesScan`) and injecting it.
  That is a legitimate, modest use — typed, validated, profile-aware config for a Flink job is
  genuinely nicer than `ParameterTool`.

If you wanted the job to run without blocking the context, `env.executeAsync()` returns a
`JobClient` you can cancel — the right shape if this app ever needed to expose an HTTP endpoint or
run more than one job.

## 3. Parallelism, and why extra parallelism does nothing here

Nothing calls `env.setParallelism(...)`, so the default applies: in local mode, **one slot per
available CPU core**. On an 8-core machine you get 8 subtasks per operator.

That sounds like free throughput. It is not, because:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic playground.https-sessions
# PartitionCount: 1
```

**Kafka partition count is the hard ceiling on source parallelism.** One partition means Flink's
`SplitEnumerator` has exactly one split to hand out; one source subtask reads, the other seven sit
idle forever. Since source and sink are chained into a single operator chain (they are connected
by a `forward` shipping strategy with equal parallelism), only one chain does any work.

The parallelism does still cost you something concrete, because `createWriter` is called per
subtask:

```java
CassandraSinkWriter(...) {
    session = CqlSession.builder()...build();   // one CQL session per parallel writer
}
MongoSinkWriter(...) {
    client = MongoClients.create(connectionUri); // one Mongo client per parallel writer
}
```

Each of those opens its own connection pool. On an 8-core machine you get 8 `CqlSession`s and 8
`MongoClient`s, seven of which will never write a record. Being explicit costs one line and removes
the surprise:

```java
env.setParallelism(1);   // matches the topic's partition count
```

To actually scale, add partitions to the topic first — and note that `HttpsSessionProducer` keys
records by `sourceIp + ":" + sourcePort`, so partitioning is already IP-affine and per-partition
ordering per source IP would be preserved.

## 4. Everything in the job graph gets serialized — this is why `transient` appears

When you write `sessions.sinkTo(new CassandraHttpsSessionSink(...))`, Flink does not call that
object. It **serializes** it into the JobGraph and ships it to whichever TaskManager will run the
operator — which is why `Sink` extends `Serializable`, and why the distinction between "field on
the Sink" and "field on the SinkWriter" is a real architectural constraint rather than style.

Look at how `CassandraHttpsSessionSink` is split:

```java
public class CassandraHttpsSessionSink implements Sink<HttpsSessionEvent> {
    private final String contactPoint;      // Serializable config — travels in the JobGraph
    private final int port;

    public SinkWriter<HttpsSessionEvent> createWriter(WriterInitContext context) {
        return new CassandraSinkWriter(...); // CqlSession is created HERE, on the task
    }
}
```

A `CqlSession` or `MongoClient` is not serializable and could not be shipped even if it were —
a live socket is meaningless on another machine. **The `Sink` carries the recipe; the `SinkWriter`
holds the connection.** That split is the whole point of the factory-shaped API, and getting it
wrong is the most common `NotSerializableException` in Flink.

The same constraint produces the idiom in `HttpsSessionDeserializationSchema`:

```java
private transient ObjectMapper objectMapper;   // Jackson's ObjectMapper is not serializable

private ObjectMapper mapper() {
    if (objectMapper == null) {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }
    return objectMapper;
}
```

`transient` keeps it out of Java serialization; lazy initialisation rebuilds it on the task after
deserialization, where the field arrives as `null`. This pairing — `transient` field plus lazy
getter — is the standard Flink pattern for any non-serializable helper. (The alternative is
`RichFunction#open()`, which gives you an explicit initialisation hook; `KafkaRecordDeserializationSchema`
has an `open()` method too, which would be the slightly more idiomatic home for this.)

`JavaTimeModule` is registered explicitly because plain Jackson has no idea what an `Instant` is —
without it, deserializing `timestamp` throws `InvalidDefinitionException`. That module lives in
`jackson-datatype-jsr310`, which is why `flink/build.gradle` declares it directly.

## 5. Dependency archaeology: the two build workarounds

### `flink-connector-base`

`libs.versions.toml` declares `flink-connector-base` and `flink/build.gradle` depends on it. It is
not obviously needed — until the job fails at startup with `NoClassDefFoundError` on
`org.apache.flink.connector.base.source.reader.RecordEmitter`.

`flink-connector-kafka` is released on its own cadence (note the version: `5.0.0-2.2`, meaning
connector 5.0.0 built for Flink 2.2) and treats the shared connector infrastructure as a
`provided`-style dependency it expects the *cluster* to have on its classpath. In a real Flink
cluster that assumption holds — `flink-connector-base` ships in `/opt/flink/lib`. Running embedded,
there is no cluster distribution, so you have to supply it yourself.

This is the general shape of "runs on the cluster, fails locally" problems: Flink's distribution
provides a set of jars that your build does not.

### The `org.lz4:lz4-java` capability conflict

```groovy
configurations.all {
    resolutionStrategy.capabilitiesResolution.withCapability("org.lz4:lz4-java") {
        selectHighestVersion()
    }
}
```

`./gradlew :flink:dependencyInsight --configuration runtimeClasspath --dependency lz4` shows what
this is resolving:

```
at.yawk.lz4:lz4-java:1.10.1
  \--- org.apache.kafka:kafka-clients:4.2.1

org.lz4:lz4-java:1.8.0 -> at.yawk.lz4:lz4-java:1.10.1
  \--- org.apache.flink:flink-runtime:2.2.0
```

Two **different modules**, same Java packages: `org.lz4:lz4-java` is the original (now unmaintained)
library, and `at.yawk.lz4:lz4-java` is the maintained fork Kafka 4.x switched to. Flink 2.2's
runtime still pulls the original.

The important distinction: this is a **capability conflict, not a version conflict**. Gradle resolves
version conflicts silently by picking the newest. It refuses to resolve capability conflicts, because
two different coordinates providing the same classes is genuinely ambiguous — putting both on the
classpath gives you whichever `LZ4Factory` the classloader happens to find first, and Kafka's
compression code would break in a way that only shows up under load.

Gradle only knows the two are equivalent because the fork *declares* `org.lz4:lz4-java` as a
capability in its Gradle Module Metadata. That declaration is what turns a silent classpath clash
into a build failure you must acknowledge — a good outcome, and a genuine advantage of Gradle's
metadata over Maven's POM model, which has no way to express it.

`selectHighestVersion()` picks the fork. `select("at.yawk.lz4:lz4-java:1.10.1")` would be more
explicit about the intent; `selectHighestVersion()` keeps working across upgrades.

## 6. What changes on a real cluster

Worth knowing what this job is *not* ready for, since the gap is instructive:

| Concern | Embedded (today) | Real cluster |
|---|---|---|
| Entry point | Spring Boot `main` → `CommandLineRunner` | `flink run -c <class> job.jar` |
| Jar layout | Spring Boot fat jar (`BOOT-INF/`, custom loader) | flat shaded jar; **Flink cannot read a Boot fat jar** |
| Addresses | `localhost:9092`, `localhost:9042` | compose/K8s service names |
| Fault tolerance | none (`docs/delivery-semantics.md` §2) | `enableCheckpointing` + a durable state backend |
| Parallelism | cores of one machine | slots across TaskManagers |
| Lifecycle | Ctrl-C | `flink cancel` / savepoints |

The jar-layout row is the one that surprises people. Spring Boot's repackaging nests dependencies
under `BOOT-INF/lib` and relies on its own classloader; Flink's client expects an ordinary jar with
a flat class tree. Submitting a Boot fat jar to `flink run` fails to find your job class. Shipping
this to a cluster means either a Shadow-plugin jar without Boot repackaging, or dropping Spring from
the deployment path entirely and keeping it only for local development.

Which raises the fair question: is Spring earning its place here? For config binding into a
`record`, at this size — yes, marginally. If this module ever needs to run on a real cluster, that
calculus inverts, and `ParameterTool` plus a plain `main` becomes the simpler answer.

## 7. Things a senior dev should know going in

**Local mode is a real cluster, not a mock.** The MiniCluster runs the same scheduler, the same
checkpoint coordinator, the same network stack (over local channels). Behaviour you observe locally
is generally representative — with the large exception of anything involving process failure, since
there are no processes to fail.

**`env.execute()` is where the job is built and shipped, not where it is defined.** Every
`sinkTo` / `fromSource` call before it only appends to a graph. Exceptions thrown inside
`SinkWriter#write` therefore surface at runtime, long after the line that "created" the sink.

**Read the connector version suffix.** `5.0.0-2.2` is connector 5.0.0 *for Flink 2.2*. Mismatching
that suffix produces `NoSuchMethodError` at runtime rather than a resolution failure at build time,
which is a considerably worse way to find out.

**The `flink` and `kafka` modules are two independent consumers of one topic**, in different
consumer groups, with different offset semantics. Neither affects the other's position. That is by
design and it is what makes the fan-out architecture in `README.md` work at all.
