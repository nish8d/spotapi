# M0 — Kafka Only: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a single-broker Kafka in KRaft mode under Docker Compose with the `plays` topic created at 3 partitions, and verify by hand that a message produced to it can be consumed back.

**Architecture:** One `kafka` service (Apache Kafka 3.9.0, KRaft — broker and controller in the same process, no ZooKeeper) plus a one-shot `kafka-init` service that creates the topic and exits. Kafka advertises two listeners: `kafka:9092` for other containers on the Compose network (this is the address the spec's Flink DDL uses) and `localhost:29092` for processes on the host. Topic state lives in a named Docker volume so it survives `docker compose down`.

**Tech Stack:** Docker Compose, `apache/kafka:3.9.0`, the `kafka-topics.sh` / `kafka-console-producer.sh` / `kafka-console-consumer.sh` CLI tools bundled in that image.

**Spec:** `docs/superpowers/specs/2026-09-11-spotify-streaming-pipeline-design.md` (see "M0 — Kafka only", "Topic configuration", and the "Service versions" table)

## Global Constraints

- Kafka image is `apache/kafka:3.9.0`, KRaft mode, **single broker, no ZooKeeper**.
- Topic `plays`: **3 partitions**, **replication factor 1**, **retention 7 days** (`retention.ms=604800000`), **compression `snappy`**.
- Everything runs locally under Docker Compose. Budget is $0 — no cloud component.
- The in-network broker address is **`kafka:9092`** and must not change: the spec's Flink source DDL hardcodes `'properties.bootstrap.servers' = 'kafka:9092'`.
- **Host port 5432 is already in use on this machine** by an unrelated local Postgres. Do not publish any service on host port 5432; when Postgres joins the stack in M2 it must map to `55432:5432`. (Recorded here so it is not rediscovered later.)
- Host port bindings used by this milestone: **29092** only.
- Do not jump ahead. M0 ends at Kafka; no Python package, no Flink image build, no Postgres, no Grafana.
- Verified Flink 1.20 connector coordinates (fixed as of 2026-09-12, all confirmed downloadable from Maven Central):
  - `org.apache.flink:flink-sql-connector-kafka:3.4.0-1.20`
  - `org.apache.flink:flink-connector-jdbc:3.3.0-1.20`
  - `org.postgresql:postgresql:42.7.7`

---

## File Structure

| File | Responsibility |
|---|---|
| `docker-compose.yml` | Create: the whole stack. In M0 it holds exactly two services — `kafka` and the one-shot `kafka-init` — plus the `kafka-data` named volume. Later milestones add services to this same file. |
| `flink/jars.txt` | Create: the manifest of verified connector jar coordinates and their Maven Central URLs. M0 only *verifies* these; `flink/Dockerfile` reads this list in M4. |
| `.gitignore` | Modify: no change expected — Kafka state lives in a Docker named volume, not in `./data`. Confirm only. |

### Why a separate `kafka-init` service

The topic could be created by hand with `docker exec`, but then a fresh clone would have a broker and no topic. A one-shot service that depends on the broker being *healthy* makes topic creation part of `docker compose up`, and `--if-not-exists` makes it safe to re-run. It exits after it runs; a stopped `kafka-init` container is the expected steady state, not an error.

---

### Task 1: Kafka broker in KRaft mode

**Files:**
- Create: `docker-compose.yml`

**Interfaces:**
- Consumes: nothing (first task in the project).
- Produces: a Compose service named `kafka`, reachable at `kafka:9092` from other Compose services and `localhost:29092` from the host. A named volume `kafka-data`. Compose project name `spot`, so containers are `spot-kafka-1` etc. and `docker compose` commands work from the repo root with no `-p` flag.

- [ ] **Step 1: Write `docker-compose.yml` with the broker only**

```yaml
name: spot

services:
  kafka:
    image: apache/kafka:3.9.0
    hostname: kafka
    ports:
      # Host-side access. In-container clients use kafka:9092 instead.
      - "29092:29092"
    environment:
      # --- KRaft: this one process is both broker and controller ---
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      # Fixed cluster id: the data volume is formatted with it on first boot,
      # and a changed id makes the existing volume unreadable.
      CLUSTER_ID: 5L6g3nShT-eMCtK--X86sw

      # --- Listeners ---
      # INTERNAL  : other containers, advertised as kafka:9092
      # HOST      : processes on the laptop, advertised as localhost:29092
      # CONTROLLER: KRaft quorum traffic. Bound to the routable hostname, not
      #   0.0.0.0: a controller listener is advertised to the quorum but is not
      #   allowed in advertised.listeners, so Kafka advertises its `listeners`
      #   entry verbatim and rejects the nonroutable meta-address. Must match
      #   the host in KAFKA_CONTROLLER_QUORUM_VOTERS.
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:9092,HOST://0.0.0.0:29092,CONTROLLER://kafka:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,HOST:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL

      # --- Single-broker sizing: every internal topic must fit on one node ---
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      # No point waiting for other consumers to join a group on a laptop.
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

      KAFKA_LOG_DIRS: /var/lib/kafka/data
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"]
      interval: 10s
      timeout: 10s
      retries: 12
      start_period: 20s

volumes:
  kafka-data:
```

> **Gotcha, found the hard way during execution.** Binding the controller as
> `CONTROLLER://0.0.0.0:9093` makes the container exit 1 before the broker ever
> starts, with `advertised.listeners cannot use the nonroutable meta-address
> 0.0.0.0` — confusing, because `advertised.listeners` contains no such address.
> The broker listeners may bind `0.0.0.0` precisely because they have explicit
> advertised values; the controller listener has none (Kafka forbids controller
> listeners in `advertised.listeners`) so Kafka advertises its `listeners` entry
> as-is, and refuses a meta-address.

- [ ] **Step 2: Start it and wait for the healthcheck to go green**

Run:
```bash
docker compose up -d kafka
docker compose ps
```
Expected: one container listed, `STATUS` reading `Up ... (healthy)`. It may read `(health: starting)` for the first ~30 seconds — re-run `docker compose ps` until it settles. If it reads `(unhealthy)` or the container is restarting, read `docker compose logs kafka` before changing anything.

- [ ] **Step 3: Confirm the broker answers on both listeners**

Run:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092 | head -1
```
Expected: a line beginning with `kafka:9092 (id: 1 rack: null ...)` — the broker resolving its own advertised internal name.

Then, from the host:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:29092 | head -1
```
Expected: a line beginning with `localhost:29092 (id: 1 ...)`. This proves the second listener is live; the host-side address matters from M1 onward when tests run outside Docker.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "M0: single-broker Kafka in KRaft mode under Compose

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: The `plays` topic, and a produce/consume round-trip

**Files:**
- Modify: `docker-compose.yml` (add the `kafka-init` service after the `kafka` service, before the `volumes:` block)

**Interfaces:**
- Consumes: the `kafka` service from Task 1, at `kafka:9092`, gated on its healthcheck.
- Produces: topic `plays` — 3 partitions, replication factor 1, `retention.ms=604800000`, `compression.type=snappy`. Every later milestone reads or writes this topic; M1's producer keys messages by `listener_id` so that a listener's plays stay ordered on one partition.

- [ ] **Step 1: Add the one-shot topic-creation service**

```yaml
  # Runs once on `docker compose up`, creates the topic, exits 0.
  # A stopped kafka-init container is the expected steady state.
  kafka-init:
    image: apache/kafka:3.9.0
    depends_on:
      kafka:
        condition: service_healthy
    restart: "no"
    # The script must be ONE argument to `bash -c`. A string `command:` is
    # word-split by Compose, which would hand bash only the first word.
    entrypoint: ["/bin/bash", "-c"]
    command:
      - |
        set -e
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
          --create --if-not-exists \
          --topic plays \
          --partitions 3 \
          --replication-factor 1 \
          --config retention.ms=604800000 \
          --config compression.type=snappy
        echo "--- plays ---"
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic plays
```

> **Gotcha, found the hard way during execution.** Writing this as a plain
> `command: |` block makes the container start, print its whole environment, and
> exit 0 without creating anything. Compose word-splits a string `command`, so
> `bash -c` receives only `set` — which with no arguments prints every shell
> variable. The list form above keeps the script as a single argument. Silent
> success is the dangerous part: check for `Created topic plays.` in the output,
> not just the exit code.

- [ ] **Step 2: Run it and read its output**

Run:
```bash
docker compose up kafka-init
```
Expected: `Created topic plays.` followed by a `--- plays ---` banner and a describe block, then the container exits 0. Re-running the command must print `--- plays ---` and the describe block without error — `--if-not-exists` makes it idempotent.

- [ ] **Step 3: Verify the partition count (acceptance criterion)**

Run:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic plays
```
Expected: a header line containing `PartitionCount: 3` and `ReplicationFactor: 1`, with `retention.ms=604800000` and `compression.type=snappy` in the `Configs:` field, followed by exactly three `Partition: 0`, `Partition: 1`, `Partition: 2` lines, each with `Leader: 1`.

If `PartitionCount` is not 3, the topic was created earlier with different settings and `--if-not-exists` silently kept it. Delete and recreate:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic plays
docker compose up kafka-init
```

- [ ] **Step 4: Consume in one terminal**

Run (leave this running):
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic plays --from-beginning \
  --property print.key=true --property print.partition=true
```
Expected: it prints nothing and does not exit — a consumer with no new messages blocks, it does not finish.

- [ ] **Step 5: Produce in a second terminal (acceptance criterion)**

Run:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic plays \
  --property parse.key=true --property key.separator=:
```
At the `>` prompt type these three lines, pressing Enter after each, then Ctrl-D:
```
nishad:hello from M0
nishad:second message
someone-else:third message
```
Expected, in the consumer terminal: three lines appear within a second or so. The two `nishad` messages carry the **same** `Partition:` number as each other; `someone-else` may land on a different one. That is the partition-key guarantee the whole design rests on — Kafka hashes the key to choose a partition, and orders messages only *within* a partition, which is why the spec keys by `listener_id` rather than `track_id`.

Stop the consumer with Ctrl-C.

- [ ] **Step 6: Confirm the messages are durable, not just relayed**

Run:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 --topic plays
```
Expected: three lines `plays:0:N`, `plays:1:N`, `plays:2:N` whose offsets sum to 3. The consumer read from the log; it was not a live relay. This is what makes replay possible in M2.

- [ ] **Step 7: Commit**

```bash
git add docker-compose.yml
git commit -m "M0: create plays topic with 3 partitions via one-shot init service

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Verify the Flink 1.20 connector jar coordinates

**Files:**
- Create: `flink/jars.txt`

**Interfaces:**
- Consumes: nothing from Tasks 1–2. Independent of Kafka; it can be done in any order relative to them.
- Produces: `flink/jars.txt`, one Maven Central URL per line. M4's `flink/Dockerfile` copies these three jars into `/opt/flink/lib`; it reads the versions from here rather than restating them.

Connector versioning is independent of the Flink release — `flink-sql-connector-kafka` 3.4.0 and 5.0.0 both exist, and only the ones whose version suffix is `-1.20` are built against Flink 1.20. Getting this wrong surfaces in M4 as a `NoSuchMethodError` at job submission, a long way from its cause, which is why the spec pulls the check forward into M0.

- [ ] **Step 1: Write the manifest**

```bash
mkdir -p flink
cat > flink/jars.txt <<'EOF'
# Flink 1.20 connector jars, baked into the image by flink/Dockerfile (M4).
# Version suffix -1.20 is load-bearing: connectors are versioned independently
# of Flink, and a jar built against another minor release fails at job
# submission, not at image build.
# Verified against Maven Central 2026-09-12.
https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar
https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar
https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar
EOF
```

- [ ] **Step 2: Download all three and confirm they are real jars**

A `200` on a URL is not proof the artifact is usable — verify each downloads completely and is a valid zip archive with the expected class layout.

Run:
```bash
tmp=$(mktemp -d)
grep -v '^#' flink/jars.txt | grep . | while read -r url; do
  curl -fsSL -o "$tmp/$(basename "$url")" "$url" || echo "DOWNLOAD FAILED: $url"
done
ls -lh "$tmp"
for j in "$tmp"/*.jar; do
  if unzip -t "$j" > /dev/null 2>&1; then echo "OK   $(basename "$j")"; else echo "BAD  $(basename "$j")"; fi
done
```
Expected: three files — Kafka connector ~5.4 MB, JDBC connector ~430 KB, Postgres driver ~1.1 MB — and three `OK` lines. No `DOWNLOAD FAILED` and no `BAD`.

- [ ] **Step 3: Confirm each jar registers the factory Flink will look up**

Flink does not find a connector by class name. It reads the Java SPI file
`META-INF/services/org.apache.flink.table.factories.Factory` from every jar on
the classpath and matches `'connector' = '...'` against what those factories
declare. A jar containing the right class but missing the SPI entry is invisible
to Flink, so check the registration, not the class.

Run:
```bash
for j in "$tmp"/flink-sql-connector-kafka-*.jar "$tmp"/flink-connector-jdbc-*.jar; do
  echo "--- $(basename "$j") ---"
  unzip -p "$j" META-INF/services/org.apache.flink.table.factories.Factory | grep -v '^#' | grep .
done
unzip -p "$tmp"/postgresql-*.jar META-INF/services/java.sql.Driver
rm -rf "$tmp"
```
Expected:
```
--- flink-sql-connector-kafka-3.4.0-1.20.jar ---
org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory
org.apache.flink.streaming.connectors.kafka.table.UpsertKafkaDynamicTableFactory
--- flink-connector-jdbc-3.3.0-1.20.jar ---
org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactory
org.apache.flink.connector.jdbc.core.table.JdbcDynamicTableFactory
org.apache.flink.connector.jdbc.core.database.catalog.factory.JdbcCatalogFactory
org.postgresql.Driver
```
Empty output for any jar means the coordinate is wrong even though the download succeeded — re-check the version suffix against https://mvnrepository.com/artifact/org.apache.flink before continuing.

- [ ] **Step 4: Commit**

```bash
git add flink/jars.txt
git commit -m "M0: pin and verify Flink 1.20 connector jar coordinates

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Milestone acceptance

From the spec's M0 section — all three must hold before starting M1:

- [ ] A message typed into the console producer appears in the console consumer (Task 2, Steps 4–5).
- [ ] `kafka-topics --describe` shows 3 partitions (Task 2, Step 3).
- [ ] Connector jar coordinates for Flink 1.20 are verified (Task 3).

And one check the spec implies but does not state — the stack must come up from cold in one command:

- [ ] **Cold-start check:** `docker compose down -v && docker compose up -d && sleep 45 && docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic plays` prints a topic with `PartitionCount: 3`. Note `-v` destroys the `kafka-data` volume and with it the three test messages; that is intended here.
