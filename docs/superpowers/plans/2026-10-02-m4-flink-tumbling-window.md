# M4 — Flink Replaces the Consumer: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Flink 1.20 session cluster reads the `plays` topic and runs two SQL jobs — a raw passthrough into `raw_plays` and a one-minute tumbling window into `agg_plays_per_minute` — after which the throwaway M2 consumer is deleted and the dashboard's plays-per-minute panel reads Flink's output.

**Architecture:** Three new Compose services share one custom image (`flink/Dockerfile`, connector jars baked in from `flink/jars.txt`): a `jobmanager`, a `taskmanager`, and a one-shot `flink-submit` that submits every file in `flink/sql/jobs/` that is not already running. Every job session starts from `flink/sql/init.sql`, which pins the session time zone and declares the Kafka source and the JDBC sinks once. The Flink SQL is verified by an integration test that runs the simulator's fixture through an isolated Compose project (`spot-it`) and asserts exact rows in Postgres.

**Tech Stack:** `flink:1.20-java17` (resolves to 1.20.5), `flink-sql-connector-kafka 3.4.0-1.20`, `flink-connector-jdbc 3.3.0-1.20`, `postgresql 42.7.7`, Docker Compose v5.5.1, `pytest` 8.3.4, `psycopg[binary]` 3.2.9 (integration test only).

**Spec:** `docs/superpowers/specs/2026-09-11-spotify-streaming-pipeline-design.md` (see "Flink SQL", "Checkpointing", "Postgres schema", "Testing", "Error handling", and "M4 — Flink replaces the consumer: tumbling window")

## Global Constraints

- **Partition key is `listener_id`.** Nothing in M4 produces with any other key, and nothing re-partitions the topic.
- **`event_id` is deterministic** and is the primary key of `raw_plays`. Never derive a new id in Flink.
- **Every sink table has a primary key and writes in upsert mode.** Each Flink sink DDL declares `PRIMARY KEY (...) NOT ENFORCED` matching the Postgres primary key exactly.
- **`started_at` is the only event-time column.** The watermark is `started_at - INTERVAL '30' SECOND`. `observed_at` is never used for windowing.
- Flink image is `flink:1.20-java17`. Connector jars are the three URLs in `flink/jars.txt`, downloaded at **image build** time into `/opt/flink/lib`, never at job submission.
- In-network addresses: Kafka `kafka:9092`, Postgres `postgres:5432` (user, password and database all `spot`), Flink REST `jobmanager:8081`. Host ports: Flink UI `8081`, Postgres `55432` (host 5432 is taken).
- Memory: `jobmanager.memory.process.size: 1024m`, `taskmanager.memory.process.size: 1536m`. The whole stack must stay inside the spec's ~4.5 GB.
- Checkpoints every 30 s to a named volume; fixed-delay restart strategy.
- **The default suite stays offline.** `.venv/bin/pytest -q` contacts no broker, database or API. The integration test is excluded unless `-m integration` is passed.
- **The integration test never touches the dev stack.** It runs as Compose project `spot-it`, with its own network and volumes.
- **Do not jump ahead.** No hopping window (M5), no session window (M6), no lag panel (M7). `agg_top_artists` and `agg_sessions` stay empty.
- `consumer/` is deleted in Task 6 and not before — only once Flink is proven to write `raw_plays` on its own.
- Commit messages carry no `Co-Authored-By` trailer.

### Decisions this plan makes that the spec leaves open — or gets wrong

1. **The tumbling window counts `COUNT(DISTINCT event_id)`, not the spec's `COUNT(*)`.** This corrects the spec. Invariants 2 and 3 make a duplicate harmless *only at a sink keyed by `event_id`*: `raw_plays` absorbs a poller re-emission because both writes land on one primary key. `agg_plays_per_minute` is keyed by `(window_start, listener_id)`, and `COUNT(*)` counts every Kafka *message* — so the M3 restart exercise, which put the same play on the topic twice, would make that minute read 2 where `raw_plays` has 1 row. Counting distinct ids is exact, not approximate: two messages share an `event_id` only if their `started_at` floors to the same 5-second bucket, 5 divides 60, so they always fall in the same one-minute window. Task 5 demonstrates the bug before fixing it, and updates the spec.
2. **One Flink job per statement, one file per job.** `flink/sql/jobs/<name>.sql` sets `pipeline.name` to `<name>`. Each milestone then *adds* a job rather than editing and restarting a shared one, and the Web UI shows each job's graph and watermark separately. The cost — each job reads the topic independently — is irrelevant at this volume.
3. **`flink-submit` is idempotent.** It runs on every `docker compose up`, lists the cluster's jobs, and submits only those whose name is missing. Without the check every `up` would add a second copy of every job, and two copies of a passthrough double-write every row.
4. **Each job sets its own Kafka `group.id`** (`flink-<job name>`) with an `OPTIONS` hint. Flink does not use consumer groups to *read* — it assigns partitions itself and keeps offsets in its checkpoints — but it *commits* offsets to the group so tools like `kafka-consumer-groups` can see progress. Two jobs sharing a group would overwrite each other's commits and make the lag reading meaningless. A hint is not allowed inside `TABLE(...)` — verified: `ParseException: Encountered "/*+"` — so the windowed job applies it through a temporary view, which keeps `started_at`'s time attribute (verified with `EXPLAIN`).
5. **`table.exec.source.idle-timeout = 30 s`.** A job's watermark is the *minimum* across its input partitions, and a partition that receives nothing holds it back forever. That happens in the fixture test (two listeners cannot cover three partitions) and in real life whenever the simulator is stopped and only the poller is writing. Marking a partition idle after 30 s of silence lets the others advance. It is safe here because every producer writes near real time, so a partition that wakes up again does so with events at about the current watermark, not behind it.
6. **The session time zone is pinned to UTC** (`table.local-time-zone`). Tumbling windows over a `TIMESTAMP_LTZ` column are aligned in the session time zone and emit `window_start`/`window_end` as zone-less `TIMESTAMP(3)`. The image's JVM default is already UTC (verified), but a default is not a decision. The integration test asserts exact instants, so an offset would fail it.
7. **`raw_plays` keeps `started_at`/`observed_at` as `TIMESTAMP_LTZ(3)` end to end**, as the spec's "unchanged" passthrough says. The JDBC Postgres dialect accepts that type (verified with `EXPLAIN` against the real jar). The JSON format parses the schema's `2026-01-01T00:00:00Z`, with no fractional seconds, into it (verified: `2026-01-01 00:00:00.000`).
8. **Every (re)submission starts from `earliest-offset`, with no savepoints.** A resubmitted job re-reads the whole topic (7-day retention, a few tens of thousands of events) and recomputes every window, and upsert makes the rewrite idempotent. Resuming from committed group offsets instead would be faster and *wrong*: a window that was half-full when the old job stopped would be rebuilt from only its second half, and the upsert would overwrite the correct count with the smaller one. Task 8 exercises the replay.
9. **The fixture stream gains two messages after its twelve plays:** a re-emission of the first play (exactly what a poller restart mid-track produces), and a *flush* event ten minutes later. A window is written only once the watermark passes its end, and the watermark trails the latest `started_at` by 30 s, so without a later event the fixture's last minute would never close. The flush event's own window never closes either, which is why it never appears in `agg_plays_per_minute` — and the test asserts that.
10. **The integration test runs as Compose project `spot-it`** using `docker-compose.it.yml`, an overlay that removes the host ports that would collide with the dev stack (`!reset`) and maps Postgres to `55433` and the Flink UI to `18081` (`!override`). A project has its own network and volumes, so the test starts from an empty topic without touching dev data, and `down -v` at the end destroys only its own. It passes dummy Spotify variables, because Compose interpolates the poller's `${SPOTIFY_CLIENT_ID:?}` guard even though the poller is never started.
11. **JSON parsing stays strict** — no `json.ignore-parse-errors`. A message that is not a valid event fails the job, the restart strategy retries it 10 times 10 s apart, and then the job shows FAILED in the Web UI. A poison message should be loud. The topic only ever receives `PlayEvent.to_json()` output.
12. **Parallelism 1, four task slots.** One source subtask reads all three partitions and tracks a watermark per partition. Four slots fit M4's two jobs plus an ad-hoc SQL client query, and leave room for M5 and M6.
13. **The plays-per-minute panel reads `agg_plays_per_minute`.** That table has no `is_synthetic` column, so the Source filter becomes a subquery on the listener ids each producer uses. Each listener id belongs to exactly one producer.

---

## File Structure

| File | Responsibility |
|---|---|
| `flink/Dockerfile` | Create: `flink:1.20-java17` plus the three jars from `jars.txt`, a checkpoint directory owned by `flink`, and the submit script. |
| `flink/submit.sh` | Create: submits each job in `/opt/flink/sql/jobs/` unless a job of that name is on the cluster. |
| `flink/sql/init.sql` | Create: session settings, the `plays` source, and every sink. Run before each job file with `sql-client.sh -i`. |
| `flink/sql/jobs/raw-passthrough.sql` | Create: `plays` → `raw_plays`. |
| `flink/sql/jobs/plays-per-minute.sql` | Create: one-minute tumbling window → `agg_plays_per_minute`. |
| `docker-compose.yml` | Modify: add `jobmanager`, `taskmanager`, `flink-submit` and the `flink-checkpoints` volume; remove `consumer`. |
| `docker-compose.it.yml` | Create: the integration overlay — ports only. |
| `simulator/main.py` | Modify: `fixture_flush_event()`, `fixture_stream()`; `run_fixture` sends the stream. |
| `tests/test_simulator.py` | Modify: four tests for the stream. |
| `tests/integration/test_flink_windows.py` | Create: fixture → Kafka → Flink → Postgres, with exact rows asserted. |
| `pytest.ini` | Modify: register the `integration` marker and exclude it by default. |
| `consumer/`, `tests/test_consumer.py`, `tests/test_pg_sink.py` | Delete (Task 6). |
| `Dockerfile` | Modify: drop `COPY consumer/`. |
| `requirements.txt`, `requirements-dev.txt` | Modify: `psycopg` moves to dev — only the integration test uses it now. |
| `events/schema.py` | Modify: the docstring stops naming the consumer. |
| `grafana/dashboards/spot.json` | Modify: panel 1 reads `agg_plays_per_minute`. |
| `docs/superpowers/specs/2026-09-11-spotify-streaming-pipeline-design.md` | Modify: tumbling SQL counts distinct `event_id`, with the reason. |
| `CLAUDE.md` | Modify: current state. |

### Why the SQL is split into an init file and job files

Flink SQL client sessions are separate: a table declared in one session does
not exist in the next. Every job needs the same source and sinks, so they live
once in `init.sql` and the client runs it before each job file (`-i init.sql
-f job.sql`). The tables are declared in Flink's in-memory catalog and point at
Kafka and Postgres — declaring them creates nothing in either. Only an
`INSERT` starts a job.

---

### Task 1: A Flink cluster with the connectors baked in

**Files:**
- Create: `flink/Dockerfile`
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: `flink/jars.txt` from M0.
- Produces: image `spot-flink:1.20`; services `jobmanager` (healthy when REST answers) and `taskmanager`; volume `flink-checkpoints` mounted at `/opt/flink/checkpoints` in both; the YAML anchor `x-flink` that Task 3's `flink-submit` reuses.

A Flink cluster is two kinds of process. The **JobManager** accepts jobs,
turns SQL into a dataflow graph, schedules it, and coordinates checkpoints. The
**TaskManager** does the work, in *slots* — one slot runs one parallel copy of
a job's pipeline. Nothing here runs a job yet.

- [ ] **Step 1: Write the image**

```bash
cat > flink/Dockerfile <<'EOF'
# Flink plus the three connector jars, fetched when the image is BUILT. A job
# that needs the network at submission time fails somewhere far from its
# cause; a missing jar here fails the build instead. Versions come from
# jars.txt, verified against the Flink 1.20 suffix in M0.
FROM flink:1.20-java17

COPY jars.txt /tmp/jars.txt
RUN grep -v '^#' /tmp/jars.txt | grep . | xargs -n1 wget -q -P /opt/flink/lib/ \
 && rm /tmp/jars.txt \
 # The checkpoint volume is mounted here. A fresh named volume copies the
 # ownership of the directory it is mounted over, and Flink runs as `flink`.
 && mkdir -p /opt/flink/checkpoints \
 && chown flink:flink /opt/flink/checkpoints
EOF
```

- [ ] **Step 2: Add the cluster to Compose**

Insert this block between `name: spot` and `services:`. Compose ignores top-level keys that begin with `x-`, and `&flink` names the block so each Flink service can merge it in with `<<: *flink`:

```yaml
# Shared by every Flink service: one image, one configuration. The image's
# entrypoint writes FLINK_PROPERTIES into Flink's config before running any
# command, including the submit script's.
x-flink: &flink
  build: ./flink
  image: spot-flink:1.20
  environment:
    FLINK_PROPERTIES: |
      jobmanager.rpc.address: jobmanager
      rest.address: jobmanager
      rest.port: 8081
      jobmanager.memory.process.size: 1024m
      taskmanager.memory.process.size: 1536m
      # Four slots: M4's two jobs, one ad-hoc SQL client query, and room for
      # M5 and M6. Parallelism 1 means one slot per job.
      taskmanager.numberOfTaskSlots: 4
      parallelism.default: 1
      # A checkpoint is a consistent snapshot of every operator's state and
      # every source's offsets. On a failure the job rewinds to the last one.
      execution.checkpointing.interval: 30s
      state.checkpoints.dir: file:///opt/flink/checkpoints
      # Ten retries, ten seconds apart, then FAILED in the UI. Finite on
      # purpose: a poison message should end up visible, not retried forever.
      restart-strategy.type: fixed-delay
      restart-strategy.fixed-delay.attempts: 10
      restart-strategy.fixed-delay.delay: 10s

```

Then add these two services after `poller` and before `consumer`:

```yaml
  jobmanager:
    <<: *flink
    command: jobmanager
    ports:
      # The Flink Web UI: jobs, their graphs, per-operator watermarks,
      # checkpoints, backpressure.
      - "8081:8081"
    volumes:
      # The JobManager writes each checkpoint's metadata and the TaskManager
      # writes its state, so both must see the same directory.
      - flink-checkpoints:/opt/flink/checkpoints
    healthcheck:
      test: ["CMD-SHELL", "curl -fs http://localhost:8081/overview > /dev/null"]
      interval: 5s
      timeout: 5s
      retries: 24
      start_period: 10s
    restart: unless-stopped

  taskmanager:
    <<: *flink
    command: taskmanager
    depends_on:
      jobmanager:
        condition: service_healthy
    volumes:
      - flink-checkpoints:/opt/flink/checkpoints
    restart: unless-stopped
```

And add the volume at the bottom:

```yaml
volumes:
  kafka-data:
  postgres-data:
  flink-checkpoints:
```

- [ ] **Step 3: Validate, build, start**

```bash
docker compose config --quiet && echo "compose config valid"
docker compose up -d --build jobmanager taskmanager
docker compose ps jobmanager taskmanager
```

Expected: `compose config valid`, both services `Up`, the JobManager `(healthy)`.

- [ ] **Step 4: Check the jars, the config and the slots**

```bash
docker compose exec -T jobmanager ls /opt/flink/lib | grep -E 'kafka|jdbc|postgres'
docker compose exec -T jobmanager grep -E 'numberOfTaskSlots|checkpointing.interval|restart-strategy.type' /opt/flink/conf/config.yaml
curl -s http://localhost:8081/overview | python3 -m json.tool
```

Expected: the three jar file names from `jars.txt`; the three config lines;
and `"taskmanagers": 1`, `"slots-total": 4`, `"slots-available": 4`,
`"jobs-running": 0`. Open <http://localhost:8081> — an empty cluster with one
TaskManager.

- [ ] **Step 5: Commit**

```bash
git add flink/Dockerfile docker-compose.yml
git commit -m "M4: a Flink 1.20 session cluster with the connector jars baked in"
```

---

### Task 2: The source table, explored before any job exists

**Files:**
- Create: `flink/sql/init.sql`
- Modify: `docker-compose.yml` (one mount on `jobmanager`)

**Interfaces:**
- Consumes: the running cluster from Task 1; the topic `plays`.
- Produces: `flink/sql/init.sql`, declaring table `plays` — the spec's DDL, unchanged — after two session `SET`s. Tasks 3 and 5 append sinks to this file. Inside containers it is `/opt/flink/sql/init.sql`.

- [ ] **Step 1: Write the init file**

```bash
mkdir -p flink/sql/jobs
cat > flink/sql/init.sql <<'EOF'
-- Run before every job file: `sql-client.sh -i init.sql -f jobs/<job>.sql`.
-- A SQL client session forgets its tables when it ends, so every job declares
-- the same tables here. Declaring a table creates nothing in Kafka or
-- Postgres; it only tells Flink where they are and what is in them.

-- Tumbling windows over a TIMESTAMP_LTZ column are aligned in the session time
-- zone. The image's default is already UTC; this makes it a decision.
SET 'table.local-time-zone' = 'UTC';

-- A job's watermark is the MINIMUM over its input partitions, so one silent
-- partition holds every window open forever. After 30s of silence a partition
-- is marked idle and stops counting. Safe here because every producer writes
-- in near real time: a partition that wakes up does not wake up in the past.
SET 'table.exec.source.idle-timeout' = '30 s';

-- The spec's source DDL, unchanged.
--
-- WATERMARK declares started_at the event-time column and says how far behind
-- the latest started_at seen Flink should assume it might still be: 30s. A
-- window ending at T is final once the watermark passes T; an event arriving
-- after that is late, and dropped.
CREATE TABLE plays (
  event_id     STRING,
  listener_id  STRING,
  is_synthetic BOOLEAN,
  track_id     STRING,
  track_name   STRING,
  artist_name  STRING,
  album_name   STRING,
  duration_ms  INT,
  started_at   TIMESTAMP_LTZ(3),
  observed_at  TIMESTAMP_LTZ(3),
  WATERMARK FOR started_at AS started_at - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'plays',
  'properties.bootstrap.servers' = 'kafka:9092',
  -- Each job overrides this with its own group id; see the job files.
  'properties.group.id' = 'flink-plays',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);
EOF
```

- [ ] **Step 2: Mount the SQL into the JobManager**

Add to the `jobmanager` service's `volumes:`, after the checkpoint line:

```yaml
      # For ad-hoc queries with the SQL client. Read-only: jobs come from git.
      - ./flink/sql:/opt/flink/sql:ro
```

The directory exists now, which matters: a bind mount of a missing host path
makes Docker create it, owned by root.

```bash
docker compose up -d jobmanager
docker compose exec -T jobmanager ls /opt/flink/sql
```

Expected: `init.sql  jobs`.

- [ ] **Step 3: Read the stream, with the watermark beside it**

`CURRENT_WATERMARK(started_at)` returns the watermark at the moment each row
passes through. A bounded `LIMIT` lets a query on an endless stream finish.

```bash
docker compose exec -T jobmanager bash -c 'cat > /tmp/explore.sql && bin/sql-client.sh -i sql/init.sql -f /tmp/explore.sql' <<'EOF'
SET 'sql-client.execution.result-mode' = 'tableau';
SELECT listener_id, is_synthetic, started_at, observed_at,
       CURRENT_WATERMARK(started_at) AS watermark
FROM plays
LIMIT 15;
EOF
```

Expected: 15 rows. `started_at` and `observed_at` are parsed timestamps, not
NULL — if they are NULL, the ISO-8601 format option is wrong. `watermark` is
NULL in the first rows, because the source emits a watermark periodically
(every 200 ms) rather than per row, and the first rows arrive before the first
one. Where it is set, it is 30 s behind the largest `started_at` read so far.
While it runs, the query appears as a job at <http://localhost:8081>, and it
finishes when the limit is reached.

- [ ] **Step 4: Commit**

```bash
git add flink/sql/init.sql docker-compose.yml
git commit -m "M4: the plays source table, explored with CURRENT_WATERMARK"
```

---

### Task 3: The raw passthrough, and Flink takes over `raw_plays`

**Files:**
- Create: `flink/submit.sh`, `flink/sql/jobs/raw-passthrough.sql`
- Modify: `flink/sql/init.sql`, `flink/Dockerfile`, `docker-compose.yml`

**Interfaces:**
- Consumes: `plays` from Task 2; Postgres `raw_plays` from `postgres/init.sql`.
- Produces: Flink table `raw_plays`, declared in `init.sql`; job `raw-passthrough`; script `/opt/flink/bin/spot-submit.sh` in the image; one-shot service `flink-submit`. Contract for every job file: `flink/sql/jobs/<name>.sql` begins with `SET 'pipeline.name' = '<name>';`. The submit script matches on that name.

- [ ] **Step 1: Declare the sink**

```bash
cat >> flink/sql/init.sql <<'EOF'

-- Sinks. Each PRIMARY KEY matches its Postgres table's exactly, and that is
-- what puts the JDBC connector in upsert mode: it writes
-- INSERT ... ON CONFLICT (key) DO UPDATE. NOT ENFORCED because Flink does not
-- check uniqueness itself -- Postgres does.

CREATE TABLE raw_plays (
  event_id     STRING,
  listener_id  STRING,
  is_synthetic BOOLEAN,
  track_id     STRING,
  track_name   STRING,
  artist_name  STRING,
  album_name   STRING,
  duration_ms  INT,
  started_at   TIMESTAMP_LTZ(3),
  observed_at  TIMESTAMP_LTZ(3),
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/spot',
  'table-name' = 'raw_plays',
  'username' = 'spot',
  'password' = 'spot'
);
EOF
```

- [ ] **Step 2: Write the job**

```bash
cat > flink/sql/jobs/raw-passthrough.sql <<'EOF'
-- Every event into raw_plays, unchanged: the job the M2 consumer did. An
-- upsert on event_id, so replaying the topic rewrites rows with the values
-- they already have.
SET 'pipeline.name' = 'raw-passthrough';

-- The hint gives this job its own Kafka group. Flink does not read through
-- the group -- it assigns partitions itself and keeps offsets in checkpoints
-- -- but it commits offsets there on each checkpoint, so
-- kafka-consumer-groups can show this job's progress apart from others'.
INSERT INTO raw_plays
SELECT event_id, listener_id, is_synthetic, track_id, track_name,
       artist_name, album_name, duration_ms, started_at, observed_at
FROM plays /*+ OPTIONS('properties.group.id' = 'flink-raw-passthrough') */;
EOF
```

- [ ] **Step 3: Write the submit script**

```bash
cat > flink/submit.sh <<'EOF'
#!/usr/bin/env bash
# Submits each job in /opt/flink/sql/jobs/ unless a job with that name is
# already on the cluster. Runs as a one-shot Compose service on every `up`, so
# it must be safe to run twice: without the check, each `up` would add another
# copy of every job, and two passthroughs double-write every row.
#
# A job's name is its file name; each job file sets pipeline.name to match.
set -euo pipefail
cd /opt/flink

# Fails the script, and so the service, if the JobManager is unreachable.
listed=$(bin/flink list -r)

for job in sql/jobs/*.sql; do
  name=$(basename "$job" .sql)
  if grep -qF ": ${name} (" <<<"$listed"; then
    echo "skip   ${name}: already on the cluster"
    continue
  fi
  echo "submit ${name}"
  bin/sql-client.sh -i sql/init.sql -f "$job"
done
EOF
```

Add the script to the image, at the end of `flink/Dockerfile`:

```dockerfile

# Baked in rather than mounted: it changes far less often than the SQL does.
COPY --chmod=755 submit.sh /opt/flink/bin/spot-submit.sh
```

- [ ] **Step 4: Add the submit service**

Insert after `taskmanager` in `docker-compose.yml`:

```yaml
  # Runs once on `docker compose up`, submits any job that is not already
  # running, exits 0. Like kafka-init, a stopped container is its steady state.
  flink-submit:
    <<: *flink
    command: ["bash", "/opt/flink/bin/spot-submit.sh"]
    depends_on:
      jobmanager:
        condition: service_healthy
      taskmanager:
        condition: service_started
      kafka-init:
        condition: service_completed_successfully
      postgres:
        condition: service_healthy
    volumes:
      - ./flink/sql:/opt/flink/sql:ro
    restart: "no"
```

- [ ] **Step 5: Note where raw_plays stands, then stop the consumer**

```bash
docker compose exec -T postgres psql -U spot -d spot -c "
SELECT count(*) AS rows, max(started_at) AS newest FROM raw_plays;"
docker compose stop consumer
```

From here until Flink starts, nothing writes `raw_plays`, and the simulator's
events wait in the topic.

- [ ] **Step 6: Submit, and check what the submit script prints**

```bash
docker compose up -d --build flink-submit
docker compose logs -f flink-submit
```

Expected: `submit raw-passthrough`, then the SQL client's
`Submitting SQL update statement to the cluster...` and a `Job ID`, then the
container exits 0. Ctrl-C the log follow.

```bash
docker compose exec -T jobmanager bin/flink list -r
```

Expected: one line in the shape
`<date> <time> : <job id> : raw-passthrough (RUNNING)`. The submit script
matches `: raw-passthrough (` in this output. If the shape differs, fix the
`grep` in `submit.sh` before going on.

- [ ] **Step 7: Prove the submit is idempotent**

```bash
docker compose up -d flink-submit
docker compose logs flink-submit | tail -2
docker compose exec -T jobmanager bin/flink list -r | grep -c raw-passthrough
```

Expected: `skip   raw-passthrough: already on the cluster`, and a count of `1`.

- [ ] **Step 8: Prove Flink is writing, with the consumer still stopped**

```bash
sleep 30
docker compose exec -T postgres psql -U spot -d spot -c "
SELECT count(*) AS rows, max(started_at) AS newest FROM raw_plays;"
```

Expected: `rows` higher than in Step 5 and `newest` within the last minute,
with no consumer running. Flink also re-read the topic from the earliest
offset and rewrote every existing row. That rewrite is invisible, because each
upsert wrote the values that were already there — invariants 2 and 3 doing
their job.

- [ ] **Step 9: See how differently Kafka sees the two readers**

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group raw-plays-writer
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group flink-raw-passthrough
```

Expected, and worth reading slowly:

- `raw-plays-writer` (the stopped M2 consumer): `has no active members`, and
  `LAG` growing — it is the group M2 drained in its lag exercise.
- `flink-raw-passthrough`: `CURRENT-OFFSET` close to `LOG-END-OFFSET` and
  small `LAG` — *and also* no `CONSUMER-ID`, `has no active members`. Flink is
  reading, but not as a group member: there is no rebalance and no group
  assignment. It commits offsets here only on each checkpoint, which is why
  `CURRENT-OFFSET` moves in 30-second steps. The offsets Flink actually
  resumes from live in its checkpoints, not in Kafka.

Leave the consumer stopped. Task 6 deletes it.

- [ ] **Step 10: Commit**

```bash
git add flink/ docker-compose.yml
git commit -m "M4: raw passthrough job, idempotent submit, Flink writes raw_plays"
```

---

### Task 4: A fixture stream that closes its own windows

**Files:**
- Modify: `simulator/main.py`
- Test: `tests/test_simulator.py`

**Interfaces:**
- Consumes: `fixture_events(base) -> list[PlayEvent]`, `FIXTURE_BASE`, `_event(...)`, `load_catalog()` — all existing in `simulator/main.py`.
- Produces: `FIXTURE_FLUSH_OFFSET_SECONDS = 600`, `FIXTURE_FLUSH_LISTENER = "fixture-flush"`, `fixture_flush_event(base: datetime | None = None) -> PlayEvent`, `fixture_stream(base: datetime | None = None) -> list[PlayEvent]` (14 events: the 12 plays, `events[0]` again, the flush event). `run_fixture(sink)` sends `fixture_stream()` in order. `fixture_events()` itself is unchanged, and so are its 12-event tests.

- [ ] **Step 1: Write the failing tests**

Add `fixture_flush_event` and `fixture_stream` to the import from
`simulator.main` at the top of `tests/test_simulator.py`, so it reads:

```python
from simulator.main import (
    FIXTURE_BASE,
    build_listeners,
    fixture_events,
    fixture_flush_event,
    fixture_stream,
    run_fixture,
    settings_from_env,
)
```

Append to `tests/test_simulator.py`:

```python
# --- the stream fixture mode actually sends --------------------------------


def test_fixture_stream_is_the_twelve_plays_a_replay_and_the_flush():
    stream = fixture_stream()
    assert len(stream) == 14
    assert stream[:12] == fixture_events()
    assert stream[13] == fixture_flush_event()


def test_fixture_stream_replays_the_first_play_as_a_restart_would():
    # A poller restarted mid-track re-emits the play in flight. Same event_id,
    # so raw_plays keeps one row, and the windowed count must too.
    stream = fixture_stream()
    assert stream[12] == stream[0]
    assert stream[12].event_id == stream[0].event_id


def test_fixture_flush_event_closes_every_fixture_window():
    # The last fixture window ends at +3min; the watermark trails the latest
    # started_at by 30s. The flush must be later than both together.
    flush = fixture_flush_event()
    assert flush.started_at >= FIXTURE_BASE + timedelta(minutes=3, seconds=30)
    assert flush.listener_id not in {e.listener_id for e in fixture_events()}
    assert flush.is_synthetic


def test_run_fixture_sends_the_stream_in_order():
    class RecordingSink:
        def __init__(self):
            self.sent = []

        def send(self, event):
            self.sent.append(event)

        def flush(self, timeout=10.0):
            return 0

    sink = RecordingSink()
    run_fixture(sink)
    assert sink.sent == fixture_stream()
```

- [ ] **Step 2: Run them to see them fail**

Run: `.venv/bin/pytest tests/test_simulator.py -q`
Expected: collection error, `ImportError: cannot import name 'fixture_flush_event'`.

- [ ] **Step 3: Implement**

In `simulator/main.py`, replace `run_fixture` and add the two functions above it:

```python
# The fixture's twelve plays end at +2m38s. A window is written only when the
# watermark -- the latest started_at seen, minus 30s -- passes its end, so with
# nothing later the third minute would stay open forever. This event is ten
# minutes on, far past every fixture window. Its own window never closes, so it
# never appears in an agg_ table.
FIXTURE_FLUSH_OFFSET_SECONDS = 600
FIXTURE_FLUSH_LISTENER = "fixture-flush"


def fixture_flush_event(base: datetime | None = None) -> PlayEvent:
    """One later event whose only job is to move event time forward."""
    anchor = base or FIXTURE_BASE
    track = load_catalog()[0]
    started_at = anchor + timedelta(seconds=FIXTURE_FLUSH_OFFSET_SECONDS)
    return _event(FIXTURE_FLUSH_LISTENER, track, started_at,
                  started_at + timedelta(seconds=3))


def fixture_stream(base: datetime | None = None) -> list[PlayEvent]:
    """Exactly what fixture mode sends, in order: the twelve plays, the first
    play again -- the duplicate a poller restart mid-track produces -- and the
    flush event."""
    events = fixture_events(base)
    return [*events, events[0], fixture_flush_event(base)]


def run_fixture(sink: KafkaSink) -> None:
    events = fixture_stream()
    for event in events:
        sink.send(event)
    remaining = sink.flush()
    log.info("fixture mode: sent %d events, %d undelivered", len(events), remaining)
```

Also update the comment above `FIXTURE_SCHEDULE`, which promises counts the
stream now has to earn:

```python
# (offset seconds from FIXTURE_BASE, listener) -> 5 in minute 0, 4 in minute 1,
# 3 in minute 2. M4's integration test asserts exactly these counts, with the
# replayed first play counted once.
```

- [ ] **Step 4: Run them to see them pass**

Run: `.venv/bin/pytest tests/test_simulator.py -q`
Expected: all pass, the 12-event fixture tests unchanged among them.

- [ ] **Step 5: Run the whole suite**

Run: `.venv/bin/pytest -q`
Expected: `109 passed` — 105 before, plus 4.

- [ ] **Step 6: Commit**

```bash
git add simulator/main.py tests/test_simulator.py
git commit -m "M4: fixture stream adds a restart replay and a watermark flush event"
```

---

### Task 5: The tumbling window, test-first through the real stack

**Files:**
- Create: `docker-compose.it.yml`, `tests/integration/test_flink_windows.py`, `flink/sql/jobs/plays-per-minute.sql`
- Modify: `pytest.ini`, `flink/sql/init.sql`, the spec

**Interfaces:**
- Consumes: `fixture_stream()`, `fixture_events()` from Task 4; `flink-submit` from Task 3; `FIXTURE_BASE` (`2026-01-01T00:00:00Z`).
- Produces: Flink table `agg_plays_per_minute`; job `plays-per-minute`; marker `integration`; command `.venv/bin/pytest -m integration -v`.

This is the only real check on the Flink SQL, so the test comes first and is
run against the spec's own `COUNT(*)` to watch it fail for the reason given in
Decision 1.

- [ ] **Step 1: Write the overlay**

```bash
cat > docker-compose.it.yml <<'EOF'
# Integration-test overlay. Used only as
#   docker compose -p spot-it -f docker-compose.yml -f docker-compose.it.yml ...
# The project name gives the test its own network and volumes, so it starts
# from an empty topic and `down -v` destroys only its own data. This file only
# moves host ports out of the dev stack's way: two projects cannot both bind
# 8081 or 55432.
services:
  kafka:
    ports: !reset []
  postgres:
    ports: !override
      - "55433:5432"
  jobmanager:
    ports: !override
      - "18081:8081"
  grafana:
    ports: !reset []
EOF
docker compose -p spot-it -f docker-compose.yml -f docker-compose.it.yml config | grep -E 'published' | sort | uniq -c
```

Expected: published ports `18081` and `55433` only. Grafana is not started by
the test, and `kafka` has no host port at all.

- [ ] **Step 2: Register the marker**

```bash
cat > pytest.ini <<'EOF'
[pytest]
pythonpath = .
testpaths = tests
# Integration tests start a Compose stack and take minutes. Excluded by default
# so `pytest -q` stays the fast offline unit run; `pytest -m integration`
# selects them, because the last -m on the command line wins.
markers =
    integration: runs against an isolated live Compose stack (project spot-it)
addopts = -m "not integration"
EOF
```

- [ ] **Step 3: Write the test**

```bash
mkdir -p tests/integration
cat > tests/integration/test_flink_windows.py <<'EOF'
"""The fixture stream through Kafka and Flink into Postgres, rows asserted
exactly.

Runs only with `pytest -m integration`. It brings up its own Compose project,
spot-it, so it never touches the dev stack. Set SPOT_IT_KEEP=1 to leave that
stack running afterwards; its Flink UI is on :18081 and Postgres on :55433.
"""

from __future__ import annotations

import os
import subprocess
import time
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from simulator.main import FIXTURE_FLUSH_LISTENER, fixture_events, fixture_stream

pytestmark = pytest.mark.integration

UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)
COMPOSE = ["docker", "compose", "-p", "spot-it",
           "-f", "docker-compose.yml", "-f", "docker-compose.it.yml"]
DSN = "postgresql://spot:spot@localhost:55433/spot"
FIXTURE_LISTENERS = ["fixture-a", "fixture-b"]

# The poller never starts here, but Compose interpolates the whole file before
# it looks at which services were asked for, and the poller's
# ${SPOTIFY_CLIENT_ID:?} guard would refuse a clone with no .env.
ENV = {**os.environ,
       "SPOTIFY_CLIENT_ID": "unused-in-integration",
       "SPOTIFY_CLIENT_SECRET": "unused-in-integration"}

# Cold image builds, cluster start, job submission, then 30s of partition
# idleness before the last window can close.
TIMEOUT_SECONDS = 300


def compose(*args: str) -> None:
    subprocess.run([*COMPOSE, *args], env=ENV, check=True)


def minute(n: int) -> datetime:
    return T0 + timedelta(minutes=n)


def wait_for_count(conn, sql: str, expected: int) -> int:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while True:
        count = conn.execute(sql).fetchone()[0]
        if count >= expected or time.monotonic() > deadline:
            return count
        time.sleep(3)


@pytest.fixture(scope="module")
def db():
    # Brings up Kafka, the topic, Postgres and the Flink cluster, and submits
    # every job: flink-submit depends on all of them.
    compose("up", "-d", "--build", "flink-submit")
    compose("run", "--rm", "--build", "-e", "SIM_MODE=fixture", "simulator")
    try:
        with psycopg.connect(DSN, autocommit=True) as conn:
            yield conn
    finally:
        if not os.environ.get("SPOT_IT_KEEP"):
            compose("down", "-v")


@pytest.fixture(scope="module")
def windows(db):
    """Every fixture window, once all six have closed."""
    wait_for_count(db, "SELECT count(*) FROM agg_plays_per_minute "
                       "WHERE listener_id IN ('fixture-a', 'fixture-b')", 6)
    return db.execute(
        "SELECT window_start, window_end, listener_id, play_count "
        "FROM agg_plays_per_minute WHERE listener_id = ANY(%s) "
        "ORDER BY window_start, listener_id",
        (FIXTURE_LISTENERS,)).fetchall()


def test_tumbling_windows_count_each_play_exactly_once(windows):
    # 5 / 4 / 3 plays across three minutes, split by listener. The replayed
    # first play sits in minute 0 for fixture-a: COUNT(*) would make that 4.
    assert windows == [
        (minute(0), minute(1), "fixture-a", 3),
        (minute(0), minute(1), "fixture-b", 2),
        (minute(1), minute(2), "fixture-a", 2),
        (minute(1), minute(2), "fixture-b", 2),
        (minute(2), minute(3), "fixture-a", 1),
        (minute(2), minute(3), "fixture-b", 2),
    ]


def test_a_window_still_open_is_never_written(db, windows):
    # The flush event's minute has no later event to close it.
    count = db.execute("SELECT count(*) FROM agg_plays_per_minute "
                       "WHERE listener_id = %s", (FIXTURE_FLUSH_LISTENER,)).fetchone()[0]
    assert count == 0


def test_passthrough_keeps_one_row_per_event_id(db, windows):
    # 14 messages on the topic, 13 distinct event_ids: the replay collapses.
    distinct_ids = {e.event_id for e in fixture_stream()}
    assert len(distinct_ids) == 13
    rows = db.execute("SELECT event_id FROM raw_plays "
                      "WHERE listener_id LIKE 'fixture-%'").fetchall()
    assert {r[0] for r in rows} == distinct_ids
    assert len(rows) == 13


def test_passthrough_preserves_the_instant(db, windows):
    first = fixture_events()[0]
    started_at, observed_at = db.execute(
        "SELECT started_at, observed_at FROM raw_plays WHERE event_id = %s",
        (first.event_id,)).fetchone()
    assert started_at == T0
    assert observed_at == T0 + timedelta(seconds=3)
EOF
```

`psycopg` returns `TIMESTAMPTZ` as an aware `datetime`, and aware datetimes
compare as instants. A time-zone mistake anywhere in the chain shifts the
instant and fails these equalities; it cannot hide behind formatting.

- [ ] **Step 4: Confirm the default suite still skips it**

Run: `.venv/bin/pytest -q`
Expected: `109 passed, 4 deselected`.

- [ ] **Step 5: Declare the windowed sink**

```bash
cat >> flink/sql/init.sql <<'EOF'

-- window_start and window_end are TIMESTAMP(3), with no zone: that is what a
-- window over a TIMESTAMP_LTZ column produces, in the session zone pinned to
-- UTC above.
CREATE TABLE agg_plays_per_minute (
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  listener_id  STRING,
  play_count   BIGINT,
  PRIMARY KEY (window_start, listener_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/spot',
  'table-name' = 'agg_plays_per_minute',
  'username' = 'spot',
  'password' = 'spot'
);
EOF
```

- [ ] **Step 6: Write the job exactly as the spec has it**

```bash
cat > flink/sql/jobs/plays-per-minute.sql <<'EOF'
SET 'pipeline.name' = 'plays-per-minute';

-- An OPTIONS hint is not allowed inside TABLE(...), so this job's group id is
-- applied through a view. A SELECT * view keeps started_at's time attribute.
CREATE TEMPORARY VIEW plays_for_plays_per_minute AS
SELECT * FROM plays /*+ OPTIONS('properties.group.id' = 'flink-plays-per-minute') */;

-- One-minute tumbling windows: each play lands in exactly one. A window's row
-- is written once, when the watermark passes window_end.
INSERT INTO agg_plays_per_minute
SELECT window_start, window_end, listener_id, COUNT(*) AS play_count
FROM TABLE(TUMBLE(TABLE plays_for_plays_per_minute, DESCRIPTOR(started_at),
                  INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, listener_id;
EOF
```

- [ ] **Step 7: Run the integration test and watch it fail**

The test starts a second full stack beside the dev one. Check there is room
first:

```bash
free -g | awk '/Mem/ {print "available GB:", $7}'
```

Below about 4 GB, stop the dev stack's heaviest services for the duration:
`docker compose stop taskmanager jobmanager simulator`, then
`docker compose up -d` afterwards. The integration test does not depend on
the dev stack at all.

Run: `.venv/bin/pytest -m integration -v`
Expected: three pass, and `test_tumbling_windows_count_each_play_exactly_once`
**fails** on its first tuple — `('fixture-a', 4)` where `3` was expected. The
passthrough tests pass on the same data: `raw_plays` collapsed the replay onto
one primary key, and the window counted both messages. That is Decision 1,
seen from the outside.

- [ ] **Step 8: Count plays, not messages**

In `flink/sql/jobs/plays-per-minute.sql`, replace the `INSERT` statement and
the comment above it with:

```sql
-- One-minute tumbling windows: each play lands in exactly one. A window's row
-- is written once, when the watermark passes window_end.
--
-- COUNT(DISTINCT event_id), not COUNT(*): the topic is at-least-once, and a
-- poller restart puts the same play on it twice. raw_plays absorbs that on
-- its primary key; this table is keyed by window, so it has to dedupe here.
-- Exact, not approximate: duplicates share event_id only if started_at floors
-- to the same 5s bucket, and 5s buckets never straddle a minute.
INSERT INTO agg_plays_per_minute
SELECT window_start, window_end, listener_id,
       COUNT(DISTINCT event_id) AS play_count
FROM TABLE(TUMBLE(TABLE plays_for_plays_per_minute, DESCRIPTOR(started_at),
                  INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, listener_id;
```

- [ ] **Step 9: Run it again**

Run: `.venv/bin/pytest -m integration -v`
Expected: `4 passed`.

- [ ] **Step 10: Correct the spec**

In the spec's "The three aggregations" section, change the tumbling statement's
`COUNT(*)` to `COUNT(DISTINCT event_id)`, and add this paragraph directly
after that code block:

```markdown
`COUNT(DISTINCT event_id)` rather than `COUNT(*)`, corrected in M4: the topic
is at-least-once, and a poller restart puts the same play on it twice.
`raw_plays` absorbs a duplicate because both writes land on its `event_id`
primary key, but this table is keyed by window, so the dedupe has to happen in
the query. It is exact: two messages share an `event_id` only if `started_at`
floors to the same 5-second bucket, and those buckets never straddle a minute.
The hopping and session statements below need the same treatment when M5 and
M6 build them.
```

- [ ] **Step 11: Submit it to the dev stack**

```bash
docker compose up -d flink-submit
docker compose logs flink-submit | grep -E '^(skip|submit)'
```

Expected: `skip   raw-passthrough: already on the cluster` and `submit plays-per-minute`.

- [ ] **Step 12: Commit**

```bash
git add docker-compose.it.yml pytest.ini tests/integration/ flink/sql/ \
  docs/superpowers/specs/2026-09-11-spotify-streaming-pipeline-design.md
git commit -m "M4: plays-per-minute tumbling window, integration test on fixture data

The spec's COUNT(*) counts Kafka messages, so a poller restart's
re-emission made one minute read 4 where raw_plays had 3 rows. Counting
distinct event_ids is exact because a duplicate never leaves its minute."
```

---

### Task 6: Retire the consumer

**Files:**
- Delete: `consumer/`, `tests/test_consumer.py`, `tests/test_pg_sink.py`
- Modify: `docker-compose.yml`, `Dockerfile`, `requirements.txt`, `requirements-dev.txt`, `events/schema.py`

**Interfaces:**
- Consumes: Flink writing `raw_plays` on its own, proven in Task 3 Step 8.
- Produces: an image with no Postgres client in it; a suite of 95 unit tests (109 minus the 14 that tested the consumer).

M2 built this consumer so that groups, offsets and rebalancing could be seen
before a framework hid them. Task 3 showed the framework hiding them. It has
done its job.

- [ ] **Step 1: Delete the package and its tests**

```bash
git rm -r -q consumer/ tests/test_consumer.py tests/test_pg_sink.py
```

- [ ] **Step 2: Remove the service and the image line**

Delete the whole `consumer:` service block from `docker-compose.yml`, from
`  consumer:` through its `restart: unless-stopped`. Then:

```bash
python3 - <<'EOF'
from pathlib import Path
path = Path("Dockerfile")
path.write_text(path.read_text().replace("COPY consumer/ consumer/\n", ""))
EOF
grep -n consumer Dockerfile docker-compose.yml || echo "no consumer left"
```

Expected: `no consumer left`.

- [ ] **Step 3: Move psycopg to the dev requirements**

```bash
cat > requirements.txt <<'EOF'
confluent-kafka==2.6.1
requests==2.32.3
EOF
cat > requirements-dev.txt <<'EOF'
-r requirements.txt
pytest==8.3.4
# The integration test reads Postgres directly. No service image needs it now.
psycopg[binary]==3.2.9
EOF
```

- [ ] **Step 4: Stop naming the consumer in the schema's docstring**

In `events/schema.py`, line 3 currently reads `Both producers, the throwaway M2
consumer, and the tests import this module`. Replace `the throwaway M2
consumer, ` with nothing, leaving `Both producers and the tests import this
module` (rewrap the paragraph if the line now reads oddly).

- [ ] **Step 5: Remove the container and the group it left behind**

```bash
docker compose up -d --build --remove-orphans
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --delete --group raw-plays-writer
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --list
```

Expected: `Deletion of requested consumer groups ('raw-plays-writer') was
successful`, then a list showing only the `flink-*` groups. A group outlives
its consumers until it is deleted — Kafka kept `raw-plays-writer`'s offsets
the whole time the consumer was stopped.

- [ ] **Step 6: Run the suite**

Run: `.venv/bin/pytest -q`
Expected: `95 passed, 4 deselected`.

- [ ] **Step 7: Commit**

```bash
git add -A consumer/ tests/ docker-compose.yml Dockerfile requirements.txt requirements-dev.txt events/schema.py
git commit -m "M4: retire the M2 consumer; Flink owns raw_plays"
```

---

### Task 7: The dashboard reads Flink's windows

**Files:**
- Modify: `grafana/dashboards/spot.json`

**Interfaces:**
- Consumes: `agg_plays_per_minute`, being filled by the dev stack's `plays-per-minute` job; the `synthetic` variable from M3.
- Produces: panel 1 reading `agg_plays_per_minute`.

- [ ] **Step 1: Repoint panel 1**

```bash
python3 - <<'EOF'
import json
from pathlib import Path

path = Path("grafana/dashboards/spot.json")
dashboard = json.loads(path.read_text())
panel = next(p for p in dashboard["panels"] if p["id"] == 1)

panel["title"] = "Plays per minute, by listener (Flink)"
panel["description"] = (
    "Read from agg_plays_per_minute, which Flink writes once per closed "
    "one-minute window. A minute appears only after the watermark passes its "
    "end -- 30s behind the newest started_at -- so the right edge of this chart "
    "always trails the clock by at least that much.")
# agg_plays_per_minute has no is_synthetic column. Each listener id belongs to
# exactly one producer, so the Source filter selects listener ids instead.
# ${synthetic:csv} renders as `false,true` for All (the custom allValue is
# inserted raw) and `false` or `true` for one choice -- all boolean literals.
panel["targets"][0]["rawSql"] = (
    "SELECT window_start AS time, listener_id AS metric, play_count AS value "
    "FROM agg_plays_per_minute "
    "WHERE $__timeFilter(window_start) "
    "AND listener_id IN (SELECT DISTINCT listener_id FROM raw_plays "
    "WHERE is_synthetic IN (${synthetic:csv})) "
    "ORDER BY 1")
path.write_text(json.dumps(dashboard, indent=2) + "\n")
print(panel["targets"][0]["rawSql"])
EOF
```

- [ ] **Step 2: Check the query with each rendering Grafana produces**

These are the three renderings M3 observed in the Postgres log, not guesses:

```bash
for v in "false,true" "false" "true"; do
  docker compose exec -T postgres psql -U spot -d spot -At -c "
  SELECT '($v)', count(*), count(DISTINCT listener_id) FROM agg_plays_per_minute
  WHERE window_start > now() - interval '15 minutes'
    AND listener_id IN (SELECT DISTINCT listener_id FROM raw_plays WHERE is_synthetic IN ($v));"
done
```

Expected: three lines, no errors. Listener counts: about 21, at most 1 (the
real account, if it played in the last 15 minutes), and about 20.

- [ ] **Step 3: Check what Grafana actually sends**

```bash
docker compose exec -T postgres psql -U spot -d spot -qc "ALTER SYSTEM SET log_statement = 'all'" -c "SELECT pg_reload_conf()"
```

Ask the owner to reload <http://localhost:3000/d/spot-live> and click *All*,
*real* and *synthetic* in the Source dropdown. Then:

```bash
docker compose logs --since 2m postgres | grep -o -E 'FROM agg_plays_per_minute.{0,200}|ERROR:.*' | sort | uniq -c
docker compose exec -T postgres psql -U spot -d spot -qc "ALTER SYSTEM RESET log_statement" -c "SELECT pg_reload_conf()"
```

Expected: the panel's query with each of the three renderings, and no `ERROR`
lines. The panel shows bars that stop a minute or so short of now.

- [ ] **Step 4: Commit**

```bash
git add grafana/dashboards/spot.json
git commit -m "M4: plays-per-minute panel reads Flink's tumbling windows"
```

---

### Task 8: Milestone acceptance

**Files:** Modify: this plan file, `CLAUDE.md`

- [ ] **Step 1: Watch the watermark advance**

```bash
cat > /tmp/watermarks.py <<'EOF'
import datetime, json, urllib.request

def get(path):
    return json.load(urllib.request.urlopen("http://localhost:8081" + path))

for job in get("/jobs/overview")["jobs"]:
    if job["state"] != "RUNNING":
        continue
    for vertex in get(f"/jobs/{job['jid']}")["vertices"]:
        for mark in get(f"/jobs/{job['jid']}/vertices/{vertex['id']}/watermarks"):
            if mark["id"].endswith("currentInputWatermark") and int(mark["value"]) > 0:
                ts = datetime.datetime.fromtimestamp(int(mark["value"]) / 1000, datetime.UTC)
                print(f"{job['name']:18} {vertex['name'][:40]:40} {ts:%H:%M:%S}")
EOF
python3 /tmp/watermarks.py; sleep 20; python3 /tmp/watermarks.py; date -u +%H:%M:%S
```

Expected: the same operators twice, each watermark later the second time, and
both roughly 30 s or more behind the UTC clock printed last. The source shows
no input watermark of its own; the window operator's is the one that matters.
In the UI: <http://localhost:8081> → `plays-per-minute` → the window operator →
*Watermarks*.

- [ ] **Step 2: Flink's count against a count of the raw rows**

For every window Flink has closed, Postgres can recount the same minute from
`raw_plays`. They must agree exactly.

```bash
docker compose exec -T postgres psql -U spot -d spot -c "
WITH flink AS (
  SELECT window_start, listener_id, play_count FROM agg_plays_per_minute
), recount AS (
  SELECT date_trunc('minute', started_at) AS window_start, listener_id,
         count(*) AS play_count
  FROM raw_plays
  WHERE started_at < (SELECT max(window_end) FROM agg_plays_per_minute)
  GROUP BY 1, 2
)
SELECT count(*)                                         AS windows,
       count(*) FILTER (WHERE f.play_count IS NULL)     AS missing_from_flink,
       count(*) FILTER (WHERE r.play_count IS NULL)     AS missing_from_raw,
       count(*) FILTER (WHERE f.play_count <> r.play_count) AS disagree
FROM flink f FULL JOIN recount r USING (window_start, listener_id);"
```

Expected: hundreds of `windows`, `missing_from_raw` and `disagree` both `0`,
and `missing_from_flink` `0` or very close to it. With `SIM_LATE_EVENT_RATE=0`
the simulator never produces late data. The poller can: when it first sees a
track more than 30 s in — for example right after the poller starts — the
derived `started_at` is already behind the watermark. If that crossed a
minute boundary, Flink correctly drops it as late, and the play exists only in
`raw_plays`. If the column is non-zero, find out whose rows they are:

```bash
docker compose exec -T postgres psql -U spot -d spot -c "
SELECT r.listener_id, r.window_start, r.play_count
FROM (SELECT date_trunc('minute', started_at) AS window_start, listener_id, count(*) AS play_count
      FROM raw_plays WHERE started_at < (SELECT max(window_end) FROM agg_plays_per_minute) GROUP BY 1, 2) r
LEFT JOIN agg_plays_per_minute f USING (window_start, listener_id)
WHERE f.play_count IS NULL;"
```

Real-listener rows are late data, and the late-event panel that will surface
them is M7's. Synthetic rows would be a bug.

- [ ] **Step 3: Kill the TaskManager and watch the job recover**

```bash
curl -s http://localhost:8081/jobs/overview | python3 -c '
import json,sys
for j in json.load(sys.stdin)["jobs"]: print(j["jid"], j["name"], j["state"])'
docker compose kill taskmanager
# Losing a TaskManager is noticed on a failed heartbeat, not instantly.
until docker compose exec -T jobmanager bin/flink list -r | grep -q RESTARTING; do sleep 2; done
docker compose exec -T jobmanager bin/flink list -r
docker compose up -d taskmanager
until [ "$(docker compose exec -T jobmanager bin/flink list -r | grep -c '(RUNNING)')" = 2 ]; do sleep 3; done
docker compose exec -T jobmanager bin/flink list -r
```

Expected: after the kill, both jobs `(RESTARTING)` — the restart strategy is
waiting for slots that no longer exist. Once the new TaskManager registers,
both are `(RUNNING)` again *with the same job ids*. Note how long it took. That is the same job
restored from its last checkpoint, not a resubmission. Confirm the restore:

```bash
for jid in $(curl -s http://localhost:8081/jobs/overview | python3 -c '
import json,sys; print(" ".join(j["jid"] for j in json.load(sys.stdin)["jobs"] if j["state"]=="RUNNING"))'); do
  curl -s "http://localhost:8081/jobs/$jid/checkpoints" | python3 -c '
import json,sys; d=json.load(sys.stdin); r=d["latest"]["restored"]
print("restored from checkpoint", r["id"] if r else None)'
done
```

Expected: a checkpoint id for each job. Re-run Step 2: still `0`, `0`, `0`. The
operator state rewound to the checkpoint and the Kafka offsets rewound with
it, so the events after the checkpoint were read again. Open windows were
rebuilt from them, and closed windows were rewritten with the same counts.

- [ ] **Step 4: Cancel the windowed job and resubmit it from scratch**

This is the replay of Decision 8: a brand-new job, no checkpoint, reading from
the earliest offset. A fixed cut-off taken before the cancel makes the before
and after numbers cover the same, already-closed windows.

```bash
q() { docker compose exec -T postgres psql -U spot -d spot -At -c "$1"; }
cutoff=$(q "SELECT max(window_end) FROM agg_plays_per_minute")
q "SELECT count(*), sum(play_count) FROM agg_plays_per_minute WHERE window_end <= '$cutoff'"
jid=$(curl -s http://localhost:8081/jobs/overview | python3 -c '
import json,sys; print(next(j["jid"] for j in json.load(sys.stdin)["jobs"] if j["name"]=="plays-per-minute" and j["state"]=="RUNNING"))')
docker compose exec -T jobmanager bin/flink cancel "$jid"
docker compose up -d flink-submit
docker compose logs flink-submit | grep -E '^(skip|submit)'
sleep 60
q "SELECT count(*), sum(play_count) FROM agg_plays_per_minute WHERE window_end <= '$cutoff'"
```

Expected: `skip   raw-passthrough` and `submit plays-per-minute`, then the
second line **identical** to the first. The new job recomputed every one of
those windows from the topic, and every upsert wrote a count that was already
there — nothing lost, nothing doubled. Re-run Step 2 for the same result as
before.

- [ ] **Step 5: The full suite and the integration test**

Run: `.venv/bin/pytest -q`
Expected: `95 passed, 4 deselected`.

Run: `.venv/bin/pytest -m integration -v`
Expected: `4 passed`, and afterwards `docker compose -p spot-it ps -a` lists nothing.

- [ ] **Step 6: A cold start reaches a filled dashboard**

```bash
docker compose down -v
docker compose up -d --build
until docker compose exec -T postgres psql -U spot -d spot -At -c "SELECT count(*) FROM agg_plays_per_minute" 2>/dev/null | grep -qv '^0$'; do sleep 5; done
docker compose ps -a --format '{{.Service}}\t{{.Status}}'
docker compose exec -T jobmanager bin/flink list -r
docker compose exec -T postgres psql -U spot -d spot -c "
SELECT (SELECT count(*) FROM raw_plays) AS raw, (SELECT count(*) FROM agg_plays_per_minute) AS per_minute,
       (SELECT count(*) FROM agg_top_artists) AS top_artists, (SELECT count(*) FROM agg_sessions) AS sessions;"
curl -s -o /dev/null -w "grafana %{http_code}\n" http://localhost:3000/d/spot-live
```

Expected: the wait ends within about two minutes — the first full minute after
the start, plus 30 s of watermark delay. No `consumer` in the service list.
`kafka-init` and `flink-submit` are `Exited (0)`, and everything else is `Up`.
Both jobs are `(RUNNING)`. `raw` and `per_minute` are non-zero,
`top_artists` and `sessions` are `0`, and Grafana returns `200`.

- [ ] **Step 7: Check the milestone acceptance criteria**

From the spec's M4 section:

- [ ] `agg_plays_per_minute` fills (Steps 2 and 6).
- [ ] The Flink Web UI shows the watermark advancing (Step 1).
- [ ] The integration test on the fixture event set passes (Step 5).
- [ ] The Python consumer is retired from the Compose stack, with Flink writing `raw_plays` (Task 3 Step 8, Task 6).

Plus the project's own standards:

- [ ] `.venv/bin/pytest -q` is green and offline.
- [ ] `docker compose up -d --build` from cold reaches a filled dashboard.
- [ ] `agg_top_artists` and `agg_sessions` are still empty.
- [ ] `.env` and `.spotify_token.json` are untracked.

- [ ] **Step 8: Update CLAUDE.md**

Rewrite "Current state" for after M4:

- M0-M4 complete; next is M5, the hopping window.
- Flink runs a session cluster, Web UI on `:8081`, with jobs submitted from `flink/sql/jobs/` by `flink-submit`.
- `consumer/` is gone. Replace its bullet with: Flink commits offsets to `flink-<job>` groups for visibility only.
- Two test commands: `.venv/bin/pytest -q` (95 unit tests, offline), and `.venv/bin/pytest -m integration` (the isolated `spot-it` stack, a few minutes).
- New windowed statements count `DISTINCT event_id`.

Also update the Architecture section's "4 stmts": with M4, two jobs exist.

- [ ] **Step 9: Record what actually happened**

Replace the Results section below with the real numbers and output, as M2 and
M3 did. Include especially: the red integration run's actual failing tuple;
the watermark lag observed in Step 1; the Step 2 window count; and the
recovery time in Step 3.

- [ ] **Step 10: Commit**

```bash
git add docs/superpowers/plans/2026-10-02-m4-flink-tumbling-window.md CLAUDE.md
git commit -m "M4: mark plan complete, record acceptance results"
```

---

## Results

*Filled in during Task 8, Step 9.*

---

## Notes for M5

1. **Hopping windows need `COUNT(DISTINCT event_id)` too**, for the same
   reason, and it stays exact. The hop is one minute, so every window edge is
   on a minute boundary, and a duplicate pair can never be split across one.
2. **M5 is one new file**, `flink/sql/jobs/top-artists.sql`, plus its sink in
   `init.sql`. `flink-submit` will submit it on the next `up` and skip the two
   running jobs. Nothing needs cancelling.
3. **The integration test grows rather than forks.** It already waits for the
   fixture's windows to close. M5's assertion that one event appears in
   exactly five hopping windows belongs in the same module, using the same
   `db` fixture and the same single stack start.
4. **Use the fixture's flush event for window closing, not `SIM_SPEED`.** The
   M2 finding still stands: high `SIM_SPEED` collides `event_id`s, and
   `COUNT(DISTINCT)` would now silently undercount them.
