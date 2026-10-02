# Real-Time "Now Playing" Event Pipeline — Design

**Date:** 2026-09-11
**Status:** Approved, ready for implementation planning

## Goal

Build a working streaming pipeline that turns Spotify listening activity into a
live dashboard, as a vehicle for learning Kafka and stream processing from
scratch. Learning is the primary output; the dashboard is the evidence it works.

### Learning objectives

Ordered by how central they are to the project:

1. Kafka fundamentals — topics, partitions, keys, offsets, consumer groups, lag
2. Event time vs processing time, and why watermarks exist
3. Window semantics — tumbling, hopping, session — and when each applies
4. Turning a polled state endpoint into an event stream
5. At-least-once delivery with idempotent writes
6. Operating a multi-service pipeline locally

### Non-goals

Explicitly out of scope. Each is a reasonable later extension, and none is
needed to meet the objectives above:

- Schema Registry / Avro / Protobuf (JSON is enough at this scale)
- Exactly-once via two-phase commit (idempotent writes cover it)
- Kafka Connect (the sinks are simple enough to declare in Flink SQL)
- Multi-broker clusters, replication, or any HA concern
- Cloud deployment
- Multi-user OAuth onboarding (the simulator covers the multi-listener case)
- Skip detection (considered and deliberately dropped to limit scope)

## Constraints

- **Budget: $0.** Everything runs locally under Docker Compose. A Spotify
  developer app is free.
- **Host resources:** ~9 GB RAM available. The chosen stack fits in ~4.5 GB.
- **Spotify API:** `/me/player/currently-playing` returns *state*, not events,
  and only for the token holder. Rate limit is roughly 180 req/min over a
  rolling window; polling at 10s uses 6 req/min.

## Architecture

```
  ┌──────────────────┐
  │ spotify-poller   │  Polls /me/player/currently-playing every 10s.
  │ (real account)   │  Holds last-seen state, emits ONE event per
  └────────┬─────────┘  play transition.
           │
           │                      ┌─ key = listener_id ─┐
           ├─────────────────────►│   Kafka topic:      │
           │                      │   plays             │
  ┌────────┴─────────┐            │   3 partitions      │
  │ simulator        │            └──────────┬──────────┘
  │ N fake listeners │                       │
  └──────────────────┘                       ▼
                                   ┌──────────────────┐
                                   │    Flink SQL     │
                                   │  4 statements    │
                                   └────────┬─────────┘
                                            │ JDBC upsert
                                            ▼
                                   ┌──────────────┐   ┌─────────┐
                                   │   Postgres   │◄──│ Grafana │
                                   │  agg_* tables│   │ 5s live │
                                   └──────────────┘   └─────────┘
```

### Service versions

| Service | Image | Notes |
|---|---|---|
| Kafka | `apache/kafka:3.9.0` | KRaft mode, single broker, no ZooKeeper |
| Flink | `flink:1.20-java17` + custom layer | 1.20 required for the `SESSION` table function |
| Postgres | `postgres:16-alpine` | |
| Grafana | `grafana/grafana:11.3.0` | Provisioned from files in git |

The Flink image is extended by a `Dockerfile` that drops three connector jars
into `/opt/flink/lib`: the Kafka SQL connector, the JDBC connector, and the
PostgreSQL driver. Exact jar coordinates are verified against the Flink 1.20
compatibility matrix as an M0 acceptance step, since connector versioning is
independent of the Flink release.

## Event model

One JSON message per play, produced to topic `plays`.

```json
{
  "event_id":     "b3f1c9...",
  "listener_id":  "nishad",
  "is_synthetic": false,
  "track_id":     "3n3Ppam7vgaVa1iaRUc9Lp",
  "track_name":   "Mr. Brightside",
  "artist_name":  "The Killers",
  "album_name":   "Hot Fuss",
  "duration_ms":  222075,
  "started_at":   "2026-09-11T10:03:22Z",
  "observed_at":  "2026-09-11T10:03:31Z"
}
```

### Partition key: `listener_id`

Kafka guarantees ordering only within a partition. Keying by `listener_id`
puts all of one listener's plays on one partition in order, which is what
session windows require for correctness. Keying by `track_id` would scatter a
listener's history across partitions and silently corrupt sessions.

This is demonstrable: M6 includes an exercise that re-keys the topic by
`track_id` and shows sessions breaking.

### Two timestamps

- `started_at` = `observed_at - progress_ms`. Derived, not observed: a poll
  that catches a track 9 seconds in tells us when it actually started. This is
  the **event time** column.
- `observed_at` = wall-clock time of the poll. Retained so that
  `observed_at - started_at` can be charted as poll drift, making event-time
  skew a visible quantity rather than an abstraction.

### Idempotent event IDs

```
event_id = sha1(listener_id | track_id | started_at rounded to 5s)
```

The poller keeps state in memory, so a restart mid-song re-emits the current
track. A deterministic `event_id` makes that duplicate harmless: every sink
upserts on primary key, so the second write is a no-op. This is at-least-once
delivery plus idempotent writes — the pattern most production pipelines use
instead of true exactly-once.

Rounding to 5s absorbs sub-second jitter in the `progress_ms` subtraction
without risking collision between genuinely distinct plays, which are at
minimum seconds apart.

### Topic configuration

| Setting | Value | Reason |
|---|---|---|
| Partitions | 3 | Enough to observe rebalancing and per-partition ordering |
| Replication factor | 1 | Single broker; HA is a non-goal |
| Retention | 7 days | Long enough to replay the topic from offset 0 |
| Compression | `snappy` | |

## Components

```
spot/
├── docker-compose.yml
├── events/schema.py          # PlayEvent + (de)serialization — shared truth
├── producer/kafka_sink.py    # confluent-kafka wrapper: key, serialize, deliver
├── poller/
│   ├── spotify_auth.py       # OAuth authorization-code flow + token refresh
│   ├── spotify_client.py     # HTTP, 429/Retry-After, 204 handling
│   ├── transition.py         # pure function, no I/O
│   └── main.py               # the polling loop, wiring the above
├── simulator/
│   ├── catalog.py            # seeded artists/tracks, fetched once and cached
│   └── main.py               # N virtual listeners, speed dial via env
├── flink/
│   ├── Dockerfile
│   └── sql/                  # DDL + 3 INSERT statements
├── postgres/init.sql
├── grafana/provisioning/     # datasource + dashboard JSON, in git
└── tests/
```

### `events/schema.py`

Single source of truth for the event shape. Both producers import it; the
throwaway M2 consumer and the integration tests deserialize with it.

- `PlayEvent` dataclass matching the JSON above
- `to_json()` / `from_json()`
- `compute_event_id(listener_id, track_id, started_at)`

Depends on: nothing but the standard library.

### `poller/transition.py`

The only genuinely subtle logic in the project, isolated so it can be tested
exhaustively without network, Kafka, or a clock.

```
transition(previous_state, current_api_response) -> (Optional[PlayEvent], new_state)
```

| Situation | Emit | New state |
|---|---|---|
| Nothing → playing T | new play event for T | T |
| T → T, `progress_ms` advanced | none | T (progress updated) |
| T → T, `progress_ms` jumped backwards >10s | new play event for T (replay) | T |
| T → U | new play event for U | U |
| T → paused | none | T, marked paused |
| paused T → playing T | none | T |
| T → nothing (`204`) | none | T retained |

Retaining state across pause and `204` is what prevents a paused-then-resumed
track from being counted twice.

The 10s backwards threshold distinguishes a genuine replay from the small
negative jitter that arises when `progress_ms` and the poll timestamp are
sampled slightly apart.

Depends on: `events/schema.py`.

### `poller/spotify_auth.py`

Authorization-code flow, run once interactively to obtain a refresh token,
which is then persisted to a gitignored file. Thereafter the poller refreshes
the access token proactively at 55 minutes.

Scope required: `user-read-currently-playing`.

### `poller/spotify_client.py`

Wraps the HTTP calls. Responsibilities: attach bearer token, honour
`Retry-After` on `429`, translate `204 No Content` into "nothing playing"
rather than an error, and back off exponentially on network failure without
ever crashing the loop.

### `simulator/main.py`

N virtual listeners, each with a preferred-artist distribution drawn from the
cached catalog, emitting plays on their own schedule. Configuration by
environment variable:

- `SIM_LISTENERS` — how many virtual listeners (default 20)
- `SIM_SPEED` — time compression multiplier (default 1.0); raising this is the
  volume dial used to build consumer lag on demand
- `SIM_LATE_EVENT_RATE` — fraction of events deliberately emitted with an
  `started_at` older than the current watermark, to exercise late-data handling
- `SIM_MODE` — `live` (continuous, randomised) or `fixture` (a fixed,
  deterministic event set used by the integration tests)

Events carry `is_synthetic: true` so real and simulated data can be separated
in any query.

### `simulator/catalog.py`

Seed data of real artists and tracks, fetched once from the Spotify search API
and cached to a JSON file committed to the repo. This keeps synthetic events
realistic without requiring API access at simulator runtime.

## Flink SQL

### Source table

```sql
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
  'properties.group.id' = 'flink-plays',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);
```

A 30-second watermark delay bounds how long Flink waits for stragglers. It is
comfortably wider than the 10-second poll interval, so ordinary poll jitter
never produces late data; only events the simulator marks late will be.

### The three aggregations

**Plays per minute — tumbling.** Non-overlapping one-minute buckets.

```sql
INSERT INTO agg_plays_per_minute
SELECT window_start, window_end, listener_id, COUNT(DISTINCT event_id)
FROM TABLE(TUMBLE(TABLE plays, DESCRIPTOR(started_at), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, listener_id;
```

`COUNT(DISTINCT event_id)` rather than `COUNT(*)`, corrected in M4: the topic
is at-least-once, and a poller restart puts the same play on it twice.
`raw_plays` absorbs a duplicate because both writes land on its `event_id`
primary key, but this table is keyed by window, so the dedupe has to happen in
the query. It is exact: two messages share an `event_id` only if `started_at`
floors to the same 5-second bucket, and those buckets never straddle a minute.
The hopping and session statements below need the same treatment when M5 and
M6 build them.

**Top artists — hopping.** Five-minute windows advancing every minute, so each
event falls into five windows. Flink emits per-artist counts per window;
Grafana applies the top-10 ranking at query time, which keeps the streaming
query simple and lets the dashboard change N without redeploying a job.

```sql
INSERT INTO agg_top_artists
SELECT window_start, window_end, artist_name, COUNT(*)
FROM TABLE(HOP(TABLE plays, DESCRIPTOR(started_at),
                INTERVAL '1' MINUTE, INTERVAL '5' MINUTE))
GROUP BY window_start, window_end, artist_name;
```

**Listening sessions — session windows.** A session closes after 10 minutes
of inactivity for that listener. The window boundaries are defined by the data
itself, which has no batch equivalent.

```sql
INSERT INTO agg_sessions
SELECT listener_id, window_start, window_end,
       COUNT(*), COUNT(DISTINCT artist_name)
FROM TABLE(SESSION(TABLE plays PARTITION BY listener_id,
                   DESCRIPTOR(started_at), INTERVAL '10' MINUTE))
GROUP BY listener_id, window_start, window_end;
```

Session windows only emit on close, so with a 10-minute gap the dashboard panel
lags reality by at least that much. This is inherent to the window type, not a
defect; the panel is labelled to say so. Raising `SIM_SPEED` compresses the wait
during development.

### Raw event passthrough

A fourth statement mirrors every event into `raw_plays` unchanged. This takes
over the job the throwaway M2 consumer was doing, so the "now playing" and
event-time-skew panels keep working after that consumer is retired in M4.

```sql
INSERT INTO raw_plays
SELECT event_id, listener_id, is_synthetic, track_id, track_name,
       artist_name, album_name, duration_ms, started_at, observed_at
FROM plays;
```

It is an upsert on `event_id`, so replaying the topic is safe.

### Checkpointing

Every 30 seconds to a local Docker volume, with a fixed-delay restart strategy.
Enough to demonstrate recovery; not tuned for production.

## Postgres schema

```sql
CREATE TABLE agg_plays_per_minute (
  window_start TIMESTAMPTZ NOT NULL,
  window_end   TIMESTAMPTZ NOT NULL,
  listener_id  TEXT        NOT NULL,
  play_count   BIGINT      NOT NULL,
  PRIMARY KEY (window_start, listener_id)
);

CREATE TABLE agg_top_artists (
  window_start TIMESTAMPTZ NOT NULL,
  window_end   TIMESTAMPTZ NOT NULL,
  artist_name  TEXT        NOT NULL,
  play_count   BIGINT      NOT NULL,
  PRIMARY KEY (window_start, artist_name)
);

CREATE TABLE agg_sessions (
  listener_id      TEXT        NOT NULL,
  session_start    TIMESTAMPTZ NOT NULL,
  session_end      TIMESTAMPTZ NOT NULL,
  play_count       BIGINT      NOT NULL,
  distinct_artists BIGINT      NOT NULL,
  PRIMARY KEY (listener_id, session_start)
);

-- Written by the throwaway M2 consumer, then by the Flink passthrough
-- statement from M4 onwards. The raw event log behind the "now playing"
-- and event-time-skew panels.
CREATE TABLE raw_plays (
  event_id     TEXT PRIMARY KEY,
  listener_id  TEXT        NOT NULL,
  is_synthetic BOOLEAN     NOT NULL,
  track_id     TEXT        NOT NULL,
  track_name   TEXT        NOT NULL,
  artist_name  TEXT        NOT NULL,
  album_name   TEXT,
  duration_ms  INT,
  started_at   TIMESTAMPTZ NOT NULL,
  observed_at  TIMESTAMPTZ NOT NULL
);
```

Every table has a primary key, and every sink writes in upsert mode. That is
what makes the idempotent `event_id` strategy work end to end.

## Dashboard

A single Grafana dashboard, provisioned from JSON in git, refreshing every 5
seconds:

1. **Plays per minute** — time series, stacked by listener
2. **Top 10 artists, last 5 minutes** — bar gauge, ranked at query time
3. **Now playing** — table of the most recent play per listener
4. **Active sessions** — table of recent closed sessions with play count and
   artist variety
5. **Consumer lag** — time series, to watch backpressure build and drain
6. **Event-time skew** — `observed_at - started_at` distribution

Panels 5 and 6 exist for the learning objectives rather than for the data.

## Build milestones

Each milestone ends with a running system and a stated way to verify it.

### M0 — Kafka only

Docker Compose with Kafka in KRaft mode. Create the `plays` topic. Produce and
consume by hand with the console tools. Verify connector jar coordinates for
Flink 1.20.

*Acceptance:* a message typed into the console producer appears in the console
consumer; `kafka-topics --describe` shows 3 partitions.

### M1 — Simulator produces

`events/schema.py`, `producer/kafka_sink.py`, `simulator/`.

*Acceptance:* the console consumer shows well-formed JSON events; messages for
a given `listener_id` all land on the same partition.

### M2 — First heartbeat: Python consumer → Postgres → Grafana

A deliberately throwaway consumer using `confluent-kafka` directly, writing to
`raw_plays`. Postgres and Grafana added to Compose.

*Acceptance:* a Grafana panel shows a rising play count.

Three exercises belong here, because they are unavailable once Flink hides the
consumer:

- Run two copies of the consumer in one group; watch partitions rebalance
- Stop the consumer, let lag build, restart, watch it drain
- Reset the group offset to 0 and replay the entire topic

### M3 — Real Spotify poller

OAuth setup, then `transition.py` built test-first, then the loop.

*Acceptance:* play a song on Spotify; within 10 seconds it appears in
`raw_plays` exactly once. Let it play through and confirm no duplicate rows.

### M4 — Flink replaces the consumer: tumbling window

The Flink image, the source DDL, the plays-per-minute job, and the raw
passthrough statement. The Python consumer is then retired from the Compose
stack, with Flink taking over the `raw_plays` write it was responsible for.

*Acceptance:* `agg_plays_per_minute` fills; the Flink Web UI shows the
watermark advancing; integration test on the fixture event set passes.

### M5 — Hopping window, top artists

*Acceptance:* the bar gauge ranks artists and shifts as the window slides; a
single event is verifiably counted in five consecutive windows.

### M6 — Session windows

*Acceptance:* a burst of plays followed by a 10-minute gap produces exactly one
row in `agg_sessions` with the correct count. Includes the re-key-by-`track_id`
exercise demonstrating why the partition key matters.

### M7 — Polish

Dashboard layout, lag and skew panels, README with an architecture diagram and
a one-command start, late-event handling verified via `SIM_LATE_EVENT_RATE`.

*Acceptance:* `docker compose up` from a clean clone reaches a populated
dashboard, with the Spotify credential step being the only manual action.

## Error handling

| Failure | Response |
|---|---|
| Access token expires | Proactive refresh at 55 minutes |
| `429` rate limited | Honour `Retry-After`; 6 req/min against a ~180 ceiling |
| `204 No Content` | Not an error; nothing is playing, state retained |
| Network failure | Exponential backoff with jitter; the loop never exits |
| Broker unavailable | `acks=all`, producer buffers, delivery callbacks log failures |
| Poller restart | Deterministic `event_id` makes the re-emitted play a no-op upsert |
| Flink job failure | 30s checkpoints, fixed-delay restart |
| Postgres unavailable | JDBC sink retries; Flink backpressures into Kafka, which is durable |
| Late events | Dropped past the watermark, but counted and surfaced on a panel |

The general stance: Kafka's retention is the safety net. Any downstream
component can be stopped, fixed, and restarted, and it will catch up from its
committed offset. Losing data requires losing the broker's volume.

## Testing

**Tier 1 — unit, test-driven.** `poller/transition.py`. Every row of the
transition table becomes a test, written before the implementation. Pure
function, no mocks, no clock, sub-second to run. This is the one component
where TDD is applied strictly.

**Tier 2 — unit.** Schema round-trip; `event_id` determinism, including that a
restart mid-play reproduces the same ID.

**Tier 3 — integration.** The simulator's `fixture` mode emits a fixed set of
events with hardcoded timestamps. The test runs them through the live Compose
stack and asserts exact expected rows in each `agg_` table — for example, that
12 events spread across 3 minutes produce specific tumbling-window counts, and
that one event appears in exactly 5 hopping windows.

This is how the Flink SQL is verified. Without it, "the chart looks plausible"
is the only available check, which is not a check.

Spotify API calls are tested against recorded responses. No test contacts the
live API.

## Observability

- **Flink Web UI** on `:8081` — per-operator watermarks, records in and out,
  checkpoint history, backpressure
- **Consumer lag** — `kafka-consumer-groups --describe`, and a Grafana panel
- **Event-time skew** — charted from `raw_plays`
- **Late event counter** — surfaced from the Flink job

## Future extensions

Deliberately deferred, listed so the design does not have to accommodate them
now: Avro plus Schema Registry; a second sink such as Elasticsearch for
free-text search over play history; the same aggregation reimplemented in Spark
Structured Streaming for comparison; skip detection via stateful processing;
multi-user OAuth onboarding.
