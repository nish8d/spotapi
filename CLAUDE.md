# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Current state

**M0-M4 complete. Next is M5, the hopping window (top artists).**

`docker compose up -d --build` from cold brings up Kafka, Postgres, the
simulator, the real Spotify poller, a Flink session cluster and Grafana, and
fills a live dashboard at `http://localhost:3000/d/spot-live` (no login). The
Flink Web UI is at `http://localhost:8081`. Flink runs two jobs: a passthrough
into `raw_plays`, and a one-minute tumbling window into
`agg_plays_per_minute`, which the plays-per-minute panel reads. A *Source*
dropdown splits real plays from synthetic ones.

Two test commands:

- `.venv/bin/pytest -q` runs 96 unit tests against fakes and recorded
  responses. No test contacts a broker, a database, or Spotify.
- `.venv/bin/pytest -m integration` runs the simulator's fixture through an
  isolated Compose project, `spot-it`, and asserts exact rows in Postgres.
  It takes about a minute and never touches the dev stack.

What exists: `events/schema.py`, `producer/kafka_sink.py`, `simulator/`,
`poller/`, `flink/` (image, `submit.sh`, `sql/init.sql`, `sql/jobs/`),
`postgres/init.sql`, `grafana/`. `agg_top_artists` and `agg_sessions` exist
and stay empty until M5 and M6.

**A fresh clone needs one manual step before `up`**, or Compose refuses to
start: `cp .env.example .env`, fill in the Spotify client id and secret, then
`set -a; . ./.env; set +a; .venv/bin/python -m poller.spotify_auth` on the
host to write `.spotify_token.json`. That file must exist before the first
`up` — if it does not, Docker creates a directory in its place for the bind
mount. After that the poller refreshes the token itself, indefinitely.

Things to know before working here:

- **Host port 5432 is taken by an unrelated local Postgres**, so Compose maps
  Postgres to `55432:5432`. Containers still use `postgres:5432`.
- **A Flink job is a file.** `flink/sql/jobs/<name>.sql` sets `pipeline.name`
  to `<name>`, and the one-shot `flink-submit` service submits every job
  whose name is not already on the cluster, on every `up`. `init.sql`
  declares the source and every sink once; each job runs as
  `sql-client.sh -i init.sql -f jobs/<name>.sql`. Adding M5 means adding a
  file and a sink, not editing a running job.
- **Every windowed count is `COUNT(DISTINCT event_id)`, never `COUNT(*)`.**
  The topic is at-least-once. Upsert absorbs a duplicate only in a table
  keyed by `event_id`; a table keyed by window has to dedupe in the query.
- **The Flink services have no `image:` name.** A fixed tag is shared across
  Compose projects, so the integration test's `--build` would move it and
  recreate the dev JobManager, which forgets its jobs without HA.
- **Flink commits offsets to `flink-<job>` Kafka groups only for
  visibility.** It does not read through the group. Its real offsets live in
  its checkpoints, so `kafka-consumer-groups` shows no members even while it
  runs.

The authoritative design is
`docs/superpowers/specs/2026-09-11-spotify-streaming-pipeline-design.md`.
Read it before making architectural decisions; it records what was considered
and rejected, not just what was chosen.

## What this project is

A learning vehicle, first and foremost. The owner is new to Kafka, stream
processing, and containers, and the goal is to understand them — the live
dashboard is evidence the understanding is real, not the deliverable.

This has a concrete consequence for how you should work here: **some
inefficiency is deliberate.** Milestone M2 built a plain Python Kafka consumer
that M4 deleted and replaced with Flink. That was not technical debt, and
similar detours later should not be "cleaned up" or skipped. It existed so the
owner saw consumer groups, offsets, and rebalancing directly before a framework
hid them.

Explain streaming concepts as they come up rather than assuming background
knowledge. Prefer showing the mechanism over asserting the conclusion.

## Architecture

```
spotify-poller ─┐
                ├─► Kafka topic `plays` ─► Flink SQL ─► Postgres ─► Grafana
simulator ──────┘    (3 partitions,        (1 job per    (agg_*)
                      key=listener_id)       statement)
```

Everything runs locally under Docker Compose. Cost is $0 by design; a Spotify
developer app is free and there is no cloud component.

Two producers write the same schema to the same topic: a real poller on the
owner's Spotify account, and a simulator generating synthetic listeners for
volume. Synthetic events carry `is_synthetic: true` so they can be filtered
apart in any query.

Flink runs four SQL statements against one source table, each as its own
job: three windowed aggregations (tumbling plays-per-minute, hopping
top-artists, session windows) plus a passthrough that mirrors raw events into
`raw_plays`. M4 built the tumbling window and the passthrough.

### Invariants that are easy to break

These four decisions are load-bearing. Changing any of them breaks correctness
in ways that are not obvious from a local test:

1. **Partition key is `listener_id`.** Kafka orders only within a partition.
   Session windows and any per-listener logic depend on one listener's plays
   staying ordered on one partition. Keying by `track_id` scatters them and
   corrupts sessions silently.

2. **`event_id` is deterministic:** `sha1(listener_id | track_id | started_at
   rounded to 5s)`. The poller holds state in memory, so a restart re-emits the
   current track. The deterministic ID plus upsert-on-primary-key makes that
   duplicate a no-op. Never switch this to a random UUID.

3. **Every sink table has a primary key and writes in upsert mode.** This is
   the other half of invariant 2. A sink without a PK turns replay into
   duplicate rows.

4. **`started_at` is derived, not observed:** `observed_at - progress_ms`. It
   is the event-time column driving all watermarks. `observed_at` is retained
   only so poll drift can be charted; do not use it for windowing.

### The Spotify API is a state endpoint, not an event stream

`/me/player/currently-playing` returns what is playing *right now*. Polling
every 10s means a 4-minute track is returned ~24 times. Converting that to one
event per play is the job of `poller/transition.py`, which is a **pure
function** — no network, no Kafka, no clock:

```
transition(previous_state, current_api_response) -> (Optional[PlayEvent], new_state)
```

It is isolated precisely so it can be tested exhaustively. The spec has the
full state table, including the two cases most likely to be got wrong: state
must be retained across pause and across `204 No Content` (otherwise resuming
double-counts), and a backwards jump in `progress_ms` greater than 10s is a
replay that must emit a new event.

**Build this one test-first.** It is the single component where strict TDD
applies; the rest of the project is verified by integration tests.

## Testing approach

- **Unit (TDD):** `poller/transition.py` — every row of the spec's state table.
- **Unit:** schema round-trip, `event_id` determinism across a simulated restart.
- **Integration:** the simulator's `SIM_MODE=fixture` emits a deterministic
  event set with hardcoded timestamps; tests assert *exact* row counts in the
  `agg_` tables. This is the only real check on the Flink SQL — a plausible
  looking chart is not a check.

No test contacts the live Spotify API; use recorded responses.

## Simulator controls

Environment variables on the simulator service, used to exercise specific
behaviour rather than just to make data:

| Var | Purpose |
|---|---|
| `SIM_LISTENERS` | Virtual listener count (default 20) |
| `SIM_SPEED` | Time compression. Raise it to build consumer lag on demand |
| `SIM_LATE_EVENT_RATE` | Fraction of events emitted behind the watermark, to exercise late-data handling |
| `SIM_MODE` | `live` (randomised) or `fixture` (deterministic, for tests) |

## Milestones

Build order is sequential and each milestone ends with a running system. The
spec has acceptance criteria for each.

`M0` Kafka alone · `M1` simulator produces · `M2` Python consumer → Postgres →
Grafana (first working dashboard) · `M3` real poller · `M4` Flink tumbling
window · `M5` hopping window · `M6` session windows · `M7` polish

Do not jump ahead. The order exists so each new concept lands on a system that
already works.

## Conventions

- Secrets live in `.env` and `.spotify_token.json`, both gitignored. The
  Spotify OAuth scope needed is `user-read-currently-playing`.
- `events/schema.py` is the single source of truth for the event shape. Both
  producers and the tests import it rather than restating it.
- Grafana dashboards and datasources are provisioned from JSON in
  `grafana/provisioning/`, checked into git — not configured through the UI.
- Flink connector jars are baked into a custom image layer in
  `flink/Dockerfile`, not downloaded at runtime.
