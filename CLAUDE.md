# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Current state

**M0-M3 complete. Next is M4, the Flink tumbling window.**

`docker compose up -d --build` from cold brings up Kafka, Postgres, the
simulator, the real Spotify poller, the throwaway consumer and Grafana, and
fills a live dashboard at `http://localhost:3000/d/spot-live` (no login). A
*Source* dropdown splits real plays from synthetic ones, and a panel at the
top lists the real account's last ten plays. `.venv/bin/pytest -q` runs 105
unit tests against fakes and recorded responses — no test contacts a broker,
a database, or Spotify.

What exists: `events/schema.py`, `producer/kafka_sink.py`, `simulator/`,
`poller/`, `consumer/`, `postgres/init.sql`, `grafana/`. What does not:
`flink/sql/` (M4-M6); `flink/jars.txt` holds the verified coordinates. The
three `agg_` tables exist and are empty until Flink fills them.

**A fresh clone needs one manual step before `up`**, or Compose refuses to
start: `cp .env.example .env`, fill in the Spotify client id and secret, then
`set -a; . ./.env; set +a; .venv/bin/python -m poller.spotify_auth` on the
host to write `.spotify_token.json`. That file must exist before the first
`up` — if it does not, Docker creates a directory in its place for the bind
mount. After that the poller refreshes the token itself, indefinitely.

Things to know before working here:

- **Host port 5432 is taken by an unrelated local Postgres**, so Compose maps
  Postgres to `55432:5432`. Containers still use `postgres:5432`.
- **`consumer/` is scheduled for deletion in M4.** Do not invest in it. See
  the note on deliberate inefficiency below.

The authoritative design is
`docs/superpowers/specs/2026-09-11-spotify-streaming-pipeline-design.md`.
Read it before making architectural decisions; it records what was considered
and rejected, not just what was chosen.

## What this project is

A learning vehicle, first and foremost. The owner is new to Kafka, stream
processing, and containers, and the goal is to understand them — the live
dashboard is evidence the understanding is real, not the deliverable.

This has a concrete consequence for how you should work here: **some
inefficiency is deliberate.** Milestone M2 builds a plain Python Kafka consumer
that M4 deletes and replaces with Flink. That is not technical debt and should
not be "cleaned up" or skipped. It exists so the owner sees consumer groups,
offsets, and rebalancing directly before a framework hides them.

Explain streaming concepts as they come up rather than assuming background
knowledge. Prefer showing the mechanism over asserting the conclusion.

## Architecture

```
spotify-poller ─┐
                ├─► Kafka topic `plays` ─► Flink SQL ─► Postgres ─► Grafana
simulator ──────┘    (3 partitions,        (4 stmts)     (agg_*)
                      key=listener_id)
```

Everything runs locally under Docker Compose. Cost is $0 by design; a Spotify
developer app is free and there is no cloud component.

Two producers write the same schema to the same topic: a real poller on the
owner's Spotify account, and a simulator generating synthetic listeners for
volume. Synthetic events carry `is_synthetic: true` so they can be filtered
apart in any query.

Flink runs four SQL statements against one source table: three windowed
aggregations (tumbling plays-per-minute, hopping top-artists, session windows)
plus a passthrough that mirrors raw events into `raw_plays`.

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
  producers, the M2 consumer, and the tests import it rather than restating it.
- Grafana dashboards and datasources are provisioned from JSON in
  `grafana/provisioning/`, checked into git — not configured through the UI.
- Flink connector jars are baked into a custom image layer in
  `flink/Dockerfile`, not downloaded at runtime.
