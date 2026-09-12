-- Runs once, on an empty data volume, via /docker-entrypoint-initdb.d.
-- All four tables are created here even though only raw_plays is written
-- before M4: this script does not re-run, so adding tables later would mean
-- destroying the volume or introducing a migration step.
--
-- Every table has a primary key and every sink upserts onto it. That is what
-- makes the deterministic event_id strategy work end to end -- replaying the
-- topic must not create duplicate rows.

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

-- started_at drives every time-bucketed panel, and both panels that read
-- raw_plays scan a trailing time range rather than the whole table.
CREATE INDEX raw_plays_started_at_idx ON raw_plays (started_at DESC);
CREATE INDEX raw_plays_listener_started_idx ON raw_plays (listener_id, started_at DESC);

-- Filled by Flink from M4. Empty until then.
CREATE TABLE agg_plays_per_minute (
  window_start TIMESTAMPTZ NOT NULL,
  window_end   TIMESTAMPTZ NOT NULL,
  listener_id  TEXT        NOT NULL,
  play_count   BIGINT      NOT NULL,
  PRIMARY KEY (window_start, listener_id)
);

-- Filled by Flink from M5. Empty until then.
CREATE TABLE agg_top_artists (
  window_start TIMESTAMPTZ NOT NULL,
  window_end   TIMESTAMPTZ NOT NULL,
  artist_name  TEXT        NOT NULL,
  play_count   BIGINT      NOT NULL,
  PRIMARY KEY (window_start, artist_name)
);

-- Filled by Flink from M6. Empty until then.
CREATE TABLE agg_sessions (
  listener_id      TEXT        NOT NULL,
  session_start    TIMESTAMPTZ NOT NULL,
  session_end      TIMESTAMPTZ NOT NULL,
  play_count       BIGINT      NOT NULL,
  distinct_artists BIGINT      NOT NULL,
  PRIMARY KEY (listener_id, session_start)
);
