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
  -- TIMESTAMP(3), not the source's TIMESTAMP_LTZ(3): the JDBC connector's
  -- planner accepts LTZ but its runtime converter does not ("Unsupported
  -- type: TIMESTAMP_LTZ(3)"). The job casts in the session zone, UTC, and
  -- Postgres stores the result as TIMESTAMPTZ in the JVM zone, also UTC.
  started_at   TIMESTAMP(3),
  observed_at  TIMESTAMP(3),
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/spot',
  'table-name' = 'raw_plays',
  'username' = 'spot',
  'password' = 'spot'
);

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
