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
