-- Every event into raw_plays: the job the M2 consumer did. An upsert on
-- event_id, so replaying the topic rewrites rows with the values they already
-- have. Unchanged except the two timestamps, cast for the JDBC sink -- see the
-- raw_plays declaration in init.sql.
SET 'pipeline.name' = 'raw-passthrough';

-- The hint gives this job its own Kafka group. Flink does not read through
-- the group -- it assigns partitions itself and keeps offsets in checkpoints
-- -- but it commits offsets there on each checkpoint, so
-- kafka-consumer-groups can show this job's progress apart from others'.
INSERT INTO raw_plays
SELECT event_id, listener_id, is_synthetic, track_id, track_name,
       artist_name, album_name, duration_ms,
       CAST(started_at AS TIMESTAMP(3)), CAST(observed_at AS TIMESTAMP(3))
FROM plays /*+ OPTIONS('properties.group.id' = 'flink-raw-passthrough') */;
