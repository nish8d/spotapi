SET 'pipeline.name' = 'plays-per-minute';

-- An OPTIONS hint is not allowed inside TABLE(...), so this job's group id is
-- applied through a view. A SELECT * view keeps started_at's time attribute.
CREATE TEMPORARY VIEW plays_for_plays_per_minute AS
SELECT * FROM plays /*+ OPTIONS('properties.group.id' = 'flink-plays-per-minute') */;

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
