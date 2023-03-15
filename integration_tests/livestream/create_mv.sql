CREATE MATERIALIZED VIEW live_video_qos_10min AS
SELECT
    window_start AS report_ts,
    room_id,
    SUM(video_total_freeze_duration) AS video_total_freeze_duration,
    AVG(video_lost_pps) as video_lost_pps,
    AVG(video_rtt) as video_rtt
FROM
    TUMBLE(
        live_stream_metrics,
        report_timestamp,
        INTERVAL '10' MINUTE
    )
GROUP BY
    window_start,
    room_id;

--
--
-- -- Unsupported yet.
-- CREATE MATERIALIZED VIEW blocked_user_ratio_10min AS
-- SELECT
--     window_start AS report_ts,
--     (
--         COUNT() FILTER (
--             WHERE
--                 video_total_freeze_duration > 0
--         ) / COUNT(DISTINCT user_id) :: DOUBLE PRECISION
--     ) AS blocked_user_ratio,
-- FROM
--     TUMBLE(
--         live_stream_metrics,
--         report_timestamp,
--         INTERVAL '10' MINUTE
--     )
-- GROUP BY
--     window_start,
--     room_id;
--
--
--
-- A real-time dashboard of the total UV.
CREATE MATERIALIZED VIEW total_user_visit_1min AS
SELECT
    window_start AS report_ts,
    COUNT(DISTINCT user_id) as uv
FROM
    TUMBLE(
        live_stream_metrics,
        report_timestamp,
        INTERVAL '1' MINUTE
    )
GROUP BY
    window_start;

CREATE MATERIALIZED VIEW room_user_visit_1min AS
SELECT
    window_start AS report_ts,
    COUNT(DISTINCT user_id) as uv,
    room_id
FROM
    TUMBLE(
        live_stream_metrics,
        report_timestamp,
        INTERVAL '1' MINUTE
    )
GROUP BY
    window_start,
    room_id;