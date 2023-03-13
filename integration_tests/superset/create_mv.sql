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