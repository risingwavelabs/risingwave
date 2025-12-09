-- Query first stream
SELECT
    *
FROM
    live_stream_metrics
LIMIT
    10;

-- Query second stream (tests multi-stream consumption with shared client)
SELECT
    *
FROM
    live_stream_metrics_2
LIMIT
    10;