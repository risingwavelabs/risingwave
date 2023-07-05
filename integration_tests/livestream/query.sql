--- TODO: we need now() for ad-hoc mode.
-- SELECT
--     *
-- FROM
--     thread_view_count
-- WHERE
--     window_time > (
--         '2022-7-22 18:43' :: TIMESTAMP - INTERVAL '1 day'
--     )
--     AND window_time < (
--         '2022-7-22 18:43' :: TIMESTAMP - INTERVAL '1 day' + INTERVAL '10 minutes'
--     )
--     AND target_id = 'thread83
SELECT
    *
FROM
    live_video_qos_10min
LIMIT
    10;