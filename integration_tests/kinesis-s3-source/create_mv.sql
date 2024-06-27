CREATE MATERIALIZED VIEW ad_ctr AS
SELECT
    ad_clicks.ad_id AS ad_id,
    ad_clicks.clicks_count :: NUMERIC / ad_impressions.impressions_count AS ctr
FROM
    (
        SELECT
            ad_impression.ad_id AS ad_id,
            COUNT(*) AS impressions_count
        FROM
            ad_impression
        GROUP BY
            ad_id
    ) AS ad_impressions
    JOIN (
        SELECT
            ai.ad_id,
            COUNT(*) AS clicks_count
        FROM
            ad_click AS ac
            LEFT JOIN ad_impression AS ai ON ac.bid_id = ai.bid_id
        GROUP BY
            ai.ad_id
    ) AS ad_clicks ON ad_impressions.ad_id = ad_clicks.ad_id;

CREATE MATERIALIZED VIEW ad_ctr_5min AS
SELECT
    ac.ad_id AS ad_id,
    ac.clicks_count :: NUMERIC / ai.impressions_count AS ctr,
    ai.window_end AS window_end
FROM
    (
        SELECT
            ad_id,
            COUNT(*) AS impressions_count,
            window_end
        FROM
            TUMBLE(
                ad_impression,
                impression_timestamp,
                INTERVAL '5' MINUTE
            )
        GROUP BY
            ad_id,
            window_end
    ) AS ai
    JOIN (
        SELECT
            ai.ad_id,
            COUNT(*) AS clicks_count,
            ai.window_end AS window_end
        FROM
            TUMBLE(ad_click, click_timestamp, INTERVAL '5' MINUTE) AS ac
            INNER JOIN TUMBLE(
                ad_impression,
                impression_timestamp,
                INTERVAL '5' MINUTE
            ) AS ai ON ai.bid_id = ac.bid_id
            AND ai.window_end = ac.window_end
        GROUP BY
            ai.ad_id,
            ai.window_end
    ) AS ac ON ai.ad_id = ac.ad_id
    AND ai.window_end = ac.window_end;