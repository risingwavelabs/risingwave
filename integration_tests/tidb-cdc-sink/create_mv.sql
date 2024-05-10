--
-- Find the top10 hotest hashtags.
--
CREATE MATERIALIZED VIEW hot_hashtags13 AS WITH tags AS (
    SELECT
        unnest(regexp_matches(tweet.text, '#\w+', 'g')) AS hashtag,
        tweet.created_at AT TIME ZONE 'UTC' AS created_at
    FROM
        tweet
        JOIN user ON tweet.author_id = user.id
)
SELECT
    hashtag,
    COUNT(*) AS hashtag_occurrences,
    window_start,
    window_end
FROM
    HOP(
        tags,
        created_at,
        INTERVAL '5 seconds',
        INTERVAL '30 seconds'
    )
GROUP BY
    hashtag,
    window_start,
    window_end
ORDER BY
    window_start DESC,
    hashtag_occurrences DESC
LIMIT
    10;

CREATE MATERIALIZED VIEW datatype_c0_boolean AS
SELECT
    c0_boolean,
    COUNT(*) as c0_count
FROM
    datatype
GROUP BY
    c0_boolean;