--
-- Find the top10 hottest hashtags.
--
CREATE MATERIALIZED VIEW hot_hashtags AS WITH tags AS (
    SELECT
        unnest(regexp_matches(tweet.text, '#\w+', 'g')) AS hashtag,
        tweet.created_at AT TIME ZONE 'UTC' AS created_at
    FROM
        tweet JOIN user
    ON
        tweet.author_id = user.id
)
SELECT
    hashtag,
    COUNT(*) AS hashtag_occurrences,
    window_start
FROM
    TUMBLE(tags, created_at, INTERVAL '5 minute')
GROUP BY
    hashtag,
    window_start
ORDER BY
    hashtag_occurrences;

CREATE MATERIALIZED VIEW datatype_c0_boolean AS
SELECT
    c0_boolean,
    COUNT(*) as c0_count
FROM
    datatype
GROUP BY
    c0_boolean;
