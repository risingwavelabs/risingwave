--
-- Find the influencers
--
CREATE MATERIALIZED VIEW influencer_tweets AS
SELECT
    (author).id as author_id,
    (data).text as tweet
FROM
    twitter
WHERE
    (author).followers > 5000
    AND (data).lang = 'English';

--
-- Find the top10 hotest hashtags.
--
CREATE MATERIALIZED VIEW hot_hashtags AS WITH tags AS (
    SELECT
        unnest(regexp_matches((data).text, '#\w+', 'g')) AS hashtag,
        (data).created_at AS created_at
    FROM
        twitter
)
SELECT
    hashtag,
    COUNT(*) AS hashtag_occurrences,
    window_start
FROM
    TUMBLE(tags, created_at, INTERVAL '1 day')
GROUP BY
    hashtag,
    window_start
ORDER BY
    hashtag_occurrences;
