CREATE MATERIALIZED VIEW city_population AS
SELECT
    city,
    COUNT(*) as population
FROM
    person
GROUP BY
    city;

CREATE MATERIALIZED VIEW nexmark_q8 AS
SELECT
    P.id,
    P.name,
    A.starttime
FROM
    person as P
    JOIN (
        SELECT
            seller,
            window_start AS starttime,
            window_end AS endtime
        FROM
            TUMBLE(auction, date_time, INTERVAL '10' SECOND)
        GROUP BY
            seller,
            window_start,
            window_end
    ) A ON P.id = A.seller;