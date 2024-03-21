-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile

CREATE FUNCTION count_char(s varchar, c varchar) RETURNS int LANGUAGE javascript AS $$
    var count = 0;
    for (var cc of s) {
        if (cc === c) {
            count++;
        }
    }
    return count;
$$;

CREATE SINK nexmark_q14 AS
SELECT auction,
       bidder,
       0.908 * price as price,
       CASE
           WHEN
                       extract(hour from date_time) >= 8 AND
                       extract(hour from date_time) <= 18
               THEN 'dayTime'
           WHEN
                       extract(hour from date_time) <= 6 OR
                       extract(hour from date_time) >= 20
               THEN 'nightTime'
           ELSE 'otherTime'
           END       AS bidTimeType,
       date_time,
       count_char(extra, 'c') AS c_counts
FROM bid
WHERE 0.908 * price > 1000000
  AND 0.908 * price < 50000000
WITH ( connector = 'blackhole', type = 'append-only');
