-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q5_rewrite AS
SELECT
  B.auction,
  B.num
FROM (
  SELECT
      auction,
      num,
      /*use rank here to express top-N with ties*/
      rank() over (partition by starttime order by num desc) as num_rank
  FROM (
      SELECT bid.auction, count(*) AS num, window_start AS starttime
      FROM HOP(bid, date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
      GROUP BY window_start, bid.auction
  )
) B
WHERE num_rank <= 1
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
