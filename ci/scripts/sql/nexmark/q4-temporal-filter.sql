-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q4_temporal_filter
AS
SELECT Q.category,
       AVG(Q.final) as avg
FROM (SELECT MAX(B.price) AS final,
             A.category
      FROM auction A,
           bid_filtered B
      WHERE A.id = B.auction
        AND B.date_time BETWEEN A.date_time AND A.expires
      GROUP BY A.id, A.category) Q
GROUP BY Q.category
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
