-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q3_no_condition
AS
SELECT P.name,
       P.city,
       P.state,
       A.id
FROM auction AS A
         INNER JOIN person AS P on A.seller = P.id
WITH ( connector = 'blackhole', type = 'append-only');
