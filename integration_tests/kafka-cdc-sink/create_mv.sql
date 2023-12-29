CREATE MATERIALIZED VIEW counts as select id, sum(num) from metrics group by id;
