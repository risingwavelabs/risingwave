SET STREAMING_PARALLELISM=1;
create table t(v1 int);
create materialized view mv1 as select v1 from t;
insert into t values (1), (2), (3), (4);
flush;

select * from mv1;

-- Some weird bug... If I comment this out, it works, reproduces
-- 4 rows in i1. If I uncomment it, it doesn't work, and i1 has 2 rows.
explain create materialized view mv2 as select * from mv1;
create materialized view mv2 as select * from mv1;
select * from mv2;

explain create index i1 on mv1(v1);
create index i1 on mv1(v1);
select * from i1;

-- explain create index i2 on t(v1);
-- create index i2 on t(v1);
-- select * from i2;

-- explain create materialized view mv3 as select v1 from mv1 group by v1;
-- create materialized view mv3 as select v1 from mv1 group by v1;
-- select * from mv3;