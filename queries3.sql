create table t1 (v1 int, cut_off_date timestamp);
create table t2 (v1 int, cut_off_date timestamp);
create table t3 (v1 int, cut_off_date timestamp);
EXPLAIN create materialized view mv1 as
    select t1.v1 v1, t1.cut_off_date cut_off_date FROM t1 join t2 on t1.v1 = t2.v1 join t3 on t2.v1 = t3.v1
    where
            (
                case t1.cut_off_date
                when NULL THEN '4096-10-24 00:00:00'
                else t1.cut_off_date
                end
            )::timestamp with time zone >= now() and
            (
                case t2.cut_off_date
                when NULL THEN '4096-10-24 00:00:00'
                else t2.cut_off_date
                end
            )::timestamp with time zone >= now() and
            (
                case t3.cut_off_date
                when NULL THEN '4096-10-24 00:00:00'
                else t3.cut_off_date
                end
            )::timestamp with time zone >= now();

create materialized view mv1 as
    select t1.v1 v1, t1.cut_off_date cut_off_date FROM t1 join t2 on t1.v1 = t2.v1 join t3 on t2.v1 = t3.v1
    where
            (
                case t1.cut_off_date
                when NULL THEN '4096-10-24 00:00:00'
                else t1.cut_off_date
                end
            )::timestamp with time zone >= now() and
            (
                case t2.cut_off_date
                when NULL THEN '4096-10-24 00:00:00'
                else t2.cut_off_date
                end
            )::timestamp with time zone >= now() and
            (
                case t3.cut_off_date
                when NULL THEN '4096-10-24 00:00:00'
                else t3.cut_off_date
                end
            )::timestamp with time zone >= now();