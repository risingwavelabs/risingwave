create materialized view metric_avg_30s as
select
    name as metric_name,
    window_start as metric_time,
    avg(value :: decimal) as metric_value
from
    tumble(
        prometheus,
        timestamp,
        interval '30 s'
    )
group by
    name,
    window_start
order by
    window_start;