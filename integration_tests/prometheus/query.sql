select
    *
from
    metric_avg_30s
where
    metric_name = 'object_store_read_bytes'
order by
    metric_time;