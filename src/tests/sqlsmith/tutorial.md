# Tutorials

## Adding new Query Clause - Emit On Window Close (EOWC)

### Informal Specification of EOWC testing requirements

First, we identify some examples from `e2e_test`:

```sql
create table t (
    tm timestamp,
    foo int,
    watermark for tm as tm - interval '5 minutes'
) append only;

statement ok
create materialized view mv
emit on window close
as
select
    window_start, max(foo)
from tumble(t, tm, interval '1 hour')
group by window_start;

statement ok
create materialized view mv1
emit on window close
as
select
    tm, foo, bar,
    lag(foo, 2) over (partition by bar order by tm),
        max(foo) over (partition by bar order by tm rows between 1 preceding and 1 following),
        sum(foo) over (partition by bar order by tm rows 2 preceding exclude current row)
from t;
```

We know that all executors should support it (directly or indirectly via `Sort`).
We also know that a table `t` needs to have `watermark` in order for us to do 
`EOWC`.

We also note that due to implementation limitations of `EOWC`, `EOWC` is supported for 1 column,
so we will take note of this in the implementation.

### Drafting

Let's list the changes required:
1. Update `table` to store watermark column index.
2. We need to provide a branch which generates `EOWC`, if there exists some `watermark` column we can use.