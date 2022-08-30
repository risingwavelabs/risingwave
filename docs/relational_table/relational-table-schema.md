# Relational Table Schema

We introduce the rough cell-based encoding format in [relational states](storing-state-using-relational-table.md#cell-based-encoding)

In this doc, we will take HashAgg with extreme state (`max`, `min`) or value state (`sum`, `count`) for example, and introduce a more detailed design for the internal table schema.

[Code](https://github.com/risingwavelabs/risingwave/blob/7f9ad2240712aa0cfe3edffb4535d43b42f32cc5/src/frontend/src/optimizer/plan_node/logical_agg.rs#L144)

## Table id
For all relational table states, the keyspace must start with `table_id`. This is a globally unique id allocated in meta. Meta is responsible for traversing the Plan Tree and calculating the total number of Relational Tables needed. For example, the Hash Join Operator needs 2, one for the left table and one for the right table. The number of tables needed for Agg depends on the number of agg calls.

## Value State (Sum, Count)
Query example:
```sql
select sum(v2), count(v3) from t group by v1 
```

This query will need to initiate 2 Relational Tables. The schema is `table_id/group_key/column_id`.

## Extreme State (Max, Min)
Query example:
```sql
select max(v2), min(v3) from t group by v1 
```

This query will need to initiate 2 Relational Tables. If the upstream is not append-only, the schema becomes `table_id/group_key/sort_key/upstrea_pk/column_id`. 

The order of `sort_key` depends on the agg call kind. For example, if it's `max()`, `sort_key` will order with `Ascending`. if it's `min()`, `sort_key` will order with `Descending`. 
The `upstream_pk` is also appended to ensure the uniqueness of the key.
This design allows the streaming executor not to read all the data from the storage when the cache fails, but only a part of it. The streaming executor will try to write all streaming data to storage, because there may be `update` or `delete` operations in the stream, it's impossible to always guarantee correct results without storing all data.

If `t` is created with append-only flag, the schema becomes `table_id/group_key/column_id`, which is the same for Value State. This is because in the append-only mode, there is no `update` or `delete` operation, so the cache will never miss. Therefore, we only need to write one value to the storage.



