# Keys

Document the different Keys in RisingWave.

## Stream Key

The key which can identify records in the RisingWave stream.

For example, given the following stream chunk, where stream key is `k1, k2`:
```text
| op | k1 | k2 | v1 | v2 |
|----|----|----|----|----|
| -  | 1  | 2  | 1  | 1  |
| +  | 1  | 2  | 3  | 4  |
| +  | 0  | 1  | 2  | 3  |
```

We can tell that the record corresponding to the key `(1, 2)`
has been updated from `(1, 2, 1, 1)` to `(1, 2, 3, 4)`.

The record corresponding to key `(0, 1)` has been inserted with `(0, 1, 2, 3)`.

It may not be the minimal set of columns required to identify a record,
for instance `group key` could be part of the stream key, to specify the distribution of records.

## Primary Key (Storage)

This discusses the internal primary key (pk) which we often see in streaming operators.
It is different from the primary key in SQL.

A more appropriate name for this would be `Storage Primary Key`.

Besides uniquely identifying a record in storage, this key may also be used
to provide ordering properties.

Let's use the following query as an example:

```sql
create table t1(id bigint primary key, i bigint);
create materialized view mv1 as select id, i from t1 order by i, id;
```

`mv1` has the following plan:
```text
 StreamMaterialize {
   columns: [id, i],
   stream_key: [id],
   pk_columns: [i, id], -- notice the pk_columns
   pk_conflict: NoCheck
 }
 └─StreamTableScan { table: t1, columns: [id, i] }
```

You can see that the `pk_columns` are `[i, id]`, although the upstream SQL primary key is just `id`.
In the storage layer, key-value pairs are sorted by their keys.

Because the materialized view contains an `order by i, id`,
the storage primary key is `[i, id]` to ensure they are ordered in storage.
Importantly, `i` will be a prefix of the key.

Then when iterating over the keys from storage, the records are returned in the correct order per partition.

When the update stream comes, we can just use `id` to identify the records that need to be updated.
We can get the whole record corresponding to the `id` and get the `i` from there.
Then we can use that to update the materialized state accordingly.
