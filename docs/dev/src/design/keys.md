# Keys

Document the different Keys in RisingWave.

## Order Key

The order key is user-specified, in SQL. For example:

```sql
create materialized view m1 as select * from t1 order by i, id;
```

The order key is `i, id`, which is the order of the columns in the `order by` clause.

It is used to ensure locality of records in storage, and locality of record updates in streaming.
To ensure storage locality, when deriving the storage key, we will use the order key as a prefix.
To ensure streaming locality, we will use the order key as a prefix of the stream key.

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

## Distribution key

This key specifies the distribution of the data streams, as mentioned in [Consistent Hash - Streaming](../design/consistent-hash.md#streaming).
We want data to be distributed in a way that minimizes data skew, and maximizes data locality, for more efficient stateful processing.

We will use the user-specified `order by` clause to determine the distribution key of an MV.
For example, given the following query:
```sql
create materialized view mv1 as select id, i from t1 order by i, id;
```

The distribution key of `mv1` will be `[i, id]`, which is the same as the order key.

To ensure data consistency of updates (U-, U+), the distribution key must always be a prefix of the stream key.
This ensures that updates on the same key are not sent to different partitions.
Otherwise if they are exchanged back to the same partition,
the order of updates may be different from the order of the original stream.

## Primary Key

This discusses the internal primary key (pk) which we often see in streaming operators.
It is different from the user-specified primary key in SQL.

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
