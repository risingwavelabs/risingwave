# Storing State Using Relational Table

- [Storing State Using Relational Table](#storing-state-using-relational-table)
  - [Row-based Encoding](#row-based-encoding)
  - [Relational Table Layer](#relational-table-layer)
    - [Write Path](#write-path)
    - [Read Path](#read-path)

    

<!-- Created by https://github.com/ekalinin/github-markdown-toc -->

## Row-based Encoding

RisingWave adapts a relational data model. Relational tables, including tables and materialized views, consist of a list of named, strong-typed columns. All streaming executors store their data into a KV state store, which is backed by a service called Hummock. There are two choices to save a relational row into key-value pairs: cell-based format and row-based format. We choose row-based format because internal states always read and write the whole row, and don't need to partially update some fields in a row. Row-based encoding has better performance than cell-based encoding, which reduces the number of read and write kv pairs. 

We implement a relational table layer as the bridge between executors and KV state store, which provides the interfaces accessing KV data in relational semantics. As the executor state's encoding is very similar to a row-based table, each kind of state is stored as a row-based relational table first. In short, one row is stored as a key-value pair. For example, encoding of some stateful executors in row-based format is as follows:
| state | key | value |
| ------ | --------------------- | ------ |
| mv     | table_id \| sort key \| pk | materialized value |
| top n | table_id \| sort key \| pk| materialized value |
| join     | table_id \| join_key \| pk | materialized value |
| agg | table_id \| group_key | agg_value |

For the detailed schema, please check [doc](relational-table-schema.md)

<!-- Todo: link cconsistence hash doc and state table agg doc -->
## Relational Table Layer
[source code](https://github.com/risingwavelabs/risingwave/blob/4e66ca3d41435c64af26b5e0003258c4f7116822/src/storage/src/table/state_table.rs)

In this part, we will introduce how stateful executors interact with KV state store through the relational table layer.

Relational table layer consists of State Table, Mem Table and Storage Table. The State Table and MemTable is used in streaming mode, and Storage Table is used in batch mode. 

State Table provides the table operations by these APIs: `get_row`, `scan`, `insert_row`, `delete_row` and `update_row`, which are the read and write interfaces for streaming executors. The Mem Table is an in-memory buffer for caching table operations during one epoch. The Storage Table is read only, and will output the partial columns  upper level needs.


![Overview of Relational Table](../images/relational-table-layer/relational-table-01.svg)
### Write Path
To write into KV state store, executors first perform operations on State Table, and these operations will be cached in Mem Table. Once a barrier flows through one executor, executor will flush the cached operations into state store. At this moment, State Table will covert these operations into kv pairs and write to state store with specific epoch. 

For example, an executor performs `insert(a, b, c)` and `delete(d, e, f)` through the State Table APIs, Mem Table first caches these two operations in memory. After receiving new barrier, State Table converts these two operations into KV operations by row-based format, and writes these KV operations into state store (Hummock).

![write example](../images/relational-table-layer/relational-table-03.svg)
### Read Path
In streaming mode, executors should be able to read the latest written data, which means uncommitted data is visible. The data in Mem Table (memory) is fresher than that in shared storage (state store). State Table provides both point-get and scan to read from state store by merging data from Mem Table and Storage Table. 
#### Get
For example, let's assume that the first column is the pk of relational table, and the following operations are performed.
```
insert [1, 11, 111]
insert [2, 22, 222]
delete [2, 22, 222]
insert [3, 33, 333]

commit

insert [3, 3333, 3333]
```

After commit, a new record is inserted again. Then the `Get` results with corresponding pk are:
```
Get(pk = 1): [1, 11, 111]
Get(pk = 2):  None
Get(pk = 3): [3, 3333, 3333]
```

#### Scan
Scan on relational table is implemented by `StateTableIter`, which is a merge iterator of `MemTableIter` and `StorageIter`. If a pk exists in both KV state store (shared storage) and memory (MemTable), result of `MemTableIter` is returned. For example, in the  following figure, `StateTableIter` will generate `1->4->5->6` in order.

![Scan example](../images/relational-table-layer/relational-table-02.svg)
