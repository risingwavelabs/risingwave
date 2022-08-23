# Storing State Using Relational Table

- [Storing State Using Relational Table](#storing-state-using-relational-table)
  - [Cell-Based Encoding](#cell-based-encoding)
  - [Relational Table Layer](#relational-table-layer)
    - [Write Path](#write-path)
    - [Read Path](#read-path)

    

<!-- Created by https://github.com/ekalinin/github-markdown-toc -->

## Cell-based Encoding

RisingWave adapts a relational data model. Relational tables, including tables and materialized views, consist of a list of named, strong-typed columns. All streaming executors store their data into a KV state store, which is backed by a service called Hummock. There are two choices to save a relational row into key-value pairs: cell-based format and row-based format. We choose cell-based format for these two reasons:

1. **Reduce the Overhead of DMLs**.
Cell-based encoding can significantly reduce write amplification since we can partially update some fields in a row. Considering the following streaming aggregation query: 
```
select sum(a), count(b), min(c), string_agg(d order by e) from t
```
- `sum(a)`, `count(b)` are trivial.
- `min(c)` is a little difficult. To get the next minimum once the current minimum is deleted, we have to keep all the `c` values, which would be a long list in the row-based format and it performs badly for inserts or deletes.
- `string_agg(d order by e)` is more difficult. we must keep all the `d` as well as their sort key `e`, and support random inserts or deletes. Again, a flatten list saved in a row would be a bad choice.


2. **Better support Semi-structured Data**. RisingWave may support semi-structured data in the future. Semi-structured data consists of nested structures and arrays, which are hard to flatten into row format, but much more simple under cell-based format, simply replace the `column id` to the JSONPath to such field.

We implement a relational table layer as the bridge between stateful executors and KV state store, which provides the interfaces accessing KV data in relational semantics. As the executor state's key encoding is very similar to a cell-based table, each kind of state is stored as a cell-based relational table first. In short, a cell instead of a whole row is stored as a key-value pair. For example, encoding of some stateful executors in cell-based format is as follows:
| state | key | value |
| ------ | --------------------- | ------ |
| mv     | table_id \| sort key \| pk \| col_id| materialized value |
| top n | table_id \| sort key \| pk \| col_id | materialized value |
| join     | table_id \| join_key \| pk \| col_id| materialized value |
| agg | table_id \| group_key \| col_id| agg_value |

For the detailed schema, please check [doc](relational-table-schema.md)

<!-- Todo: link cconsistence hash doc and state table agg doc -->
## Relational Table Layer
[source code](https://github.com/singularity-data/risingwave/blob/main/src/storage/src/table/state_table.rs)

In this part, we will introduce how stateful executors interact with KV state store through the relational table layer.

Relational table layer consists of State Table, Mem Table and Storage Table. The State Table provides the table operations by these APIs: `get_row`, `scan`, `insert_row`, `delete_row` and `update_row`, which are the read and write interfaces for executors. The Mem Table is an in-memory buffer for caching table operations during one epoch, and the Storage Table is responsible for writing row operations into kv pairs, which performs serialization and deserialization between cell-based encoding and KV encoding.


![Overview of Relational Table](../images/relational-table-layer/relational-table-01.svg)
### Write Path
To write into KV state store, executors first perform operations on State Table, and these operations will be cached in Mem Table. Once a barrier flows through one executor, executor will flush the cached operations into state store. At this moment, Storage Table will covert these operations into kv pairs and write to state store with specific epoch. 

For example, an executor performs `insert(a, b, c)` and `delete(d, e, f)` through the State Table APIs, Mem Table first caches these two operations in memory. After receiving new barrier, Cell Based Table converts these two operations into KV operations by cell-based format, and write these KV operations into state store (Hummock).

![write example](../images/relational-table-layer/relational-table-03.svg)
### Read Path
Executors should be able to read the just written data, which means uncommitted data is visible. The data in Mem Table (memory) is fresher than that in shared storage (state store). State Table provides both point-get and scan to read from state store by merging data from Mem Table and Storage Table. 
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
Scan on relational table is implemented by `StateTableIter`, which is a merge iterator of `MemTableIter` and `StorageTableIter`. If a pk exist in both KV state store (StorageTable) and memory (MemTable), result of `MemTableIter` is returned. For example, in the  following figure, `StateTableIter` will generate `1->4->5->6` in order.

![Scan example](../images/relational-table-layer/relational-table-02.svg)