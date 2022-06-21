# An Overview of RisingWave State Store

- [Storing State Using Relational Table](#an-overview-of-risingwave-state-store)
  - [Overview](#overview)
  - [Relational Table](#Relational-table)
    - [Write Path](#relational-table-write-path)
    - [Read Path](#relational-table-read-path)

    

<!-- Created by https://github.com/ekalinin/github-markdown-toc -->

## Overview

In RisingWave, all streaming executors store their data into a state store. This KV state store is backed by a service called Hummock, a cloud-native LSM-Tree-based storage engine. Hummock provides key-value API, and stores all data on S3-compatible service. However, it is not a KV store for general purpose, but a storage engine co-designed with RisingWave streaming engine and optimized for streaming workload.

As the executor state's key encoding is very similar to a cell-based table, each kind of state is stored as a cell-based relational table first. We implement a relational table layer as the bridge between stateful executors and KV state store, which provides the interfaces accessing KV data in relational semantic.



## Relational Table
[source code](https://github.com/singularity-data/risingwave/blob/main/src/storage/src/table/state_table.rs)

In this part, we will introduce how stateful executors interact with KV state store through the relational table layer.

Relational table layer consists of State Table, Mem Table and Cell-based Table. The State Table provides the table operations by these APIs: `get_row`, `scan`, `insert_row`, `delete_row` and `update_row`, which are the read and write  interfaces for executors. The Mem Table is an in-memory buffer for caching table operations during one epoch, and the Cell-based Table is responsible for performing serialization and deserialization between cell-based encoding and KV encoding.

![Overview of Relational Table](images/relational-table-layer/relational-table-01.svg)

### Write Path
Executors perform operations on relational table, and these operations will first be cached in Mem Table. Once an executor wants to write these operations to state store, cell-based table will covert these operations into kv pairs and write to state store with specific epoch. 

### Read Path
Executors should be able to read the just written data, which means uncommited data is visible. The data in Mem Table(memory) is fresher than that in shared storage(state store). State Table provides both point-get and scan to read from state store by merging data from Mem Table and Cell-based Table. For example, let's assume that the first column is the pk of relational table, and the following operations are performed.
```
insert [1, 11, 111]
insert [2, 22, 222]
delete [2, 22, 222]
insert [3, 33, 333]

commit

insert [3, 3333, 3333]
```

After commit, a new record is inserted. The read results with corresponding pk are:
```
Read pk = 1: [1, 11, 111]
Read pk = 2:  None
Read pk = 3: [3, 3333, 3333]
```