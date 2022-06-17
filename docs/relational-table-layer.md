# An Overview of Relational Table Layer
- [An Overview of RisingWave State Store](#an-overview-of-risingwave-state-store)
  - [Overview](#overview)
  - [Architecture](#architecture)
  - [The Hummock User API](#the-hummock-user-api)
  - [Hummock Internals](#hummock-internals)
    - [Storage Format](#storage-format)
    - [Write Path](#write-path)
    - [Read Path](#read-path)
    - [Compaction](#compaction)
    - [Transaction Management with Hummock Manager](#transaction-management-with-hummock-manager)
    - [Checkpointing in Streaming](#checkpointing-in-streaming)

<!-- Created by https://github.com/ekalinin/github-markdown-toc -->

## Overview

In RisingWave, all streaming executors store their data into a state store. As the state's key encoding is very similar to a cell-based table, each kind of state is stored as a cell-based relational table first. And the cell-based relational table provides the interface accessing relational data in KV. Therefore, relational table can be regared as the bridge between state-ful executors and state store.
## Architecture


![Overview of Architecture](images/relational-table-layer/relational-table-01.svg)

Relational table consists of State Table, Mem Table and Cell-based Table. The State Table provides the table operations by these APIs: `get_row`, `insert_row`, `delete_row` and `update_row`, the Mem Table
is a buffer for modify operations without encoding, and the Cell-based Table is responsible for performing serialization and deserialization between cell-based encoding and KV encoding.


### Write Path
Operations on relational table will first be cached in Mem Table, which is a BTreeMap data structure in memory. Once executor wants to write these operations to state store, cell-based table will covert these operations into kv pairs and write to state store with specific epoch. 

### Read Path
Executors should be able to read the just written data, which means every written data including uncommited is visiable. The data in mem_table(memory) is fresher than that in shared storage(state store). State table provides both point-get and scan to read from state store by merging data from Mem Table and Cell-based Table.

