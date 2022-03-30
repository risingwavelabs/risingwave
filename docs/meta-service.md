# Meta Service

## Background

RisingWave provides high-concurrent access to Materailzed Views aka. the real-time results of a specified SQL query maintained by streaming tasks. Therefore, both the front-end and compute nodes are designed to be scalable, and they may share host machines or not, which depends on cluster size.

However, some components must be centralized such as metadata service, scheduler, monitoring, etc. A typical on-premise deployment may look like follows, where the boxes means minimal independent components.

![Cluster Deployment](./images/meta-service/cluster-deployment.svg)

## Types of Metadata

### Catalog

Catalog is the metadata of relational tables in databases. 

- **Database & Schema**: Namespace of database objects like Postgres
- **Table & Materialized Views**: Definition of tables & materialized views
- **Source**: User-defined data sources

To execute a DDL statement like `CREATE` or `DROP TABLE`, the front-end sends an RPC to meta node and waits the updated catalog to take effect.

### Storage

Hummock, as an LSM-Tree-based storage, stores it the mapping from version to the set of SSTable files in Meta Service. See more details in the [overview of State Store](./state-store-overview.md).

## Push on Updates

There are 2 choices for how to distribute information across multiple nodes. 

* *Push*: When metadata changes, the meta node tells all nodes to update, and master node must wait for others' acknowledges before continuing. 
* *Pull*: When data changes, the master node does nothing. Other nodes may not have the latest information, so they need to ask the master node every time.

Currently, for simplicity, we choose the push-style approach for all kinds of metadata. This is implemented as `NotificationService` on meta service and `ObserverManager` on front-end and compute nodes. 

![Notification](./images/meta-service/notification.svg)

`ObserverManager` will register itself to meta service on bootstrap and subscribe metadata it needs. Afterwards, once metadata changed, the meta node streams the changes to it, expecting all subscribers to acknowledge. 

## Persistent Storage 

Metadata should be regarded as transactional data. Especially, we need guarantees like atomicity, consistency (e.g. read-after-write) and transaction support. Besides, It's not necessray to scale out it to multiple nodes.

We choose [etcd](https://etcd.io/) for production deployments, which is a replicated key-value storage based on B-Tree and Raft protocol. 

