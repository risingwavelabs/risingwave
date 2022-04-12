# Data Source

- [Data Source](#data-source)
  - [Components](#components)
    - [Connectors](#connectors)
    - [Enumerators](#enumerators)
    - [ConnectorSource](#connectorsource)
    - [SourceExecutor](#sourceexecutor)
  - [How It Works](#how-it-works)

This page describes RisingWave's Data Source API and the architecture behind it. This may help if you are interested in how data sources work, or if you want to implement a new Data Source.

## Components

RisingWave's data source covers four parts: connectors, enumerators, ConnectorSource and SourceExecutor.

![data source arch](../docs/images/data-source/data-source-arch.svg)

### Connectors

`Connector` serve as an interface to upstream data pipeline, including the message queue and file system. In the current design, it does not have infinite concurrency. One connector instance only reads from one split from the upstream. For example, if upstream is a Kafka and it has three partitions so, in RisingWave, there should be three connectors.

All connectors need to implement the following trait and it exposes two methods to the upper layer.

```rust
// src/connector/src/base.rs
#[async_trait]
pub trait SourceReader {
    async fn next(&mut self) -> Result<Option<Vec<InnerMessage>>>;
    async fn new(config: HashMap<String, String>, state: Option<ConnectorState>) -> Result<Self>
    where
        Self: Sized;
}
```

- `next`: return a batch of new messages and their offsets in the split.
- `new`: create a new connector with some properties, and this method should support restoring to a specific state via ConnectorState.

### Enumerators

`Enumerator` periodically requests upstream to discover changes in splits, and in most cases the number of splits only increases. The enumerator is a separate task that runs on the [meta](./meta-service.md). If the upstream split changes, the enumerator notifies the connector by means of config change to change the subscription relationship.

All enumerators need to implement the following trait.

```rust
// src/connector/src/base.rs
#[async_trait]
pub trait SplitEnumerator {
    type Split: SourceSplit + Send + Sync;
    async fn list_splits(&mut self) -> Result<Vec<Self::Split>>;
}
```

- `list_splits`: requests the upstream and returns all partitions.

### ConnectorSource

`ConnectorSource` unites all connectors via `SourceReader` trait. Also, a parser is held here, which parses raw data to data chunks or stream chunks according to column description. One `ConnectorSource` keeps one connector and one parser.

### SourceExecutor

`SourceExecutor` relies on batch processing in the current implementation, which builds connector and parser. The primary responsibility of the `SourceExecutor` is to correctly process barriers.

## How It Works

1. When a source is defined, meta service will register its schema and broadcast to compute nodes. Compute node extracts properties from the frontend and builds corresponding components and stores them as `SourceDesc` in `source_manager` identified by table_id.
2. `SourceExecutor` fetches its SourceDesc by table_id and builds a state handler. Then the building process is completed and no data is read from upstream.
3. When receiving a barrier, SourceExecutor will check whether it contains an assign_split mutation. If the partition assignment in the assign_split mutation is different from the current situation, the `SourceExecutor` needs to rebuild the `ConnectorSource` and other underlying services based on the information in the mutation, then starts reading from the new split and offset.
4. Whenever receiving a barrier, the state handler always takes a snapshot of the `ConnectorSource` then labels the snapshot with an epoch number. When an error occurs, SourceExecutor takes a specific state and applies it.
