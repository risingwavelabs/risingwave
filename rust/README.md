## Introduction

Several components of risingwave are developed in rust: query engine, streaming engine and storage engine(WIP). And 
the workspace are organized as following:

1. `config`: Contains default configurations for servers, e.g. logging configuration.
2. `proto`: Contains generated protobuf rust code, e.g. grpc definition and message definition.
3. `util`: Several independent util crates which helps to simplify development. We plan to publish them to 
[crates.io](crates.io) in future when they are more mature.
4. `server`: The crate contains our query engine server and streaming server. We may split them into smaller crates 
   in future.
5. `meta`: The crate contains our meta server.