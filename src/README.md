## Introduction

Almost all components of RisingWave are developed in rust, and they are split to several crates:

1. `config` contains default configurations for servers.
2. `prost` contains generated protobuf rust code, e.g. grpc definition and message definition.
3. `stream` contains our stream compute engine, read [Stream Engine][stream-engine] for more details.
4. `batch` contains our batch compute engine for queries against materialized views.
5. `frontend` contains our SQL query planner and scheduler.
6. `storage` contains our cloud native storage engine, read [State Store Overview][state-store] for more details.
7. `meta` contains our meta engine, read [Meta Service][meta-service] for more details.
8. `utils` contains several independent util crates which helps to simplify development. We plan to publish them to [crates.io](https://crates.io/) in future when they are more mature.
9. `cmd` contains all binaries, and `cmd_all` contains the all-in-one binary `risingwave`.
10. `risedevtool` is an awesome developer tool for RisingWave, read [RiseDev Guide][risedev] for more details.

[stream-engine]: https://github.com/risingwavelabs/risingwave/blob/main/docs/streaming-overview.md
[state-store]: https://github.com/risingwavelabs/risingwave/blob/main/docs/state-store-overview.md
[meta-service]: https://github.com/risingwavelabs/risingwave/blob/main/docs/meta-service.md
[risedev]: https://github.com/risingwavelabs/risingwave/tree/main/src/risedevtool
