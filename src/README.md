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

## Understanding RisingWave Macros

RisingWave uses a lot of macros to simplify development.
You may choose the read to macro definition directly,
but something complementary / easier is to use `cargo expand`
to expand the macro and read the expanded code.

### Example 1: declarative macro `commit_meta!()`

For instance, within `meta::manager::catalog::CatalogManager::finish_create_table_procedure`
there's a call to the macro `commit_meta!()`.

To understand it, first I dump the expanded code to a file:

```bash
cargo expand -p risingwave_meta > meta.rs
```

Then I search for the function call `finish_create_table_procedure` in my editor,
and compare the original code with the expanded code in `meta.rs`.

From there I can see that it does the following, followed by some instrumentation code which can be ignored:
```text
async {
    tables.apply_to_txn(&mut trx)?;
    self.env.meta_store().txn(trx).await?;
    tables.commit();
    MetaResult::Ok(())
}.instrument(/* ... */)
```

### Example 2: Procedural macro `aggregate`

The other example would be the `#[aggregate]` procedural macro.

To understand it, first I dump the expanded code to a file:

```bash
cargo expand -p risingwave_expr > expr.rs
```

Then we identify the `#[aggregate]` macro call to examine. In this case it's `string_agg`.

First, we know that Rust merges the individual modules into one big file, namespaced by the module name.

In this case `string_agg` is its own module, so we can search for `mod string_agg` in `expr.rs`.

We can see that the module now also contains `extern fn string_agg_varchar_varchar_varchar`.

Reading the code we understand now that it does the following:
1. Register it to a global registry for aggregates.
2. "it" here refers to an `AggFuncSig`, whose definition we can checkout.
3. Of note is that it will generate an anonymous struct which implements the `AggFunc` trait.