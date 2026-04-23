<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# RFC: Modularize `iceberg` Implementations

## Background

Issue #1819 highlighted that the current `iceberg` crate mixes the Iceberg protocol abstractions (catalog/table/plan/transaction) with concrete runtime, storage, and execution code (Tokio runtime wrappers, opendal-based `FileIO`, Arrow helpers, DataFusion glue, etc.). This coupling makes the crate heavy and blocks users from composing their own storage or execution stacks.

Two principles have been agreed:
1. The `iceberg` crate remains the single source of truth for all protocol traits and data structures. We will not create a separate “kernel” crate or facade layer.
2. Concrete integrations (Tokio runtime, opendal `FileIO`, Arrow/DataFusion glue, catalog adapters, etc.) move out into dedicated companion crates. Users needing a ready path can depend on those crates (e.g., `iceberg-datafusion` or `integrations/local`), while custom stacks depend only on `iceberg`.

This RFC focuses on modularizing implementations; detailed trait signatures (e.g., `FileIO`, `Runtime`) will be handled in separate RFCs.

## Goals and Scope

- Keep `iceberg` as the protocol crate (traits + metadata + planning), without bundling runtimes, storage adapters, or execution glue.
- Relocate concrete code into companion crates under `crates/fileio/*`, `crates/runtime/*`, and `crates/integrations/*`.
- Provide a staged plan for extracting Arrow-dependent APIs to avoid destabilizing file-format code.
- Minimize breaking surfaces: traits stay in `iceberg`; downstream crates mainly adjust dependencies.

Out of scope: changes to the Iceberg table specification or catalog adapter external behavior; detailed trait method design (covered by follow-up RFCs).

## Architecture Overview

### Workspace Layout (target)

```
crates/
  iceberg/                # core traits, metadata, planning, transactions
  fileio/
    opendal/             # e.g. `iceberg-fileio-opendal`
    fs/                  # other FileIO implementations
  runtime/
    tokio/               # e.g. `iceberg-runtime-tokio`
    smol/
  catalog/*              # catalog adapters (REST, HMS, Glue, etc.)
  integrations/
    local/               # simple local/arrow-based helper crate
    datafusion/          # combines core + implementations for DF
    cache-moka/
    playground/
```

- `crates/iceberg` drops direct deps on opendal, Tokio, Arrow, and DataFusion.
- Implementation crates depend on `iceberg` to implement the traits.
- Higher-level crates (`integrations/local`, `iceberg-datafusion`) assemble the pieces for ready-to-use scenarios.

### Core Trait Surfaces

`FileIO`, `Runtime`, `Catalog`, `Table`, `Transaction`, `TableScan` (plan descriptors) all remain hosted in `iceberg`. Precise method signatures are deferred to dedicated RFCs to avoid locking details prematurely.

### Usage Modes

- **Custom stacks**: depend on `iceberg` and provide your own implementations.
- **Pre-built stacks**: depend on `integrations/local` or `iceberg-datafusion`, which bundle `iceberg` with selected runtime/FileIO/Arrow helpers.
- `iceberg` does not re-export companion crates; users compose explicitly.

## Migration Plan (staged, with Arrow extraction phased)

1. **Phase 1 – Confirm trait hosting, defer details**
   - Keep all protocol traits in `iceberg`; move detailed API design (FileIO, Runtime, etc.) to separate RFCs.
   - Add temporary shims/deprecations only when traits are finalized.

2. **Phase 2 – First Arrow step: move `to_arrow()` out**
   - Relocate the public `to_arrow()` API to `integrations/local` (or another higher-level crate). Core no longer exposes Arrow entry points.
   - Keep internal Arrow-dependent helpers (e.g., `ArrowFileReader`) temporarily in `iceberg` to avoid breaking file-format flows.

3. **Phase 3 – Gradual Arrow dependency removal**
   - Incrementally migrate/replace Arrow-dependent internals (`ArrowFileReader`, format-specific readers) into `integrations/local` or other helper crates.
   - Adjust file-format APIs as needed; expect this to be multi-release work.

4. **Phase 4 – Dependency cleanup**
   - Ensure catalog and integration crates depend only on `iceberg` plus the specific runtime/FileIO/helper crates they need.
   - Verify build/test pipelines against the new dependency graph.

5. **Phase 5 – Docs & release**
   - Publish migration guides: where `to_arrow()` moved, how to assemble local/DataFusion stacks.
   - Schedule deprecation windows for remaining Arrow helpers; target a breaking release once Arrow is fully removed from `iceberg`.

## Compatibility

- Short term: users of `Table::scan().to_arrow()` must switch to `integrations/local` (or another crate that rehosts that API). Other Arrow types stay temporarily but will migrate in later phases.
- Long term: `iceberg` will be Arrow-free; companion crates provide Arrow-based helpers.
- Tests/examples move alongside the implementations they exercise.

## Risks and Mitigations

| Risk | Description | Mitigation |
| ---- | ----------- | ---------- |
| Arrow dependency unwinding is complex | File-format readers may rely on Arrow types | Phase the work; move `to_arrow()` first, then refactor readers; document interim state |
| Discoverability | Users may not know where Arrow helpers went | Clear docs pointing to `integrations/local` and `iceberg-datafusion`; migration guide |
| Trait churn | Future trait RFCs may break early adopters | Use deprecation shims and communicate timelines |
| Duplicate impls | Multiple helper crates could overlap | Provide recommended combinations and feature guidance |

## Open Questions

1. Versioning: align companion crate versions with `iceberg`, or allow independent versions plus compatibility matrix?
2. Deprecation schedule: how long do we keep interim Arrow helpers before full removal from `iceberg`?

## Conclusion

We will keep `iceberg` as the protocol crate while modularizing concrete implementations. Arrow removal will be phased: first relocating `to_arrow()` to `integrations/local`, then gradually moving Arrow-dependent readers and helpers. This keeps the core lean, lets users compose their preferred runtime/FileIO stacks, and still offers ready-to-use combinations via companion crates.
