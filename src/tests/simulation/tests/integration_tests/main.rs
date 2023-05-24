// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The entry point to combine all integration tests into a single binary.
//!
//! See [this post](https://matklad.github.io/2021/02/27/delete-cargo-integration-tests.html)
//! for the rationale behind this approach.

#![feature(stmt_expr_attributes)]
#![cfg(madsim)]
#![feature(lazy_cell)]

// TODO: uncomment all of these mods again.
// mod cascade_materialized_view;
// mod delta_join;
// mod dynamic_filter;
// mod hello;
// mod nexmark_chaos;
// mod nexmark_q4;
// mod nexmark_recovery;
mod nexmark_scaling;
// mod nexmark_source;
// mod singleton_migration;
// mod streaming_parallelism;
// mod recovery;
// mod scale;
