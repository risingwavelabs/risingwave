// Copyright 2025 RisingWave Labs
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

use risingwave_expr::{ExprError, Result, capture_context, function};

use super::context::CATALOG_READER;
use crate::catalog::CatalogReader;

/// Computes the disk space used by one “fork” of the specified relation.
#[function("pg_relation_size(int4) -> int8")]
fn pg_relation_size(oid: i32) -> Result<i64> {
    pg_relation_size_impl_captured(oid, "main")
}

#[function("pg_relation_size(int4, varchar) -> int8")]
fn pg_relation_size_fork(oid: i32, fork: &str) -> Result<i64> {
    pg_relation_size_impl_captured(oid, fork)
}

#[capture_context(CATALOG_READER)]
fn pg_relation_size_impl(catalog: &CatalogReader, oid: i32, fork: &str) -> Result<i64> {
    match fork {
        "main" => {}
        // These options are invalid in RW so we return 0 value as the result
        "fsm" | "vm" | "init" => return Ok(0),
        _ => return Err(ExprError::InvalidParam {
            name: "fork",
            reason:
                "invalid fork name. Valid fork names are \"main\", \"fsm\", \"vm\", and \"init\""
                    .into(),
        }),
    }
    let catalog = catalog.read_guard();
    if let Some(stats) = catalog.table_stats().table_stats.get(&(oid as u32)) {
        Ok(stats.total_key_size + stats.total_value_size)
    } else {
        Ok(0)
    }
}
