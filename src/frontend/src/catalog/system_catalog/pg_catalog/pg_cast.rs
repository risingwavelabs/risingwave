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

use std::sync::LazyLock;

use itertools::Itertools;
use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::BuiltinView;
use crate::expr::cast_map_array;

pub static PG_CAST_DATA: LazyLock<Vec<String>> = LazyLock::new(|| {
    let mut cast_array = cast_map_array();
    cast_array.sort();
    cast_array
        .iter()
        .enumerate()
        .map(|(idx, (src, target, ctx))| {
            format!(
                "({}, {}, {}, \'{}\')",
                idx,
                DataType::from(*src).to_oid(),
                DataType::from(*target).to_oid(),
                ctx
            )
        })
        .collect_vec()
});

/// The catalog `pg_cast` stores data type conversion paths.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-cast.html`]
pub static PG_CAST: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_cast",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "oid"),
        (DataType::Int32, "castsource"),
        (DataType::Int32, "casttarget"),
        (DataType::Varchar, "castcontext"),
    ],
    sql: format!(
        "SELECT oid, castsource, casttarget, castcontext \
            FROM (VALUES {}) AS _(oid, castsource, casttarget, castcontext)\
    ",
        PG_CAST_DATA.join(",")
    ),
});
