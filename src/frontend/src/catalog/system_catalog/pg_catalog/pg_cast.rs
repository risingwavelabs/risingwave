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
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::SystemCatalogColumnsDef;
use crate::expr::cast_map_array;

/// The catalog `pg_cast` stores data type conversion paths.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-cast.html`]
pub const PG_CAST_TABLE_NAME: &str = "pg_cast";
pub const PG_CAST_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "oid"),
    (DataType::Int32, "castsource"),
    (DataType::Int32, "casttarget"),
    (DataType::Varchar, "castcontext"),
];

pub static PG_CAST_DATA_ROWS: LazyLock<Vec<OwnedRow>> = LazyLock::new(|| {
    let mut cast_array = cast_map_array();
    cast_array.sort();
    cast_array
        .iter()
        .enumerate()
        .map(|(idx, (src, target, ctx))| {
            OwnedRow::new(vec![
                Some(ScalarImpl::Int32(idx as i32)),
                Some(ScalarImpl::Int32(DataType::from(*src).to_oid())),
                Some(ScalarImpl::Int32(DataType::from(*target).to_oid())),
                Some(ScalarImpl::Utf8(ctx.to_string().into())),
            ])
        })
        .collect_vec()
});
