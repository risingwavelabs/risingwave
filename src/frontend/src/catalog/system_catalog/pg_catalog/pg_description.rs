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

use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::SystemCatalogColumnsDef;

/// The catalog `pg_description` stores description.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-description.html`]
pub const PG_DESCRIPTION_TABLE_NAME: &str = "pg_description";
pub const PG_DESCRIPTION_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "objoid"),
    // None
    (DataType::Int32, "classoid"),
    // 0
    (DataType::Int32, "objsubid"),
    // None
    (DataType::Varchar, "description"),
];

pub fn new_pg_description_row(id: u32) -> OwnedRow {
    OwnedRow::new(vec![
        Some(ScalarImpl::Int32(id as i32)),
        None,
        Some(ScalarImpl::Int32(0)),
        None,
    ])
}
