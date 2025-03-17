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
use thiserror_ext::AsReport;

use super::context::{CATALOG_READER, DB_NAME};
use crate::catalog::CatalogReader;

/// Computes the total disk space used by indexes attached to the specified table.
#[function("pg_indexes_size(int4) -> int8")]
fn pg_indexes_size(oid: i32) -> Result<i64> {
    pg_indexes_size_impl_captured(oid)
}

#[capture_context(CATALOG_READER, DB_NAME)]
fn pg_indexes_size_impl(catalog: &CatalogReader, db_name: &str, oid: i32) -> Result<i64> {
    let catalog = catalog.read_guard();
    let database = catalog
        .get_database_by_name(db_name)
        .map_err(|e| ExprError::InvalidParam {
            name: "oid",
            reason: e.to_report_string().into(),
        })?;
    let mut sum = 0;
    for schema in database.iter_schemas() {
        for index in schema.iter_index() {
            if index.primary_table.id().table_id == oid as u32 {
                if let Some(table_stats) = catalog
                    .table_stats()
                    .table_stats
                    .get(&index.primary_table.id().table_id)
                {
                    sum += table_stats.total_key_size + table_stats.total_value_size;
                }
            }
        }
    }
    Ok(sum)
}
