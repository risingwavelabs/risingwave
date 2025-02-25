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

use std::fmt::Write;

use risingwave_expr::{ExprError, Result, capture_context, function};
use thiserror_ext::AsReport;

use super::context::{CATALOG_READER, DB_NAME};
use crate::catalog::CatalogReader;

#[function("pg_get_indexdef(int4) -> varchar")]
fn pg_get_indexdef(oid: i32, writer: &mut impl Write) -> Result<()> {
    pg_get_indexdef_impl_captured(oid, 0, writer)
}

#[function("pg_get_indexdef(int4, int4, boolean) -> varchar")]
fn pg_get_indexdef_col(
    oid: i32,
    column_no: i32,
    _pretty_bool: bool,
    writer: &mut impl Write,
) -> Result<()> {
    pg_get_indexdef_impl_captured(oid, column_no, writer)
}

#[capture_context(CATALOG_READER, DB_NAME)]
fn pg_get_indexdef_impl(
    catalog: &CatalogReader,
    db_name: &str,
    oid: i32,
    column_no: i32,
    writer: &mut impl Write,
) -> Result<()> {
    let ans = if column_no == 0 {
        catalog
            .read_guard()
            .get_index_by_id(db_name, oid as u32)
            .map_err(|e| ExprError::InvalidParam {
                name: "oid",
                reason: e.to_report_string().into(),
            })?
            .index_table
            .create_sql()
    } else {
        catalog
            .read_guard()
            .get_index_by_id(db_name, oid as u32)
            .map_err(|e| ExprError::InvalidParam {
                name: "oid",
                reason: e.to_report_string().into(),
            })?
            .get_column_def(column_no as usize - 1)
            .unwrap_or_default()
    };
    write!(writer, "{}", ans).unwrap();
    Ok(())
}
