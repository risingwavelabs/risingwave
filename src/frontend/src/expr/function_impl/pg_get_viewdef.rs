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

use anyhow::anyhow;
use risingwave_expr::{ExprError, Result, capture_context, function};
use risingwave_sqlparser::ast::Statement;

use super::context::{CATALOG_READER, DB_NAME};
use crate::catalog::CatalogReader;

#[function("pg_get_viewdef(int4) -> varchar")]
fn pg_get_viewdef(oid: i32, writer: &mut impl Write) -> Result<()> {
    pg_get_viewdef_pretty(oid, false, writer)
}

#[function("pg_get_viewdef(int4, boolean) -> varchar")]
fn pg_get_viewdef_pretty(oid: i32, _pretty: bool, writer: &mut impl Write) -> Result<()> {
    pg_get_viewdef_impl_captured(oid, writer)
}

#[capture_context(CATALOG_READER, DB_NAME)]
fn pg_get_viewdef_impl(
    catalog: &CatalogReader,
    db_name: &str,
    oid: i32,
    writer: &mut impl Write,
) -> Result<()> {
    let catalog_reader = catalog.read_guard();

    if let Ok(view) = catalog_reader.get_view_by_id(db_name, oid as u32) {
        write!(writer, "{}", view.sql).unwrap();
        Ok(())
    } else if let Ok(mv) = catalog_reader.get_created_table_by_id_with_db(db_name, oid as u32) {
        let stmt = mv.create_sql_ast().map_err(|e| anyhow!(e))?;
        if let Statement::CreateView {
            query,
            materialized,
            ..
        } = stmt
            && materialized
        {
            write!(writer, "{}", query).unwrap();
            Ok(())
        } else {
            Err(ExprError::InvalidParam {
                name: "oid",
                reason: format!("view or materialized view does not exist: {oid}").into(),
            })
        }
    } else {
        Err(ExprError::InvalidParam {
            name: "oid",
            reason: format!("view or materialized view does not exist: {oid}").into(),
        })
    }
}
