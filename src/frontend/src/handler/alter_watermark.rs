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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_sqlparser::ast::{Expr, Ident, ObjectName, Statement};

use super::alter_table_column::{fetch_table_catalog_for_alter, get_replace_table_plan};
use super::create_source::SqlColumnStrategy;
use super::{HandlerArgs, RwPgResponse};
use crate::TableCatalog;
use crate::error::{ErrorCode, Result, RwError};

pub async fn handle_alter_watermark(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    column_name: Ident,
    expr: Expr,
    with_ttl: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let (original_catalog, _) = fetch_table_catalog_for_alter(session.as_ref(), &table_name)?;

    if original_catalog.webhook_info.is_some() {
        return Err(ErrorCode::BindError(
            "ALTER WATERMARK on a table with webhook has not been implemented.".to_owned(),
        )
        .into());
    }

    let mut definition = original_catalog.create_sql_ast_purified()?;
    let Statement::CreateTable {
        source_watermarks, ..
    } = &mut definition
    else {
        panic!("unexpected statement: {:?}", definition);
    };

    let column_real_value = column_name.real_value();
    let existing = source_watermarks
        .iter_mut()
        .find(|w| w.column.real_value() == column_real_value)
        .ok_or_else(|| {
            ErrorCode::InvalidInputSyntax(format!(
                "no watermark defined on column \"{}\" of table \"{}\"",
                column_real_value, table_name
            ))
        })?;

    // v1: toggling WITH TTL changes `stream_key` / `clean_watermark_indices` on the
    // materialize catalog, which `fit_internal_tables_trivial` cannot safely reconcile.
    // Deferred to v2.
    if existing.with_ttl != with_ttl {
        return Err(ErrorCode::NotSupported(
            "toggling WITH TTL via ALTER WATERMARK is not supported".to_owned(),
            "drop and recreate the table".to_owned(),
        )
        .into());
    }

    existing.expr = expr;

    let (source, new_table, graph, job_type) = Box::pin(get_replace_table_plan(
        &session,
        table_name,
        definition,
        &original_catalog,
        SqlColumnStrategy::FollowChecked,
    ))
    .await?;

    check_replace_safe(&original_catalog, &new_table)?;

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .replace_table(
            source.map(|x| x.to_prost()),
            new_table.to_prost(),
            graph,
            job_type,
        )
        .await?;
    Ok(PgResponse::empty_result(StatementType::ALTER_TABLE))
}

// Defense-in-depth. Today every v1 input that survives the AST guard above also
// survives `get_replace_table_plan`'s binder type check, so none of the diffs below
// can fire for valid input. The check is kept for two reasons:
//   1. Future relaxations (e.g. allowing TTL toggle in v2) will start producing
//      `stream_key` / `clean_watermark_indices` diffs and need this guard.
//   2. The meta-side `fit_internal_tables_trivial` path is a wholesale-overwrite
//      that would silently corrupt state if it ever paired tables with mismatched
//      schemas; this is the last line of defense before that happens.
fn check_replace_safe(old: &TableCatalog, new: &TableCatalog) -> Result<()> {
    fn diff<T: std::fmt::Debug>(field: &str, old: T, new: T) -> RwError {
        ErrorCode::NotSupported(
            format!(
                "ALTER WATERMARK would change `{}` of the table\nold: {:?}\nnew: {:?}",
                field, old, new
            ),
            "only changes that preserve the plan's state schema are supported; \
             drop and recreate the table for shape changes"
                .to_owned(),
        )
        .into()
    }

    if old.stream_key != new.stream_key {
        return Err(diff("stream_key", &old.stream_key, &new.stream_key));
    }
    if old.pk != new.pk {
        return Err(diff("pk", &old.pk, &new.pk));
    }
    let old_wm: Vec<_> = old.watermark_columns.ones().collect();
    let new_wm: Vec<_> = new.watermark_columns.ones().collect();
    if old_wm != new_wm {
        return Err(diff("watermark_columns", &old_wm, &new_wm));
    }
    if old.clean_watermark_indices != new.clean_watermark_indices {
        return Err(diff(
            "clean_watermark_indices",
            &old.clean_watermark_indices,
            &new.clean_watermark_indices,
        ));
    }
    if old.clean_watermark_index_in_pk != new.clean_watermark_index_in_pk {
        return Err(diff(
            "clean_watermark_index_in_pk",
            &old.clean_watermark_index_in_pk,
            &new.clean_watermark_index_in_pk,
        ));
    }
    // `original_catalog` is loaded via `TableCatalog::from_prost`, which appends the
    // `_rw_timestamp` system column to `columns`. The freshly-built `new` table from
    // `get_replace_table_plan` doesn't go through `from_prost` and therefore lacks it.
    // Compare only user-visible columns to avoid a spurious mismatch.
    let old_cols = old.columns_without_rw_timestamp();
    let new_cols = new.columns_without_rw_timestamp();
    if old_cols.len() != new_cols.len() {
        return Err(diff("columns.len()", &old_cols.len(), &new_cols.len()));
    }
    for (i, (o, n)) in old_cols.iter().zip_eq_fast(new_cols.iter()).enumerate() {
        if o.data_type() != n.data_type() {
            return Err(ErrorCode::NotSupported(
                format!(
                    "ALTER WATERMARK would change the data type of column \"{}\" (index {}): {:?} -> {:?}",
                    o.name(), i, o.data_type(), n.data_type()
                ),
                "drop and recreate the table".to_owned(),
            )
            .into());
        }
    }
    Ok(())
}
