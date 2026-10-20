// Copyright 2026 RisingWave Labs
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

use anyhow::{Context, anyhow};
use fancy_regex::Regex;
use itertools::Itertools;
use risingwave_common::catalog::{CdcKeyComparison, ColumnCatalog};
use risingwave_connector::source::cdc::external::{ExternalTableConfig, SchemaTableName};
use risingwave_connector::source::cdc::{
    CdcExternalTableHandler, CdcExternalTableNameParser, CdcSourceType,
};
use risingwave_connector::{WithOptionsSecResolved, WithPropertiesExt, dispatch_cdc_source_type};
use risingwave_sqlparser::ast::{ColumnDef, ColumnOption, SourceWatermark, TableConstraint};
use thiserror_ext::AsReport;

use crate::error::{ErrorCode, Result, RwError};
use crate::handler::create_source::reject_variant_columns;
use crate::handler::create_table::{bind_sql_columns, bind_sql_pk_names, bind_table_constraints};

/// Derive connector properties and normalize `external_table_name` for CDC tables.
///
/// Returns (`connector_properties`, `normalized_external_table_name`). The parsing is the
/// connector's [`CdcExternalTableNameParser::fill_table_name_options`].
pub(crate) fn derive_with_options_for_cdc_table(
    source_with_properties: &WithOptionsSecResolved,
    external_table_name: String,
) -> Result<(WithOptionsSecResolved, String)> {
    if source_with_properties.get("database.name").is_none() {
        return Err(anyhow!("The source with properties does not contain 'database.name'").into());
    }
    let mut with_options = source_with_properties.clone();
    let external_table_name = dispatch_cdc_source_type!(
        source_with_properties.cdc_source_type(),
        T,
        T::fill_table_name_options(&mut with_options, &external_table_name)
            .map_err(|err| RwError::from(anyhow::Error::from(err)))?,
        return Err(anyhow!(
            "connector {} is not supported for cdc table",
            source_with_properties.get_connector().unwrap_or_default()
        )
        .into())
    );
    Ok((with_options, external_table_name))
}

/// Reject a CDC table when a primary-key column is filtered out of Debezium change-event
/// values via `debezium.column.exclude.list` or `debezium.column.include.list`.
///
/// Debezium's column filters only apply to the change-event **value** payload. Message keys are
/// always built from the upstream PRIMARY KEY and are not affected. If a PK column is filtered out
/// of the value, RisingWave reads NULL for that PK column from the payload, causing silent data
/// corruption: UPDATE turns into a fresh INSERT (PK mismatch with the original row) and DELETE
/// silently no-ops.
///
/// Debezium entries are regex patterns matched against the fully qualified column name
/// `<namespace>.<table>.<column>`, where namespace is `schema` for Postgres / SQL Server and
/// `database` for MySQL.
pub(crate) fn reject_pk_filtered_by_debezium_column_filter(
    pk_names: &[String],
    cdc_with_options: &WithOptionsSecResolved,
) -> Result<()> {
    const EXCLUDE_KEY: &str = "debezium.column.exclude.list";
    const INCLUDE_KEY: &str = "debezium.column.include.list";

    let st = dispatch_cdc_source_type!(
        cdc_with_options.cdc_source_type(),
        T,
        T::schema_table_name_from_options(cdc_with_options.as_plaintext()),
        unreachable!("cdc table options must name a cdc connector")
    );
    reject_pk_filtered_by_debezium_column_filter_inner(
        pk_names,
        &st,
        cdc_with_options.get(EXCLUDE_KEY).map(String::as_str),
        cdc_with_options.get(INCLUDE_KEY).map(String::as_str),
    )
}

fn reject_pk_filtered_by_debezium_column_filter_inner(
    pk_names: &[String],
    st: &SchemaTableName,
    exclude_list: Option<&str>,
    include_list: Option<&str>,
) -> Result<()> {
    const EXCLUDE_KEY: &str = "debezium.column.exclude.list";
    const INCLUDE_KEY: &str = "debezium.column.include.list";

    let pk_full_names = pk_names
        .iter()
        .map(|pk| (pk, format!("{}.{}.{}", st.schema_name, st.table_name, pk)))
        .collect_vec();

    if let Some(exclude_list) = exclude_list {
        let patterns = compile_debezium_column_filter_patterns(EXCLUDE_KEY, exclude_list)?;
        for (pk, pk_full_name) in &pk_full_names {
            for (pattern, regex) in &patterns {
                if regex.is_match(pk_full_name).map_err(|err| {
                    ErrorCode::InvalidInputSyntax(format!(
                        "failed to evaluate Debezium column filter pattern `{pattern}` in `{EXCLUDE_KEY}`: {}",
                        err.as_report()
                    ))
                })? {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "primary key column `{pk}` is excluded by `{EXCLUDE_KEY}` pattern \
                         `{pattern}`. Excluding a PK column causes silent data corruption: \
                         Debezium keeps the PK in the message key but drops it from the payload, \
                         so RisingWave cannot match UPDATE/DELETE events against the original row."
                    ))
                    .into());
                }
            }
        }
    }

    if let Some(include_list) = include_list {
        let patterns = compile_debezium_column_filter_patterns(INCLUDE_KEY, include_list)?;
        for (pk, pk_full_name) in &pk_full_names {
            let mut included = false;
            for (_, regex) in &patterns {
                if regex.is_match(pk_full_name).map_err(|err| {
                    ErrorCode::InvalidInputSyntax(format!(
                        "failed to evaluate Debezium column filter pattern in `{INCLUDE_KEY}`: {}",
                        err.as_report()
                    ))
                })? {
                    included = true;
                    break;
                }
            }
            if !included {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "primary key column `{pk}` is not included by `{INCLUDE_KEY}`. Omitting a PK \
                     column causes silent data corruption: Debezium keeps the PK in the message key \
                     but drops it from the payload, so RisingWave cannot match UPDATE/DELETE events \
                     against the original row."
                ))
                .into());
            }
        }
    }

    Ok(())
}

fn compile_debezium_column_filter_patterns(
    key: &str,
    filter_list: &str,
) -> Result<Vec<(String, Regex)>> {
    filter_list
        .split(',')
        .map(str::trim)
        .filter(|pattern| !pattern.is_empty())
        .map(|pattern| {
            let anchored_pattern = format!("(?i:^(?:{pattern})$)");
            let regex = Regex::new(&anchored_pattern).map_err(|err| {
                ErrorCode::InvalidInputSyntax(format!(
                    "invalid Debezium column filter pattern `{pattern}` in `{key}`: {}",
                    err.as_report()
                ))
            })?;
            Ok((pattern.to_owned(), regex))
        })
        .collect()
}

// For both table from cdc source and table with cdc connector
pub(crate) fn not_null_check_for_cdc_table(
    wildcard_idx: &Option<usize>,
    column_defs: &Vec<ColumnDef>,
) -> Result<()> {
    if !wildcard_idx.is_some()
        && column_defs.iter().any(|col| {
            col.options
                .iter()
                .any(|opt| matches!(opt.option, ColumnOption::NotNull))
        })
    {
        return Err(ErrorCode::NotSupported(
            "CDC table with NOT NULL constraint is not supported".to_owned(),
            "Please remove the NOT NULL constraint for columns".to_owned(),
        )
        .into());
    }
    Ok(())
}

// Only for table from cdc source
pub(crate) fn sanity_check_for_table_on_cdc_source(
    append_only: bool,
    column_defs: &Vec<ColumnDef>,
    wildcard_idx: &Option<usize>,
    constraints: &Vec<TableConstraint>,
    source_watermarks: &Vec<SourceWatermark>,
) -> Result<()> {
    // wildcard cannot be used with column definitions
    if wildcard_idx.is_some() && !column_defs.is_empty() {
        return Err(ErrorCode::NotSupported(
            "wildcard(*) and column definitions cannot be used together".to_owned(),
            "Remove the wildcard or column definitions".to_owned(),
        )
        .into());
    }

    // cdc table must have primary key constraint or primary key column
    if !wildcard_idx.is_some()
        && !constraints.iter().any(|c| {
            matches!(
                c,
                TableConstraint::Unique {
                    is_primary: true,
                    ..
                }
            )
        })
        && !column_defs.iter().any(|col| {
            col.options
                .iter()
                .any(|opt| matches!(opt.option, ColumnOption::Unique { is_primary: true }))
        })
    {
        return Err(ErrorCode::NotSupported(
            "CDC table without primary key constraint is not supported".to_owned(),
            "Please define a primary key".to_owned(),
        )
        .into());
    }

    if append_only {
        return Err(ErrorCode::NotSupported(
            "append only modifier on the table created from a CDC source".into(),
            "Remove the APPEND ONLY clause".into(),
        )
        .into());
    }

    if !source_watermarks.is_empty()
        && source_watermarks
            .iter()
            .any(|watermark| !watermark.with_ttl)
    {
        return Err(ErrorCode::NotSupported(
            "non-TTL watermark defined on the table created from a CDC source".into(),
            "Use `WATERMARK ... WITH TTL` instead.".into(),
        )
        .into());
    }

    Ok(())
}

/// Derive the schema of a CDC table from its upstream external table.
pub(crate) async fn bind_cdc_table_schema_externally(
    cdc_with_options: WithOptionsSecResolved,
) -> Result<(Vec<ColumnCatalog>, Vec<String>, Vec<CdcKeyComparison>)> {
    let (options, secret_refs) = cdc_with_options.into_parts();
    let config = ExternalTableConfig::try_from_btreemap(options, secret_refs)
        .context("failed to extract external table config")?;

    let table = dispatch_cdc_source_type!(
        CdcSourceType::from(config.connector.as_str()),
        T,
        T::connect_external_table(config).await,
        Err(anyhow!("Unsupported cdc connector type: {}", config.connector).into())
    )
    .context("failed to auto derive table schema")?;

    let pk_names = table.pk_names().to_vec();
    let pk_comparisons = table.pk_column_comparisons(&pk_names)?;
    Ok((
        table
            .column_descs()
            .iter()
            .cloned()
            .map(|column_desc| ColumnCatalog {
                column_desc,
                is_hidden: false,
            })
            .collect(),
        pk_names,
        pk_comparisons,
    ))
}

pub(crate) async fn bind_cdc_pk_comparisons_externally(
    cdc_with_options: WithOptionsSecResolved,
    pk_names: &[String],
) -> Result<Vec<CdcKeyComparison>> {
    // Replacement plans also use this path, so a successful schema change persists the current
    // upstream comparison semantics in the new stream graph.
    let (options, secret_refs) = cdc_with_options.into_parts();
    let config = ExternalTableConfig::try_from_btreemap(options, secret_refs)
        .context("failed to extract external table config")?;
    Ok(dispatch_cdc_source_type!(
        CdcSourceType::from(config.connector.as_str()),
        T,
        T::discover_pk_column_comparisons(&config, pk_names).await?,
        vec![CdcKeyComparison::Native; pk_names.len()]
    ))
}

/// Derive the schema of a CDC table from explicit SQL columns and constraints.
pub(crate) fn bind_cdc_table_schema(
    column_defs: &Vec<ColumnDef>,
    constraints: &Vec<TableConstraint>,
    is_for_replace_plan: bool,
) -> Result<(Vec<ColumnCatalog>, Vec<String>)> {
    let columns = bind_sql_columns(column_defs, is_for_replace_plan)?;
    // CDC parsers cannot produce variant values, and this path has no FORMAT/ENCODE gate.
    reject_variant_columns(&columns, "on a table created from a CDC source")?;

    let pk_names = bind_sql_pk_names(column_defs, bind_table_constraints(constraints)?)?;
    Ok((columns, pk_names))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema_table_name() -> SchemaTableName {
        SchemaTableName {
            schema_name: "public".to_owned(),
            table_name: "orders".to_owned(),
        }
    }

    fn pk_names() -> Vec<String> {
        vec!["plan_id".to_owned(), "site_id".to_owned()]
    }

    #[test]
    fn test_debezium_filter_rejects_literal_excluded_pk() {
        let err = reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            Some("public.orders.site_id"),
            None,
        )
        .unwrap_err();

        assert!(err.to_report_string().contains("site_id"));
        assert!(
            err.to_report_string()
                .contains("debezium.column.exclude.list")
        );
    }

    #[test]
    fn test_debezium_filter_rejects_include_list_missing_pk() {
        let err = reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            None,
            Some("public.orders.plan_id,public.orders.payload"),
        )
        .unwrap_err();

        assert!(err.to_report_string().contains("site_id"));
        assert!(
            err.to_report_string()
                .contains("debezium.column.include.list")
        );
    }

    #[test]
    fn test_debezium_filter_accepts_include_list_covering_all_pks() {
        reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            None,
            Some("public.orders.plan_id,public.orders.site_id,public.orders.payload"),
        )
        .unwrap();
    }

    #[test]
    fn test_debezium_filter_matches_regex_patterns() {
        reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            None,
            Some(r"public[.]orders[.](plan_id|site_id),public[.]orders[.]payload"),
        )
        .unwrap();

        let err = reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            Some(r".*[.]orders[.]site_id"),
            None,
        )
        .unwrap_err();

        assert!(err.to_report_string().contains("site_id"));
    }

    #[test]
    fn test_debezium_filter_matches_patterns_case_insensitively() {
        let err = reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            Some("PUBLIC.ORDERS.SITE_ID"),
            None,
        )
        .unwrap_err();

        assert!(err.to_report_string().contains("site_id"));

        reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            None,
            Some("Public.Orders.Plan_ID,Public.Orders.Site_ID"),
        )
        .unwrap();
    }
}
