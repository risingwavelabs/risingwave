// Copyright 2024 RisingWave Labs
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

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use either::Either;
use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{
    CdcTableDesc, ColumnCatalog, ColumnDesc, TableId, TableVersionId, DEFAULT_SCHEMA_NAME,
    INITIAL_TABLE_VERSION_ID,
};
use risingwave_common::license::Feature;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_common::util::value_encoding::DatumToProtoExt;
use risingwave_connector::source::cdc::build_cdc_table_id;
use risingwave_connector::source::cdc::external::{
    ExternalTableConfig, ExternalTableImpl, DATABASE_NAME_KEY, SCHEMA_NAME_KEY, TABLE_NAME_KEY,
};
use risingwave_connector::{source, WithOptionsSecResolved};
use risingwave_pb::catalog::{PbSource, PbTable, Table, WatermarkDesc};
use risingwave_pb::ddl_service::TableJobType;
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_pb::plan_common::{
    AdditionalColumn, ColumnDescVersion, DefaultColumnDesc, GeneratedColumnDesc,
};
use risingwave_pb::stream_plan::StreamFragmentGraph;
use risingwave_sqlparser::ast::{
    CdcTableInfo, ColumnDef, ColumnOption, ConnectorSchema, DataType as AstDataType,
    ExplainOptions, Format, ObjectName, OnConflict, SourceWatermark, TableConstraint,
};
use risingwave_sqlparser::parser::IncludeOption;
use thiserror_ext::AsReport;

use super::RwPgResponse;
use crate::binder::{bind_data_type, bind_struct_field, Clause};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::table_catalog::TableVersion;
use crate::catalog::{check_valid_column_name, ColumnId, DatabaseId, SchemaId};
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{Expr, ExprImpl, ExprRewriter};
use crate::handler::create_source::{
    bind_columns_from_source, bind_connector_props, bind_create_source_or_table_with_connector,
    bind_source_watermark, handle_addition_columns, UPSTREAM_SOURCE_KEY,
};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::generic::{CdcScanOptions, SourceNodeKind};
use crate::optimizer::plan_node::{LogicalCdcScan, LogicalSource};
use crate::optimizer::property::{Order, RequiredDist};
use crate::optimizer::{OptimizerContext, OptimizerContextRef, PlanRef, PlanRoot};
use crate::session::SessionImpl;
use crate::stream_fragmenter::build_graph;
use crate::utils::OverwriteOptions;
use crate::{Binder, TableCatalog, WithOptions};

/// Column ID generator for a new table or a new version of an existing table to alter.
#[derive(Debug)]
pub struct ColumnIdGenerator {
    /// Existing column names and their IDs.
    ///
    /// This is used for aligning column IDs between versions (`ALTER`s). If a column already
    /// exists, its ID is reused. Otherwise, a new ID is generated.
    ///
    /// For a new table, this is empty.
    pub existing: HashMap<String, ColumnId>,

    /// The next column ID to generate, used for new columns that do not exist in `existing`.
    pub next_column_id: ColumnId,

    /// The version ID of the table to be created or altered.
    ///
    /// For a new table, this is 0. For altering an existing table, this is the **next** version ID
    /// of the `version_id` field in the original table catalog.
    pub version_id: TableVersionId,
}

impl ColumnIdGenerator {
    /// Creates a new [`ColumnIdGenerator`] for altering an existing table.
    pub fn new_alter(original: &TableCatalog) -> Self {
        let existing = original
            .columns()
            .iter()
            .map(|col| (col.name().to_owned(), col.column_id()))
            .collect();

        let version = original.version().expect("version field not set");

        Self {
            existing,
            next_column_id: version.next_column_id,
            version_id: version.version_id + 1,
        }
    }

    /// Creates a new [`ColumnIdGenerator`] for a new table.
    pub fn new_initial() -> Self {
        Self {
            existing: HashMap::new(),
            next_column_id: ColumnId::first_user_column(),
            version_id: INITIAL_TABLE_VERSION_ID,
        }
    }

    /// Generates a new [`ColumnId`] for a column with the given name.
    pub fn generate(&mut self, name: &str) -> ColumnId {
        if let Some(id) = self.existing.get(name) {
            *id
        } else {
            let id = self.next_column_id;
            self.next_column_id = self.next_column_id.next();
            id
        }
    }

    /// Consume this generator and return a [`TableVersion`] for the table to be created or altered.
    pub fn into_version(self) -> TableVersion {
        TableVersion {
            version_id: self.version_id,
            next_column_id: self.next_column_id,
        }
    }
}

fn ensure_column_options_supported(c: &ColumnDef) -> Result<()> {
    for option_def in &c.options {
        match option_def.option {
            ColumnOption::GeneratedColumns(_) => {}
            ColumnOption::DefaultColumns(_) => {}
            ColumnOption::Unique { is_primary: true } => {}
            _ => bail_not_implemented!("column constraints \"{}\"", option_def),
        }
    }
    Ok(())
}

/// Binds the column schemas declared in CREATE statement into `ColumnDesc`.
/// If a column is marked as `primary key`, its `ColumnId` is also returned.
/// This primary key is not combined with table constraints yet.
pub fn bind_sql_columns(column_defs: &[ColumnDef]) -> Result<Vec<ColumnCatalog>> {
    let mut columns = Vec::with_capacity(column_defs.len());

    for column in column_defs {
        ensure_column_options_supported(column)?;
        // Destruct to make sure all fields are properly handled rather than ignored.
        // Do NOT use `..` to ignore fields you do not want to deal with.
        // Reject them with a clear NotImplemented error.
        let ColumnDef {
            name,
            data_type,
            collation,
            ..
        } = column;

        let data_type = data_type
            .clone()
            .ok_or_else(|| ErrorCode::InvalidInputSyntax("data type is not specified".into()))?;
        if let Some(collation) = collation {
            // PostgreSQL will limit the datatypes that collate can work on.
            // https://www.postgresql.org/docs/16/collation.html#COLLATION-CONCEPTS
            //   > The built-in collatable data types are `text`, `varchar`, and `char`.
            //
            // But we don't support real collation, we simply ignore it here.
            if !["C", "POSIX"].contains(&collation.real_value().as_str()) {
                bail_not_implemented!(
                    "Collate collation other than `C` or `POSIX` is not implemented"
                );
            }

            match data_type {
                AstDataType::Text | AstDataType::Varchar | AstDataType::Char(_) => {}
                _ => {
                    return Err(ErrorCode::NotSupported(
                        format!("{} is not a collatable data type", data_type),
                        "The only built-in collatable data types are `varchar`, please check your type".into(),
                    ).into());
                }
            }
        }

        check_valid_column_name(&name.real_value())?;

        let field_descs: Vec<ColumnDesc> = if let AstDataType::Struct(fields) = &data_type {
            fields
                .iter()
                .map(bind_struct_field)
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![]
        };
        columns.push(ColumnCatalog {
            column_desc: ColumnDesc {
                data_type: bind_data_type(&data_type)?,
                column_id: ColumnId::placeholder(),
                name: name.real_value(),
                field_descs,
                type_name: "".to_string(),
                generated_or_default_column: None,
                description: None,
                additional_column: AdditionalColumn { column_type: None },
                version: ColumnDescVersion::Pr13707,
            },
            is_hidden: false,
        });
    }

    Ok(columns)
}

fn check_generated_column_constraints(
    column_name: &String,
    column_id: ColumnId,
    expr: &ExprImpl,
    column_catalogs: &[ColumnCatalog],
    generated_column_names: &[String],
    pk_column_ids: &[ColumnId],
) -> Result<()> {
    let input_refs = expr.collect_input_refs(column_catalogs.len());
    for idx in input_refs.ones() {
        let referred_generated_column = &column_catalogs[idx].column_desc.name;
        if generated_column_names
            .iter()
            .any(|c| c == referred_generated_column)
        {
            return Err(ErrorCode::BindError(format!(
                "Generated can not reference another generated column. \
                But here generated column \"{}\" referenced another generated column \"{}\"",
                column_name, referred_generated_column
            ))
            .into());
        }
    }

    if pk_column_ids.contains(&column_id) && expr.is_impure() {
        return Err(ErrorCode::BindError(format!(
            "Generated columns with impure expressions should not be part of the primary key. \
            Here column \"{}\" is defined as part of the primary key.",
            column_name
        ))
        .into());
    }

    Ok(())
}

/// Binds constraints that can be only specified in column definitions,
/// currently generated columns and default columns.
pub fn bind_sql_column_constraints(
    session: &SessionImpl,
    table_name: String,
    column_catalogs: &mut [ColumnCatalog],
    columns: Vec<ColumnDef>,
    pk_column_ids: &[ColumnId],
) -> Result<()> {
    let generated_column_names = {
        let mut names = vec![];
        for column in &columns {
            for option_def in &column.options {
                if let ColumnOption::GeneratedColumns(_) = option_def.option {
                    names.push(column.name.real_value());
                    break;
                }
            }
        }
        names
    };

    let mut binder = Binder::new_for_ddl(session);
    binder.bind_columns_to_context(table_name.clone(), column_catalogs)?;

    for column in columns {
        for option_def in column.options {
            match option_def.option {
                ColumnOption::GeneratedColumns(expr) => {
                    binder.set_clause(Some(Clause::GeneratedColumn));
                    let idx = binder
                        .get_column_binding_index(table_name.clone(), &column.name.real_value())?;
                    let expr_impl = binder.bind_expr(expr).with_context(|| {
                        format!(
                            "fail to bind expression in generated column \"{}\"",
                            column.name.real_value()
                        )
                    })?;

                    check_generated_column_constraints(
                        &column.name.real_value(),
                        column_catalogs[idx].column_id(),
                        &expr_impl,
                        column_catalogs,
                        &generated_column_names,
                        pk_column_ids,
                    )?;

                    column_catalogs[idx].column_desc.generated_or_default_column = Some(
                        GeneratedOrDefaultColumn::GeneratedColumn(GeneratedColumnDesc {
                            expr: Some(expr_impl.to_expr_proto()),
                        }),
                    );
                    binder.set_clause(None);
                }
                ColumnOption::DefaultColumns(expr) => {
                    let idx = binder
                        .get_column_binding_index(table_name.clone(), &column.name.real_value())?;
                    let expr_impl = binder
                        .bind_expr(expr)?
                        .cast_assign(column_catalogs[idx].data_type().clone())?;

                    // Rewrite expressions to evaluate a snapshot value, used for missing values in the case of
                    // schema change.
                    //
                    // TODO: Currently we don't support impure expressions other than `now()` (like `random()`),
                    // so the rewritten expression should almost always be pure and we directly call `fold_const`
                    // here. Actually we do not require purity of the expression here since we're only to get a
                    // snapshot value.
                    let rewritten_expr_impl = session
                        .pinned_snapshot()
                        .inline_now_proc_time()
                        .rewrite_expr(expr_impl.clone());

                    if let Some(snapshot_value) = rewritten_expr_impl.try_fold_const() {
                        let snapshot_value = snapshot_value?;

                        column_catalogs[idx].column_desc.generated_or_default_column =
                            Some(GeneratedOrDefaultColumn::DefaultColumn(DefaultColumnDesc {
                                snapshot_value: Some(snapshot_value.to_protobuf()),
                                expr: Some(expr_impl.to_expr_proto()),
                                // persist the original expression
                            }));
                    } else {
                        return Err(ErrorCode::BindError(format!(
                            "Default expression used in column `{}` cannot be evaluated. \
                            Use generated columns instead if you mean to reference other columns.",
                            column.name
                        ))
                        .into());
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

pub fn ensure_table_constraints_supported(table_constraints: &[TableConstraint]) -> Result<()> {
    for constraint in table_constraints {
        match constraint {
            TableConstraint::Unique {
                name: _,
                columns: _,
                is_primary: true,
            } => {}
            _ => bail_not_implemented!("table constraint \"{}\"", constraint),
        }
    }
    Ok(())
}

pub fn bind_sql_pk_names(
    columns_defs: &[ColumnDef],
    table_constraints: &[TableConstraint],
) -> Result<Vec<String>> {
    let mut pk_column_names = vec![];

    for column in columns_defs {
        for option_def in &column.options {
            if let ColumnOption::Unique { is_primary: true } = option_def.option {
                if !pk_column_names.is_empty() {
                    return Err(multiple_pk_definition_err());
                }
                pk_column_names.push(column.name.real_value());
            };
        }
    }

    for constraint in table_constraints {
        if let TableConstraint::Unique {
            name: _,
            columns,
            is_primary: true,
        } = constraint
        {
            if !pk_column_names.is_empty() {
                return Err(multiple_pk_definition_err());
            }
            pk_column_names = columns.iter().map(|c| c.real_value()).collect_vec();
        }
    }
    Ok(pk_column_names)
}

fn multiple_pk_definition_err() -> RwError {
    ErrorCode::BindError("multiple primary keys are not allowed".into()).into()
}

/// Binds primary keys defined in SQL.
///
/// It returns the columns together with `pk_column_ids`, and an optional row id column index if
/// added.
pub fn bind_pk_and_row_id_on_relation(
    mut columns: Vec<ColumnCatalog>,
    pk_names: Vec<String>,
    must_need_pk: bool,
) -> Result<(Vec<ColumnCatalog>, Vec<ColumnId>, Option<usize>)> {
    for c in &columns {
        assert!(c.column_id() != ColumnId::placeholder());
    }

    // Mapping from column name to column id.
    let name_to_id = columns
        .iter()
        .map(|c| (c.name(), c.column_id()))
        .collect::<HashMap<_, _>>();

    let mut pk_column_ids: Vec<_> = pk_names
        .iter()
        .map(|name| {
            name_to_id.get(name.as_str()).copied().ok_or_else(|| {
                ErrorCode::BindError(format!("column \"{name}\" named in key does not exist"))
            })
        })
        .try_collect()?;

    // Add `_row_id` column if `pk_column_ids` is empty and must_need_pk
    let need_row_id = pk_column_ids.is_empty() && must_need_pk;

    let row_id_index = need_row_id.then(|| {
        let column = ColumnCatalog::row_id_column();
        let index = columns.len();
        pk_column_ids = vec![column.column_id()];
        columns.push(column);
        index
    });

    if let Some(col) = columns.iter().map(|c| c.name()).duplicates().next() {
        Err(ErrorCode::InvalidInputSyntax(format!(
            "column \"{col}\" specified more than once"
        )))?;
    }

    Ok((columns, pk_column_ids, row_id_index))
}

/// `gen_create_table_plan_with_source` generates the plan for creating a table with an external
/// stream source.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn gen_create_table_plan_with_source(
    mut handler_args: HandlerArgs,
    explain_options: ExplainOptions,
    table_name: ObjectName,
    column_defs: Vec<ColumnDef>,
    wildcard_idx: Option<usize>,
    constraints: Vec<TableConstraint>,
    source_schema: ConnectorSchema,
    source_watermarks: Vec<SourceWatermark>,
    mut col_id_gen: ColumnIdGenerator,
    append_only: bool,
    on_conflict: Option<OnConflict>,
    with_version_column: Option<String>,
    include_column_options: IncludeOption,
) -> Result<(PlanRef, Option<PbSource>, PbTable)> {
    if append_only
        && source_schema.format != Format::Plain
        && source_schema.format != Format::Native
    {
        return Err(ErrorCode::BindError(format!(
            "Append only table does not support format {}.",
            source_schema.format
        ))
        .into());
    }

    let session = &handler_args.session;
    let with_properties = bind_connector_props(&handler_args, &source_schema, false)?;

    let (columns_from_resolve_source, source_info) =
        bind_columns_from_source(session, &source_schema, Either::Left(&with_properties)).await?;

    let overwrite_options = OverwriteOptions::new(&mut handler_args);
    let rate_limit = overwrite_options.source_rate_limit;
    let (source_catalog, database_id, schema_id) = bind_create_source_or_table_with_connector(
        handler_args.clone(),
        table_name,
        source_schema,
        with_properties,
        &column_defs,
        constraints,
        wildcard_idx,
        source_watermarks,
        columns_from_resolve_source,
        source_info,
        include_column_options,
        &mut col_id_gen,
        false,
        rate_limit,
    )
    .await?;

    let pb_source = source_catalog.to_prost(schema_id, database_id);

    let context = OptimizerContext::new(handler_args, explain_options);

    let (plan, table) = gen_table_plan_with_source(
        context.into(),
        source_catalog,
        append_only,
        on_conflict,
        with_version_column,
        Some(col_id_gen.into_version()),
        database_id,
        schema_id,
    )?;

    Ok((plan, Some(pb_source), table))
}

/// `gen_create_table_plan` generates the plan for creating a table without an external stream
/// source.
pub(crate) fn gen_create_table_plan(
    context: OptimizerContext,
    table_name: ObjectName,
    column_defs: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    mut col_id_gen: ColumnIdGenerator,
    source_watermarks: Vec<SourceWatermark>,
    append_only: bool,
    on_conflict: Option<OnConflict>,
    with_version_column: Option<String>,
) -> Result<(PlanRef, PbTable)> {
    let definition = context.normalized_sql().to_owned();
    let mut columns = bind_sql_columns(&column_defs)?;
    for c in &mut columns {
        c.column_desc.column_id = col_id_gen.generate(c.name())
    }

    let (_, secret_refs) = context.with_options().clone().into_parts();
    if !secret_refs.is_empty() {
        return Err(crate::error::ErrorCode::InvalidParameterValue("Secret reference is not allowed in options when creating table without external source".to_string()).into());
    }

    gen_create_table_plan_without_source(
        context,
        table_name,
        columns,
        column_defs,
        constraints,
        definition,
        source_watermarks,
        append_only,
        on_conflict,
        with_version_column,
        Some(col_id_gen.into_version()),
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn gen_create_table_plan_without_source(
    context: OptimizerContext,
    table_name: ObjectName,
    columns: Vec<ColumnCatalog>,
    column_defs: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    definition: String,
    source_watermarks: Vec<SourceWatermark>,
    append_only: bool,
    on_conflict: Option<OnConflict>,
    with_version_column: Option<String>,
    version: Option<TableVersion>,
) -> Result<(PlanRef, PbTable)> {
    ensure_table_constraints_supported(&constraints)?;
    let pk_names = bind_sql_pk_names(&column_defs, &constraints)?;
    let (mut columns, pk_column_ids, row_id_index) =
        bind_pk_and_row_id_on_relation(columns, pk_names, true)?;

    let watermark_descs: Vec<WatermarkDesc> = bind_source_watermark(
        context.session_ctx(),
        table_name.real_value(),
        source_watermarks,
        &columns,
    )?;

    bind_sql_column_constraints(
        context.session_ctx(),
        table_name.real_value(),
        &mut columns,
        column_defs,
        &pk_column_ids,
    )?;
    let session = context.session_ctx().clone();

    let db_name = session.database();
    let (schema_name, name) = Binder::resolve_schema_qualified_name(db_name, table_name)?;
    let (database_id, schema_id) =
        session.get_database_and_schema_id_for_create(schema_name.clone())?;

    gen_table_plan_inner(
        context.into(),
        name,
        columns,
        pk_column_ids,
        row_id_index,
        definition,
        watermark_descs,
        append_only,
        on_conflict,
        with_version_column,
        version,
        None,
        database_id,
        schema_id,
    )
}

fn gen_table_plan_with_source(
    context: OptimizerContextRef,
    source_catalog: SourceCatalog,
    append_only: bool,
    on_conflict: Option<OnConflict>,
    with_version_column: Option<String>,
    version: Option<TableVersion>, /* TODO: this should always be `Some` if we support `ALTER
                                    * TABLE` for `CREATE TABLE AS`. */
    database_id: DatabaseId,
    schema_id: SchemaId,
) -> Result<(PlanRef, PbTable)> {
    let cloned_source_catalog = source_catalog.clone();
    gen_table_plan_inner(
        context,
        source_catalog.name,
        source_catalog.columns,
        source_catalog.pk_col_ids,
        source_catalog.row_id_index,
        source_catalog.definition,
        source_catalog.watermark_descs,
        append_only,
        on_conflict,
        with_version_column,
        version,
        Some(cloned_source_catalog),
        database_id,
        schema_id,
    )
}

#[allow(clippy::too_many_arguments)]
fn gen_table_plan_inner(
    context: OptimizerContextRef,
    table_name: String,
    columns: Vec<ColumnCatalog>,
    pk_column_ids: Vec<ColumnId>,
    row_id_index: Option<usize>,
    definition: String,
    watermark_descs: Vec<WatermarkDesc>,
    append_only: bool,
    on_conflict: Option<OnConflict>,
    with_version_column: Option<String>,
    version: Option<TableVersion>, /* TODO: this should always be `Some` if we support `ALTER
                                    * TABLE` for `CREATE TABLE AS`. */
    source_catalog: Option<SourceCatalog>,
    database_id: DatabaseId,
    schema_id: SchemaId,
) -> Result<(PlanRef, PbTable)> {
    let session = context.session_ctx().clone();
    let retention_seconds = context.with_options().retention_seconds();
    let is_external_source = source_catalog.is_some();

    let source_node: PlanRef = LogicalSource::new(
        source_catalog.map(|source| Rc::new(source.clone())),
        columns.clone(),
        row_id_index,
        SourceNodeKind::CreateTable,
        context.clone(),
        None,
    )?
    .into();

    let required_cols = FixedBitSet::with_capacity(columns.len());
    let plan_root = PlanRoot::new_with_logical_plan(
        source_node,
        RequiredDist::Any,
        Order::any(),
        required_cols,
        vec![],
    );

    if append_only && row_id_index.is_none() {
        return Err(ErrorCode::InvalidInputSyntax(
            "PRIMARY KEY constraint can not be applied to an append-only table.".to_owned(),
        )
        .into());
    }

    if !append_only && !watermark_descs.is_empty() {
        return Err(ErrorCode::NotSupported(
            "Defining watermarks on table requires the table to be append only.".to_owned(),
            "Use the key words `APPEND ONLY`".to_owned(),
        )
        .into());
    }

    if !append_only && retention_seconds.is_some() {
        return Err(ErrorCode::NotSupported(
            "Defining retention seconds on table requires the table to be append only.".to_owned(),
            "Use the key words `APPEND ONLY`".to_owned(),
        )
        .into());
    }

    let materialize = plan_root.gen_table_plan(
        context,
        table_name,
        columns,
        definition,
        pk_column_ids,
        row_id_index,
        append_only,
        on_conflict,
        with_version_column,
        watermark_descs,
        version,
        is_external_source,
        retention_seconds,
        None,
    )?;

    let mut table = materialize.table().to_prost(schema_id, database_id);

    table.owner = session.user_id();
    Ok((materialize.into(), table))
}

/// Generate stream plan for cdc table based on shared source.
/// In replace workflow, the `table_id` is the id of the table to be replaced
/// in create table workflow, the `table_id` is a placeholder will be filled in the Meta
#[allow(clippy::too_many_arguments)]
pub(crate) fn gen_create_table_plan_for_cdc_table(
    context: OptimizerContextRef,
    source: Arc<SourceCatalog>,
    external_table_name: String,
    column_defs: Vec<ColumnDef>,
    mut columns: Vec<ColumnCatalog>,
    pk_names: Vec<String>,
    connect_properties: WithOptionsSecResolved,
    mut col_id_gen: ColumnIdGenerator,
    on_conflict: Option<OnConflict>,
    with_version_column: Option<String>,
    include_column_options: IncludeOption,
    table_name: ObjectName,
    resolved_table_name: String, // table name without schema prefix
    database_id: DatabaseId,
    schema_id: SchemaId,
    table_id: TableId,
) -> Result<(PlanRef, PbTable)> {
    let session = context.session_ctx().clone();

    // append additional columns to the end
    handle_addition_columns(
        None,
        &connect_properties,
        include_column_options,
        &mut columns,
        true,
    )?;

    for c in &mut columns {
        c.column_desc.column_id = col_id_gen.generate(c.name())
    }

    let (mut columns, pk_column_ids, _row_id_index) =
        bind_pk_and_row_id_on_relation(columns, pk_names, true)?;

    // NOTES: In auto schema change, default value is not provided in column definition.
    bind_sql_column_constraints(
        context.session_ctx(),
        table_name.real_value(),
        &mut columns,
        column_defs,
        &pk_column_ids,
    )?;

    let definition = context.normalized_sql().to_owned();

    let pk_column_indices = {
        let mut id_to_idx = HashMap::new();
        columns.iter().enumerate().for_each(|(idx, c)| {
            id_to_idx.insert(c.column_id(), idx);
        });
        // pk column id must exist in table columns.
        pk_column_ids
            .iter()
            .map(|c| id_to_idx.get(c).copied().unwrap())
            .collect_vec()
    };
    let table_pk = pk_column_indices
        .iter()
        .map(|idx| ColumnOrder::new(*idx, OrderType::ascending()))
        .collect();

    let (options, secret_refs) = connect_properties.into_parts();

    let cdc_table_desc = CdcTableDesc {
        table_id,
        source_id: source.id.into(), // id of cdc source streaming job
        external_table_name: external_table_name.clone(),
        pk: table_pk,
        columns: columns.iter().map(|c| c.column_desc.clone()).collect(),
        stream_key: pk_column_indices,
        connect_properties: options,
        secret_refs,
    };

    tracing::debug!(?cdc_table_desc, "create cdc table");

    let options = CdcScanOptions::from_with_options(context.with_options())?;

    let logical_scan = LogicalCdcScan::create(
        external_table_name.clone(),
        Rc::new(cdc_table_desc),
        context.clone(),
        options,
    );

    let scan_node: PlanRef = logical_scan.into();
    let required_cols = FixedBitSet::with_capacity(columns.len());
    let plan_root = PlanRoot::new_with_logical_plan(
        scan_node,
        RequiredDist::Any,
        Order::any(),
        required_cols,
        vec![],
    );

    let cdc_table_id = build_cdc_table_id(source.id, &external_table_name);
    let materialize = plan_root.gen_table_plan(
        context,
        resolved_table_name,
        columns,
        definition,
        pk_column_ids,
        None,
        false,
        on_conflict,
        with_version_column,
        vec![],
        Some(col_id_gen.into_version()),
        true,
        None,
        Some(cdc_table_id),
    )?;

    let mut table = materialize.table().to_prost(schema_id, database_id);
    table.owner = session.user_id();
    table.dependent_relations = vec![source.id];

    Ok((materialize.into(), table))
}

fn derive_connect_properties(
    source_with_properties: &WithOptionsSecResolved,
    external_table_name: String,
) -> Result<WithOptionsSecResolved> {
    use source::cdc::{MYSQL_CDC_CONNECTOR, POSTGRES_CDC_CONNECTOR, SQL_SERVER_CDC_CONNECTOR};
    // we should remove the prefix from `full_table_name`
    let mut connect_properties = source_with_properties.clone();
    if let Some(connector) = source_with_properties.get(UPSTREAM_SOURCE_KEY) {
        let table_name = match connector.as_str() {
            MYSQL_CDC_CONNECTOR => {
                let db_name = connect_properties.get(DATABASE_NAME_KEY).ok_or_else(|| {
                    anyhow!("{} not found in source properties", DATABASE_NAME_KEY)
                })?;

                let prefix = format!("{}.", db_name.as_str());
                external_table_name
                    .strip_prefix(prefix.as_str())
                    .ok_or_else(|| anyhow!("The upstream table name must contain database name prefix, e.g. 'mydb.table'."))?
            }
            POSTGRES_CDC_CONNECTOR => {
                let (schema_name, table_name) = external_table_name
                    .split_once('.')
                    .ok_or_else(|| anyhow!("The upstream table name must contain schema name prefix, e.g. 'public.table'"))?;

                // insert 'schema.name' into connect properties
                connect_properties.insert(SCHEMA_NAME_KEY.into(), schema_name.into());

                table_name
            }
            SQL_SERVER_CDC_CONNECTOR => {
                let (schema_name, table_name) = external_table_name
                    .split_once('.')
                    .ok_or_else(|| anyhow!("The upstream table name must contain schema name prefix, e.g. 'dbo.table'"))?;

                // insert 'schema.name' into connect properties
                connect_properties.insert(SCHEMA_NAME_KEY.into(), schema_name.into());

                table_name
            }
            _ => {
                return Err(RwError::from(anyhow!(
                    "connector {} is not supported for cdc table",
                    connector
                )));
            }
        };
        connect_properties.insert(TABLE_NAME_KEY.into(), table_name.into());
    }
    Ok(connect_properties)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_create_table_plan(
    handler_args: HandlerArgs,
    explain_options: ExplainOptions,
    source_schema: Option<ConnectorSchema>,
    cdc_table_info: Option<CdcTableInfo>,
    table_name: ObjectName,
    column_defs: Vec<ColumnDef>,
    wildcard_idx: Option<usize>,
    constraints: Vec<TableConstraint>,
    source_watermarks: Vec<SourceWatermark>,
    append_only: bool,
    on_conflict: Option<OnConflict>,
    with_version_column: Option<String>,
    include_column_options: IncludeOption,
) -> Result<(PlanRef, Option<PbSource>, PbTable, TableJobType)> {
    let col_id_gen = ColumnIdGenerator::new_initial();
    let source_schema = check_create_table_with_source(
        &handler_args.with_options,
        source_schema,
        &include_column_options,
        &cdc_table_info,
    )?;

    let ((plan, source, table), job_type) =
        match (source_schema, cdc_table_info.as_ref()) {
            (Some(source_schema), None) => (
                gen_create_table_plan_with_source(
                    handler_args,
                    explain_options,
                    table_name.clone(),
                    column_defs,
                    wildcard_idx,
                    constraints,
                    source_schema,
                    source_watermarks,
                    col_id_gen,
                    append_only,
                    on_conflict,
                    with_version_column,
                    include_column_options,
                )
                .await?,
                TableJobType::General,
            ),
            (None, None) => {
                let context = OptimizerContext::new(handler_args, explain_options);
                let (plan, table) = gen_create_table_plan(
                    context,
                    table_name.clone(),
                    column_defs,
                    constraints,
                    col_id_gen,
                    source_watermarks,
                    append_only,
                    on_conflict,
                    with_version_column,
                )?;

                ((plan, None, table), TableJobType::General)
            }

            (None, Some(cdc_table)) => {
                sanity_check_for_cdc_table(
                    append_only,
                    &column_defs,
                    &wildcard_idx,
                    &constraints,
                    &source_watermarks,
                )?;

                let session = &handler_args.session;
                let db_name = session.database();
                let (schema_name, resolved_table_name) =
                    Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
                let (database_id, schema_id) =
                    session.get_database_and_schema_id_for_create(schema_name.clone())?;

                // cdc table cannot be append-only
                let (source_schema, source_name) =
                    Binder::resolve_schema_qualified_name(db_name, cdc_table.source_name.clone())?;

                let source = {
                    let catalog_reader = session.env().catalog_reader().read_guard();
                    let schema_name = source_schema
                        .clone()
                        .unwrap_or(DEFAULT_SCHEMA_NAME.to_string());
                    let (source, _) = catalog_reader.get_source_by_name(
                        db_name,
                        SchemaPath::Name(schema_name.as_str()),
                        source_name.as_str(),
                    )?;
                    source.clone()
                };
                let connect_properties = derive_connect_properties(
                    &source.with_properties,
                    cdc_table.external_table_name.clone(),
                )?;

                let (columns, pk_names) = derive_schema_for_cdc_table(
                    &column_defs,
                    &constraints,
                    connect_properties.clone(),
                    wildcard_idx.is_some(),
                    None,
                )
                .await?;

                let context: OptimizerContextRef =
                    OptimizerContext::new(handler_args, explain_options).into();
                let (plan, table) = gen_create_table_plan_for_cdc_table(
                    context,
                    source,
                    cdc_table.external_table_name.clone(),
                    column_defs,
                    columns,
                    pk_names,
                    connect_properties,
                    col_id_gen,
                    on_conflict,
                    with_version_column,
                    include_column_options,
                    table_name,
                    resolved_table_name,
                    database_id,
                    schema_id,
                    TableId::placeholder(),
                )?;

                ((plan, None, table), TableJobType::SharedCdcSource)
            }
            (Some(_), Some(_)) => return Err(ErrorCode::NotSupported(
                "Data format and encoding format doesn't apply to table created from a CDC source"
                    .into(),
                "Remove the FORMAT and ENCODE specification".into(),
            )
            .into()),
        };
    Ok((plan, source, table, job_type))
}

fn sanity_check_for_cdc_table(
    append_only: bool,
    column_defs: &Vec<ColumnDef>,
    wildcard_idx: &Option<usize>,
    constraints: &Vec<TableConstraint>,
    source_watermarks: &Vec<SourceWatermark>,
) -> Result<()> {
    for c in column_defs {
        for op in &c.options {
            if let ColumnOption::GeneratedColumns(_) = op.option {
                return Err(ErrorCode::NotSupported(
                    "generated column defined on the table created from a CDC source".into(),
                    "Remove the generated column in the column list".into(),
                )
                .into());
            }
        }
    }

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

    if !source_watermarks.is_empty() {
        return Err(ErrorCode::NotSupported(
            "watermark defined on the table created from a CDC source".into(),
            "Remove the Watermark definitions".into(),
        )
        .into());
    }

    Ok(())
}

struct CdcSchemaChangeArgs {
    /// original table catalog
    original_catalog: Arc<TableCatalog>,
    /// new version table columns, only provided in auto schema change
    new_version_columns: Option<Vec<ColumnCatalog>>,
}

/// Derive schema for cdc table when create a new Table or alter an existing Table
async fn derive_schema_for_cdc_table(
    column_defs: &Vec<ColumnDef>,
    constraints: &Vec<TableConstraint>,
    connect_properties: WithOptionsSecResolved,
    need_auto_schema_map: bool,
    schema_change_args: Option<CdcSchemaChangeArgs>,
) -> Result<(Vec<ColumnCatalog>, Vec<String>)> {
    // read cdc table schema from external db or parsing the schema from SQL definitions
    if need_auto_schema_map {
        Feature::CdcTableSchemaMap
            .check_available()
            .map_err(|err| {
                ErrorCode::NotSupported(
                    err.to_report_string(),
                    "Please define the schema manually".to_owned(),
                )
            })?;
        let (options, secret_refs) = connect_properties.into_parts();
        let config = ExternalTableConfig::try_from_btreemap(options, secret_refs)
            .context("failed to extract external table config")?;

        let table = ExternalTableImpl::connect(config)
            .await
            .context("failed to auto derive table schema")?;
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
            table.pk_names().clone(),
        ))
    } else {
        let mut columns = bind_sql_columns(column_defs)?;
        let pk_names = if let Some(args) = schema_change_args {
            // If new_version_columns is provided, we are in the process of auto schema change.
            // update the default value column since the default value column is not set in the
            // column sql definition.
            if let Some(new_version_columns) = args.new_version_columns {
                for (col, new_version_col) in columns
                    .iter_mut()
                    .zip_eq_fast(new_version_columns.into_iter())
                {
                    assert_eq!(col.name(), new_version_col.name());
                    col.column_desc.generated_or_default_column =
                        new_version_col.column_desc.generated_or_default_column;
                }
            }

            // For table created by `create table t (*)` the constraint is empty, we need to
            // retrieve primary key names from original table catalog if available
            args.original_catalog
                .pk
                .iter()
                .map(|x| {
                    args.original_catalog.columns[x.column_index]
                        .name()
                        .to_string()
                })
                .collect()
        } else {
            bind_sql_pk_names(column_defs, constraints)?
        };

        Ok((columns, pk_names))
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_create_table(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    column_defs: Vec<ColumnDef>,
    wildcard_idx: Option<usize>,
    constraints: Vec<TableConstraint>,
    if_not_exists: bool,
    source_schema: Option<ConnectorSchema>,
    source_watermarks: Vec<SourceWatermark>,
    append_only: bool,
    on_conflict: Option<OnConflict>,
    with_version_column: Option<String>,
    cdc_table_info: Option<CdcTableInfo>,
    include_column_options: IncludeOption,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();

    if append_only {
        session.notice_to_user("APPEND ONLY TABLE is currently an experimental feature.");
    }

    session.check_cluster_limits().await?;

    if let Either::Right(resp) = session.check_relation_name_duplicated(
        table_name.clone(),
        StatementType::CREATE_TABLE,
        if_not_exists,
    )? {
        return Ok(resp);
    }

    let (graph, source, table, job_type) = {
        let (plan, source, table, job_type) = handle_create_table_plan(
            handler_args,
            ExplainOptions::default(),
            source_schema,
            cdc_table_info,
            table_name.clone(),
            column_defs,
            wildcard_idx,
            constraints,
            source_watermarks,
            append_only,
            on_conflict,
            with_version_column,
            include_column_options,
        )
        .await?;

        let graph = build_graph(plan)?;

        (graph, source, table, job_type)
    };

    tracing::trace!(
        "name={}, graph=\n{}",
        table_name,
        serde_json::to_string_pretty(&graph).unwrap()
    );

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .create_table(source, table, graph, job_type)
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_TABLE))
}

pub fn check_create_table_with_source(
    with_options: &WithOptions,
    source_schema: Option<ConnectorSchema>,
    include_column_options: &IncludeOption,
    cdc_table_info: &Option<CdcTableInfo>,
) -> Result<Option<ConnectorSchema>> {
    // skip check for cdc table
    if cdc_table_info.is_some() {
        return Ok(source_schema);
    }
    let defined_source = with_options.contains_key(UPSTREAM_SOURCE_KEY);
    if !include_column_options.is_empty() && !defined_source {
        return Err(ErrorCode::InvalidInputSyntax(
            "INCLUDE should be used with a connector".to_owned(),
        )
        .into());
    }
    if defined_source {
        source_schema.as_ref().ok_or_else(|| {
            ErrorCode::InvalidInputSyntax("Please specify a source schema using FORMAT".to_owned())
        })?;
    }
    Ok(source_schema)
}

#[allow(clippy::too_many_arguments)]
pub async fn generate_stream_graph_for_table(
    _session: &Arc<SessionImpl>,
    table_name: ObjectName,
    original_catalog: &Arc<TableCatalog>,
    source_schema: Option<ConnectorSchema>,
    handler_args: HandlerArgs,
    col_id_gen: ColumnIdGenerator,
    column_defs: Vec<ColumnDef>,
    wildcard_idx: Option<usize>,
    constraints: Vec<TableConstraint>,
    source_watermarks: Vec<SourceWatermark>,
    append_only: bool,
    on_conflict: Option<OnConflict>,
    with_version_column: Option<String>,
    cdc_table_info: Option<CdcTableInfo>,
    new_version_columns: Option<Vec<ColumnCatalog>>,
) -> Result<(StreamFragmentGraph, Table, Option<PbSource>, TableJobType)> {
    use risingwave_pb::catalog::table::OptionalAssociatedSourceId;

    let ((plan, source, table), job_type) = match (source_schema, cdc_table_info.as_ref()) {
        (Some(source_schema), None) => (
            gen_create_table_plan_with_source(
                handler_args,
                ExplainOptions::default(),
                table_name,
                column_defs,
                wildcard_idx,
                constraints,
                source_schema,
                source_watermarks,
                col_id_gen,
                append_only,
                on_conflict,
                with_version_column,
                vec![],
            )
            .await?,
            TableJobType::General,
        ),
        (None, None) => {
            let context = OptimizerContext::from_handler_args(handler_args);
            let (plan, table) = gen_create_table_plan(
                context,
                table_name,
                column_defs,
                constraints,
                col_id_gen,
                source_watermarks,
                append_only,
                on_conflict,
                with_version_column,
            )?;
            ((plan, None, table), TableJobType::General)
        }
        (None, Some(cdc_table)) => {
            let session = &handler_args.session;
            let (source, resolved_table_name, database_id, schema_id) =
                get_source_and_resolved_table_name(session, cdc_table.clone(), table_name.clone())?;

            let connect_properties = derive_connect_properties(
                &source.with_properties,
                cdc_table.external_table_name.clone(),
            )?;

            let (columns, pk_names) = derive_schema_for_cdc_table(
                &column_defs,
                &constraints,
                connect_properties.clone(),
                false,
                Some(CdcSchemaChangeArgs {
                    original_catalog: original_catalog.clone(),
                    new_version_columns,
                }),
            )
            .await?;

            let context: OptimizerContextRef =
                OptimizerContext::new(handler_args, ExplainOptions::default()).into();
            let (plan, table) = gen_create_table_plan_for_cdc_table(
                context,
                source,
                cdc_table.external_table_name.clone(),
                column_defs,
                columns,
                pk_names,
                connect_properties,
                col_id_gen,
                on_conflict,
                with_version_column,
                IncludeOption::default(),
                table_name,
                resolved_table_name,
                database_id,
                schema_id,
                original_catalog.id(),
            )?;

            ((plan, None, table), TableJobType::SharedCdcSource)
        }
        (Some(_), Some(_)) => {
            return Err(ErrorCode::NotSupported(
                "Data format and encoding format doesn't apply to table created from a CDC source"
                    .into(),
                "Remove the FORMAT and ENCODE specification".into(),
            )
            .into());
        }
    };

    // TODO: avoid this backward conversion.
    if TableCatalog::from(&table).pk_column_ids() != original_catalog.pk_column_ids() {
        Err(ErrorCode::InvalidInputSyntax(
            "alter primary key of table is not supported".to_owned(),
        ))?
    }

    let graph = build_graph(plan)?;

    // Fill the original table ID.
    let table = Table {
        id: original_catalog.id().table_id(),
        optional_associated_source_id: original_catalog
            .associated_source_id()
            .map(|source_id| OptionalAssociatedSourceId::AssociatedSourceId(source_id.into())),
        ..table
    };

    Ok((graph, table, source, job_type))
}

fn get_source_and_resolved_table_name(
    session: &Arc<SessionImpl>,
    cdc_table: CdcTableInfo,
    table_name: ObjectName,
) -> Result<(Arc<SourceCatalog>, String, DatabaseId, SchemaId)> {
    let db_name = session.database();
    let (schema_name, resolved_table_name) =
        Binder::resolve_schema_qualified_name(db_name, table_name)?;
    let (database_id, schema_id) =
        session.get_database_and_schema_id_for_create(schema_name.clone())?;

    let (source_schema, source_name) =
        Binder::resolve_schema_qualified_name(db_name, cdc_table.source_name.clone())?;

    let source = {
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_name = source_schema.unwrap_or(DEFAULT_SCHEMA_NAME.to_string());
        let (source, _) = catalog_reader.get_source_by_name(
            db_name,
            SchemaPath::Name(schema_name.as_str()),
            source_name.as_str(),
        )?;
        source.clone()
    };

    Ok((source, resolved_table_name, database_id, schema_id))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{Field, DEFAULT_DATABASE_NAME, ROWID_PREFIX};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[test]
    fn test_col_id_gen() {
        let mut gen = ColumnIdGenerator::new_initial();
        assert_eq!(gen.generate("v1"), ColumnId::new(1));
        assert_eq!(gen.generate("v2"), ColumnId::new(2));

        let mut gen = ColumnIdGenerator::new_alter(&TableCatalog {
            columns: vec![
                ColumnCatalog {
                    column_desc: ColumnDesc::from_field_with_column_id(
                        &Field::with_name(DataType::Float32, "f32"),
                        1,
                    ),
                    is_hidden: false,
                },
                ColumnCatalog {
                    column_desc: ColumnDesc::from_field_with_column_id(
                        &Field::with_name(DataType::Float64, "f64"),
                        2,
                    ),
                    is_hidden: false,
                },
            ],
            version: Some(TableVersion::new_initial_for_test(ColumnId::new(2))),
            ..Default::default()
        });

        assert_eq!(gen.generate("v1"), ColumnId::new(3));
        assert_eq!(gen.generate("v2"), ColumnId::new(4));
        assert_eq!(gen.generate("f32"), ColumnId::new(1));
        assert_eq!(gen.generate("f64"), ColumnId::new(2));
        assert_eq!(gen.generate("v3"), ColumnId::new(5));
    }

    #[tokio::test]
    async fn test_create_table_handler() {
        let sql =
            "create table t (v1 smallint, v2 struct<v3 bigint, v4 float, v5 double>) append only;";
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check table exists.
        let (table, _) = catalog_reader
            .get_created_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
            .unwrap();
        assert_eq!(table.name(), "t");

        let columns = table
            .columns
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<HashMap<&str, DataType>>();

        let expected_columns = maplit::hashmap! {
            ROWID_PREFIX => DataType::Serial,
            "v1" => DataType::Int16,
            "v2" => DataType::new_struct(
                vec![DataType::Int64,DataType::Float64,DataType::Float64],
                vec!["v3".to_string(), "v4".to_string(), "v5".to_string()],
            ),
        };

        assert_eq!(columns, expected_columns);
    }

    #[test]
    fn test_bind_primary_key() {
        // Note: Column ID 0 is reserved for row ID column.

        for (sql, expected) in [
            ("create table t (v1 int, v2 int)", Ok(&[0] as &[_])),
            ("create table t (v1 int primary key, v2 int)", Ok(&[1])),
            ("create table t (v1 int, v2 int primary key)", Ok(&[2])),
            (
                "create table t (v1 int primary key, v2 int primary key)",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int primary key primary key, v2 int)",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int, v2 int, primary key (v1))",
                Ok(&[1]),
            ),
            (
                "create table t (v1 int, primary key (v2), v2 int)",
                Ok(&[2]),
            ),
            (
                "create table t (primary key (v2, v1), v1 int, v2 int)",
                Ok(&[2, 1]),
            ),
            (
                "create table t (v1 int, primary key (v1), v2 int, primary key (v1))",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int primary key, primary key (v1), v2 int)",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int, primary key (V3), v2 int)",
                Err("column \"v3\" named in key does not exist"),
            ),
        ] {
            let mut ast = risingwave_sqlparser::parser::Parser::parse_sql(sql).unwrap();
            let risingwave_sqlparser::ast::Statement::CreateTable {
                columns: column_defs,
                constraints,
                ..
            } = ast.remove(0)
            else {
                panic!("test case should be create table")
            };
            let actual: Result<_> = (|| {
                let mut columns = bind_sql_columns(&column_defs)?;
                let mut col_id_gen = ColumnIdGenerator::new_initial();
                for c in &mut columns {
                    c.column_desc.column_id = col_id_gen.generate(c.name())
                }
                ensure_table_constraints_supported(&constraints)?;
                let pk_names = bind_sql_pk_names(&column_defs, &constraints)?;
                let (_, pk_column_ids, _) =
                    bind_pk_and_row_id_on_relation(columns, pk_names, true)?;
                Ok(pk_column_ids)
            })();
            match (expected, actual) {
                (Ok(expected), Ok(actual)) => assert_eq!(
                    expected.iter().copied().map(ColumnId::new).collect_vec(),
                    actual,
                    "sql: {sql}"
                ),
                (Ok(_), Err(actual)) => panic!("sql: {sql}\nunexpected error: {actual:?}"),
                (Err(_), Ok(actual)) => panic!("sql: {sql}\nexpects error but got: {actual:?}"),
                (Err(expected), Err(actual)) => assert!(
                    actual.to_string().contains(expected),
                    "sql: {sql}\nexpected: {expected:?}\nactual: {actual:?}"
                ),
            }
        }
    }

    #[tokio::test]
    async fn test_duplicate_props_options() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE TABLE t
    WITH (
        connector = 'kinesis',
        aws.region='user_test_topic',
        endpoint='172.10.1.1:9090,172.10.1.2:9090',
        aws.credentials.access_key_id = 'your_access_key_1',
        aws.credentials.secret_access_key = 'your_secret_key_1'
    )
    FORMAT PLAIN ENCODE PROTOBUF (
        message = '.test.TestRecord',
        aws.credentials.access_key_id = 'your_access_key_2',
        aws.credentials.secret_access_key = 'your_secret_key_2',
        schema.location = 'file://{}',
    )"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
            .unwrap();
        assert_eq!(source.name, "t");

        // AwsAuth params exist in options.
        assert_eq!(
            source
                .info
                .format_encode_options
                .get("aws.credentials.access_key_id")
                .unwrap(),
            "your_access_key_2"
        );
        assert_eq!(
            source
                .info
                .format_encode_options
                .get("aws.credentials.secret_access_key")
                .unwrap(),
            "your_secret_key_2"
        );

        // AwsAuth params exist in props.
        assert_eq!(
            source
                .with_properties
                .get("aws.credentials.access_key_id")
                .unwrap(),
            "your_access_key_1"
        );
        assert_eq!(
            source
                .with_properties
                .get("aws.credentials.secret_access_key")
                .unwrap(),
            "your_secret_key_1"
        );

        // Options are not merged into props.
        assert!(!source.with_properties.contains_key("schema.location"));
    }
}
