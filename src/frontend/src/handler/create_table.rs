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

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use anyhow::anyhow;
use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{
    CdcTableDesc, ColumnCatalog, ColumnDesc, TableId, TableVersionId, DEFAULT_SCHEMA_NAME,
    INITIAL_SOURCE_VERSION_ID, INITIAL_TABLE_VERSION_ID, USER_COLUMN_ID_OFFSET,
};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_common::util::value_encoding::DatumToProtoExt;
use risingwave_connector::source;
use risingwave_connector::source::cdc::{CDC_SNAPSHOT_BACKFILL, CDC_SNAPSHOT_MODE_KEY};
use risingwave_connector::source::external::{
    CdcTableType, DATABASE_NAME_KEY, SCHEMA_NAME_KEY, TABLE_NAME_KEY,
};
use risingwave_pb::catalog::source::OptionalAssociatedTableId;
use risingwave_pb::catalog::{PbSource, PbTable, StreamSourceInfo, WatermarkDesc};
use risingwave_pb::ddl_service::TableJobType;
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_pb::plan_common::{DefaultColumnDesc, GeneratedColumnDesc};
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_sqlparser::ast::{
    CdcTableInfo, ColumnDef, ColumnOption, ConnectorSchema, DataType as AstDataType, Format,
    ObjectName, SourceWatermark, TableConstraint,
};

use super::RwPgResponse;
use crate::binder::{bind_data_type, bind_struct_field, Clause};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::table_catalog::TableVersion;
use crate::catalog::{check_valid_column_name, CatalogError, ColumnId};
use crate::expr::{Expr, ExprImpl, ExprRewriter, InlineNowProcTime};
use crate::handler::create_source::{
    bind_all_columns, bind_columns_from_source, bind_source_pk, bind_source_watermark,
    check_source_schema, validate_compatibility, UPSTREAM_SOURCE_KEY,
};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::{LogicalScan, LogicalSource};
use crate::optimizer::property::{Order, RequiredDist};
use crate::optimizer::{OptimizerContext, OptimizerContextRef, PlanRef, PlanRoot};
use crate::session::{CheckRelationError, SessionImpl};
use crate::stream_fragmenter::build_graph;
use crate::utils::resolve_privatelink_in_with_option;
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
            next_column_id: ColumnId::from(USER_COLUMN_ID_OFFSET),
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
            _ => {
                return Err(ErrorCode::NotImplemented(
                    format!("column constraints \"{}\"", option_def),
                    None.into(),
                )
                .into())
            }
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
                return Err(ErrorCode::NotImplemented(
                    "Collate collation other than `C` or `POSIX` is not implemented".into(),
                    None.into(),
                )
                .into());
            }

            match data_type {
                AstDataType::Text | AstDataType::Varchar | AstDataType::Char(_) => {}
                _ => {
                    return Err(ErrorCode::NotSupported(
                        format!("{} is not a collatable data type", data_type),
                        "The only built-in collatable data types are `varchar`, please check your type".into()
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
    binder.bind_columns_to_context(table_name.clone(), column_catalogs.to_vec())?;

    for column in columns {
        for option_def in column.options {
            match option_def.option {
                ColumnOption::GeneratedColumns(expr) => {
                    binder.set_clause(Some(Clause::GeneratedColumn));
                    let idx = binder
                        .get_column_binding_index(table_name.clone(), &column.name.real_value())?;
                    let expr_impl = binder.bind_expr(expr)?;

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
                    let rewritten_expr_impl =
                        InlineNowProcTime::new(session.pinned_snapshot().epoch())
                            .rewrite_expr(expr_impl.clone());

                    if let Some(snapshot_value) = rewritten_expr_impl.try_fold_const() {
                        let snapshot_value = snapshot_value?;

                        column_catalogs[idx].column_desc.generated_or_default_column = Some(
                            GeneratedOrDefaultColumn::DefaultColumn(DefaultColumnDesc {
                                snapshot_value: Some(snapshot_value.to_protobuf()),
                                expr: Some(expr_impl.to_expr_proto()), /* persist the original expression */
                            }),
                        );
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
            _ => {
                return Err(ErrorCode::NotImplemented(
                    format!("table constraint \"{}\"", constraint),
                    None.into(),
                )
                .into())
            }
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
pub fn bind_pk_on_relation(
    mut columns: Vec<ColumnCatalog>,
    pk_names: Vec<String>,
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

    // Add `_row_id` column if `pk_column_ids` is empty.
    let row_id_index = pk_column_ids.is_empty().then(|| {
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
    context: OptimizerContext,
    table_name: ObjectName,
    column_defs: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    source_schema: ConnectorSchema,
    source_watermarks: Vec<SourceWatermark>,
    mut col_id_gen: ColumnIdGenerator,
    append_only: bool,
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

    let session = context.session_ctx();
    let mut properties = context.with_options().inner().clone().into_iter().collect();
    validate_compatibility(&source_schema, &mut properties)?;

    ensure_table_constraints_supported(&constraints)?;

    let sql_pk_names = bind_sql_pk_names(&column_defs, &constraints)?;

    let (columns_from_resolve_source, mut source_info) =
        bind_columns_from_source(&source_schema, &properties, false).await?;
    let columns_from_sql = bind_sql_columns(&column_defs)?;

    let mut columns = bind_all_columns(
        &source_schema,
        columns_from_resolve_source,
        columns_from_sql,
        &column_defs,
    )?;
    let pk_names = bind_source_pk(
        &source_schema,
        &source_info,
        &mut columns,
        sql_pk_names,
        &properties,
    )
    .await?;

    for c in &mut columns {
        c.column_desc.column_id = col_id_gen.generate(c.name())
    }

    let (mut columns, pk_column_ids, row_id_index) = bind_pk_on_relation(columns, pk_names)?;

    let watermark_descs = bind_source_watermark(
        session,
        table_name.real_value(),
        source_watermarks,
        &columns,
    )?;
    // TODO(yuhao): allow multiple watermark on source.
    assert!(watermark_descs.len() <= 1);

    let definition = context.normalized_sql().to_owned();

    bind_sql_column_constraints(
        session,
        table_name.real_value(),
        &mut columns,
        column_defs,
        &pk_column_ids,
    )?;

    check_source_schema(&properties, row_id_index, &columns)?;

    if row_id_index.is_none() && columns.iter().any(|c| c.is_generated()) {
        // TODO(yuhao): allow delete from a non append only source
        return Err(ErrorCode::BindError(
            "Generated columns are only allowed in an append only source.".to_string(),
        )
        .into());
    }

    let cdc_table_type = CdcTableType::from_properties(&properties);
    if cdc_table_type.can_backfill() && context.session_ctx().config().get_cdc_backfill() {
        // debezium connector will only consume changelogs from latest offset on this mode
        properties.insert(CDC_SNAPSHOT_MODE_KEY.into(), CDC_SNAPSHOT_BACKFILL.into());

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

        let cdc_table_desc = CdcTableDesc {
            table_id: TableId::placeholder(),
            external_table_name: "".to_string(),
            pk: table_pk,
            columns: columns.iter().map(|c| c.column_desc.clone()).collect(),
            stream_key: pk_column_indices,
            value_indices: (0..columns.len()).collect_vec(),
            connect_properties: Default::default(),
        };

        tracing::debug!(?cdc_table_desc, "create table with source w/ backfill");
        // save external table info to `source_info`
        source_info.external_table = Some(cdc_table_desc.to_protobuf());
    }

    gen_table_plan_inner(
        context.into(),
        table_name,
        columns,
        properties,
        pk_column_ids,
        row_id_index,
        Some(source_info),
        definition,
        watermark_descs,
        Some(cdc_table_type),
        append_only,
        Some(col_id_gen.into_version()),
    )
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
) -> Result<(PlanRef, Option<PbSource>, PbTable)> {
    let definition = context.normalized_sql().to_owned();
    let mut columns = bind_sql_columns(&column_defs)?;
    for c in &mut columns {
        c.column_desc.column_id = col_id_gen.generate(c.name())
    }
    let properties = context.with_options().inner().clone().into_iter().collect();
    gen_create_table_plan_without_bind(
        context,
        table_name,
        columns,
        column_defs,
        constraints,
        properties,
        definition,
        source_watermarks,
        append_only,
        Some(col_id_gen.into_version()),
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn gen_create_table_plan_without_bind(
    context: OptimizerContext,
    table_name: ObjectName,
    columns: Vec<ColumnCatalog>,
    column_defs: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    properties: HashMap<String, String>,
    definition: String,
    source_watermarks: Vec<SourceWatermark>,
    append_only: bool,
    version: Option<TableVersion>,
) -> Result<(PlanRef, Option<PbSource>, PbTable)> {
    ensure_table_constraints_supported(&constraints)?;
    let pk_names = bind_sql_pk_names(&column_defs, &constraints)?;
    let (mut columns, pk_column_ids, row_id_index) = bind_pk_on_relation(columns, pk_names)?;

    let watermark_descs = bind_source_watermark(
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

    gen_table_plan_inner(
        context.into(),
        table_name,
        columns,
        properties,
        pk_column_ids,
        row_id_index,
        None,
        definition,
        watermark_descs,
        None,
        append_only,
        version,
    )
}

#[allow(clippy::too_many_arguments)]
fn gen_table_plan_inner(
    context: OptimizerContextRef,
    table_name: ObjectName,
    columns: Vec<ColumnCatalog>,
    properties: HashMap<String, String>,
    pk_column_ids: Vec<ColumnId>,
    row_id_index: Option<usize>,
    source_info: Option<StreamSourceInfo>,
    definition: String,
    watermark_descs: Vec<WatermarkDesc>,
    cdc_table_type: Option<CdcTableType>,
    append_only: bool,
    version: Option<TableVersion>, /* TODO: this should always be `Some` if we support `ALTER
                                    * TABLE` for `CREATE TABLE AS`. */
) -> Result<(PlanRef, Option<PbSource>, PbTable)> {
    let session = context.session_ctx().clone();
    let db_name = session.database();
    let (schema_name, name) = Binder::resolve_schema_qualified_name(db_name, table_name)?;
    let (database_id, schema_id) =
        session.get_database_and_schema_id_for_create(schema_name.clone())?;

    // resolve privatelink connection for Table backed by Kafka source
    let mut with_options = WithOptions::new(properties);
    let connection_id =
        resolve_privatelink_in_with_option(&mut with_options, &schema_name, &session)?;

    let source = source_info.map(|source_info| PbSource {
        id: TableId::placeholder().table_id,
        schema_id,
        database_id,
        name: name.clone(),
        row_id_index: row_id_index.map(|i| i as _),
        columns: {
            let mut source_columns = columns.clone();
            if let Some(t) = cdc_table_type && t.can_backfill() {
                // Append the offset column to be used in the cdc backfill
                let offset_column = ColumnCatalog::offset_column();
                source_columns.push(offset_column);
            }
            source_columns
                .iter()
                .map(|column| column.to_protobuf())
                .collect_vec()
        },
        pk_column_ids: pk_column_ids.iter().map(Into::into).collect_vec(),
        properties: with_options.into_inner().into_iter().collect(),
        info: Some(source_info),
        owner: session.user_id(),
        watermark_descs: watermark_descs.clone(),
        definition: "".to_string(),
        connection_id,
        initialized_at_epoch: None,
        created_at_epoch: None,
        optional_associated_table_id: Some(OptionalAssociatedTableId::AssociatedTableId(
            TableId::placeholder().table_id,
        )),
        version: INITIAL_SOURCE_VERSION_ID,
    });

    let source_catalog = source.as_ref().map(|source| Rc::new((source).into()));
    let source_node: PlanRef = LogicalSource::new(
        source_catalog,
        columns.clone(),
        row_id_index,
        false,
        true,
        context.clone(),
    )?
    .into();

    let required_cols = FixedBitSet::with_capacity(columns.len());
    let mut plan_root = PlanRoot::new(
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

    let materialize = plan_root.gen_table_plan(
        context,
        name,
        columns,
        definition,
        pk_column_ids,
        row_id_index,
        append_only,
        watermark_descs,
        version,
    )?;

    let mut table = materialize.table().to_prost(schema_id, database_id);

    table.owner = session.user_id();
    Ok((materialize.into(), source, table))
}

#[allow(clippy::too_many_arguments)]
fn gen_create_table_plan_for_cdc_source(
    context: OptimizerContextRef,
    source_name: ObjectName,
    table_name: ObjectName,
    external_table_name: String,
    column_defs: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    mut col_id_gen: ColumnIdGenerator,
) -> Result<(PlanRef, PbTable)> {
    let session = context.session_ctx().clone();
    let db_name = session.database();
    let (schema_name, name) = Binder::resolve_schema_qualified_name(db_name, table_name)?;
    let (database_id, schema_id) =
        session.get_database_and_schema_id_for_create(schema_name.clone())?;

    // cdc table cannot be append-only
    let append_only = false;
    let source_name = source_name.real_value();

    let source = {
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_name = schema_name
            .clone()
            .unwrap_or(DEFAULT_SCHEMA_NAME.to_string());
        let (source, _) = catalog_reader.get_source_by_name(
            db_name,
            SchemaPath::Name(schema_name.as_str()),
            source_name.as_str(),
        )?;
        source.clone()
    };

    let mut columns = bind_sql_columns(&column_defs)?;

    for c in &mut columns {
        c.column_desc.column_id = col_id_gen.generate(c.name())
    }

    let pk_names = bind_sql_pk_names(&column_defs, &constraints)?;
    let (columns, pk_column_ids, _) = bind_pk_on_relation(columns, pk_names)?;

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

    let connect_properties =
        derive_connect_properties(source.as_ref(), external_table_name.clone())?;

    let cdc_table_desc = CdcTableDesc {
        table_id: source.id.into(), // source can be considered as an external table
        external_table_name: external_table_name.clone(),
        pk: table_pk,
        columns: columns.iter().map(|c| c.column_desc.clone()).collect(),
        stream_key: pk_column_indices,
        value_indices: (0..columns.len()).collect_vec(),
        connect_properties,
    };

    tracing::debug!(?cdc_table_desc, "create cdc table");

    let logical_scan = LogicalScan::create_for_cdc(
        external_table_name,
        Rc::new(cdc_table_desc),
        context.clone(),
    );

    let scan_node: PlanRef = logical_scan.into();
    let required_cols = FixedBitSet::with_capacity(columns.len());
    let mut plan_root = PlanRoot::new(
        scan_node,
        RequiredDist::Any,
        Order::any(),
        required_cols,
        vec![],
    );

    let materialize = plan_root.gen_table_plan(
        context,
        name,
        columns,
        definition,
        pk_column_ids,
        None,
        append_only,
        vec![], // no watermarks
        Some(col_id_gen.into_version()),
    )?;

    let mut table = materialize.table().to_prost(schema_id, database_id);
    table.owner = session.user_id();
    Ok((materialize.into(), table))
}

fn derive_connect_properties(
    source: &SourceCatalog,
    external_table_name: String,
) -> Result<BTreeMap<String, String>> {
    use source::cdc::{MYSQL_CDC_CONNECTOR, POSTGRES_CDC_CONNECTOR};
    // we should remove the prefix from `full_table_name`
    let mut connect_properties = source.properties.clone();
    if let Some(connector) = source.properties.get(UPSTREAM_SOURCE_KEY) {
        let table_name = match connector.as_str() {
            MYSQL_CDC_CONNECTOR => {
                let db_name = connect_properties.get(DATABASE_NAME_KEY).ok_or_else(|| {
                    anyhow!("{} not found in source properties", DATABASE_NAME_KEY)
                })?;

                let prefix = format!("{}.", db_name.as_str());
                external_table_name
                    .strip_prefix(prefix.as_str())
                    .ok_or_else(|| anyhow!("external table name must contain database prefix"))?
            }
            POSTGRES_CDC_CONNECTOR => {
                let schema_name = connect_properties
                    .get(SCHEMA_NAME_KEY)
                    .ok_or_else(|| anyhow!("{} not found in source properties", SCHEMA_NAME_KEY))?;

                let prefix = format!("{}.", schema_name.as_str());
                external_table_name
                    .strip_prefix(prefix.as_str())
                    .ok_or_else(|| anyhow!("external table name must contain schema prefix"))?
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
    Ok(connect_properties.into_iter().collect())
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_create_table(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    column_defs: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    if_not_exists: bool,
    source_schema: Option<ConnectorSchema>,
    source_watermarks: Vec<SourceWatermark>,
    append_only: bool,
    notice: Option<String>,
    cdc_table_info: Option<CdcTableInfo>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    // TODO(st1page): refactor it
    if let Some(notice) = notice {
        session.notice_to_user(notice)
    }

    if append_only {
        session.notice_to_user("APPEND ONLY TABLE is currently an experimental feature.");
    }

    match session.check_relation_name_duplicated(table_name.clone()) {
        Err(CheckRelationError::Catalog(CatalogError::Duplicated(_, name))) if if_not_exists => {
            return Ok(PgResponse::builder(StatementType::CREATE_TABLE)
                .notice(format!("relation \"{}\" already exists, skipping", name))
                .into());
        }
        Err(e) => return Err(e.into()),
        Ok(_) => {}
    };

    let (graph, source, table, job_type) = {
        let context = OptimizerContext::from_handler_args(handler_args);
        let source_schema = check_create_table_with_source(context.with_options(), source_schema)?;
        let col_id_gen = ColumnIdGenerator::new_initial();

        let ((plan, source, table), job_type) = match (source_schema, cdc_table_info.as_ref()) {
            (Some(source_schema), None) => (
                gen_create_table_plan_with_source(
                    context,
                    table_name.clone(),
                    column_defs,
                    constraints,
                    source_schema,
                    source_watermarks,
                    col_id_gen,
                    append_only,
                )
                .await?,
                TableJobType::General,
            ),
            (None, None) => (
                gen_create_table_plan(
                    context,
                    table_name.clone(),
                    column_defs,
                    constraints,
                    col_id_gen,
                    source_watermarks,
                    append_only,
                )?,
                TableJobType::General,
            ),

            (None, Some(cdc_table)) => {
                let (plan, table) = gen_create_table_plan_for_cdc_source(
                    context.into(),
                    cdc_table.source_name.clone(),
                    table_name.clone(),
                    cdc_table.external_table_name.clone(),
                    column_defs,
                    constraints,
                    col_id_gen,
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
        let mut graph = build_graph(plan);
        graph.parallelism = session
            .config()
            .get_streaming_parallelism()
            .map(|parallelism| Parallelism { parallelism });
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
) -> Result<Option<ConnectorSchema>> {
    if with_options.inner().contains_key(UPSTREAM_SOURCE_KEY) {
        source_schema.as_ref().ok_or_else(|| {
            ErrorCode::InvalidInputSyntax("Please specify a source schema using FORMAT".to_owned())
        })?;
    }
    Ok(source_schema)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::{
        row_id_column_name, Field, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME,
    };
    use risingwave_common::types::DataType;

    use super::*;
    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::LocalFrontend;

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
            .get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
            .unwrap();
        assert_eq!(table.name(), "t");

        let columns = table
            .columns
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<HashMap<&str, DataType>>();

        let row_id_col_name = row_id_column_name();
        let expected_columns = maplit::hashmap! {
            row_id_col_name.as_str() => DataType::Serial,
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
                let (_, pk_column_ids, _) = bind_pk_on_relation(columns, pk_names)?;
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
}
