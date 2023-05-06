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

use std::collections::HashMap;
use std::rc::Rc;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{
    ColumnCatalog, ColumnDesc, TableId, TableVersionId, INITIAL_TABLE_VERSION_ID,
    USER_COLUMN_ID_OFFSET,
};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::catalog::source::OptionalAssociatedTableId;
use risingwave_pb::catalog::{PbSource, PbTable, StreamSourceInfo, WatermarkDesc};
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_pb::plan_common::{DefaultColumnDesc, GeneratedColumnDesc};
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_sqlparser::ast::{
    ColumnDef, ColumnOption, DataType as AstDataType, ObjectName, SourceSchema, SourceWatermark,
    TableConstraint,
};

use super::create_source::resolve_source_schema;
use super::RwPgResponse;
use crate::binder::{bind_data_type, bind_struct_field};
use crate::catalog::table_catalog::TableVersion;
use crate::catalog::{check_valid_column_name, ColumnId};
use crate::expr::{Expr, ExprImpl};
use crate::handler::create_source::{
    bind_source_watermark, check_source_schema, UPSTREAM_SOURCE_KEY,
};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::LogicalSource;
use crate::optimizer::property::{Order, RequiredDist};
use crate::optimizer::{OptimizerContext, OptimizerContextRef, PlanRef, PlanRoot};
use crate::session::SessionImpl;
use crate::stream_fragmenter::build_graph;
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

/// Binds the column schemas declared in CREATE statement into `ColumnDesc`.
/// If a column is marked as `primary key`, its `ColumnId` is also returned.
/// This primary key is not combined with table constraints yet.
pub fn bind_sql_columns(
    columns: Vec<ColumnDef>,
    col_id_gen: &mut ColumnIdGenerator,
) -> Result<Vec<ColumnDesc>> {
    let mut column_descs = Vec::with_capacity(columns.len());

    for column in columns {
        let column_id = col_id_gen.generate(&column.name.real_value());
        // Destruct to make sure all fields are properly handled rather than ignored.
        // Do NOT use `..` to ignore fields you do not want to deal with.
        // Reject them with a clear NotImplemented error.
        let ColumnDef {
            name,
            data_type,
            collation,
            ..
        } = column;

        let data_type = data_type.ok_or(ErrorCode::InvalidInputSyntax(
            "data type is not specified".into(),
        ))?;
        if let Some(collation) = collation {
            return Err(ErrorCode::NotImplemented(
                format!("collation \"{}\"", collation),
                None.into(),
            )
            .into());
        }

        check_valid_column_name(&name.real_value())?;

        let field_descs = if let AstDataType::Struct(fields) = &data_type {
            fields
                .iter()
                .map(bind_struct_field)
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![]
        };
        column_descs.push(ColumnDesc {
            data_type: bind_data_type(&data_type)?,
            column_id,
            name: name.real_value(),
            field_descs,
            type_name: "".to_string(),
            generated_or_default_column: None,
        });
    }

    Ok(column_descs)
}

fn check_generated_column_constraints(
    column_name: &String,
    expr: &ExprImpl,
    column_catalogs: &[ColumnCatalog],
    generated_column_names: &[String],
) -> Result<()> {
    let input_refs = expr.collect_input_refs(column_catalogs.len());
    for idx in input_refs.ones() {
        let referred_generated_column = &column_catalogs[idx].column_desc.name;
        if generated_column_names
            .iter()
            .any(|c| c == referred_generated_column)
        {
            return Err(ErrorCode::BindError(
                format!("Generated can not reference another generated column, but here generated column \"{}\" referenced another generated column \"{}\"", column_name, referred_generated_column),
            )
            .into());
        }
    }
    Ok(())
}

fn check_default_column_constraints(
    expr: &ExprImpl,
    column_catalogs: &[ColumnCatalog],
) -> Result<()> {
    let input_refs = expr.collect_input_refs(column_catalogs.len());
    if input_refs.count_ones(..) > 0 {
        return Err(ErrorCode::BindError(
            "Default can not reference another column, and you should try generated column instead."
                .to_string(),
        )
        .into());
    }
    Ok(())
}

/// Binds constraints that can be only specified in column definitions.
pub fn bind_sql_column_constraints(
    session: &SessionImpl,
    table_name: String,
    column_catalogs: &mut [ColumnCatalog],
    columns: Vec<ColumnDef>,
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
                    let idx = binder
                        .get_column_binding_index(table_name.clone(), &column.name.real_value())?;
                    let expr_impl = binder.bind_expr(expr)?;

                    check_generated_column_constraints(
                        &column.name.real_value(),
                        &expr_impl,
                        column_catalogs,
                        &generated_column_names,
                    )?;

                    column_catalogs[idx].column_desc.generated_or_default_column = Some(
                        GeneratedOrDefaultColumn::GeneratedColumn(GeneratedColumnDesc {
                            expr: Some(expr_impl.to_expr_proto()),
                        }),
                    );
                }
                ColumnOption::DefaultColumns(expr) => {
                    let idx = binder
                        .get_column_binding_index(table_name.clone(), &column.name.real_value())?;
                    let expr_impl = binder.bind_expr(expr)?;

                    check_default_column_constraints(&expr_impl, column_catalogs)?;

                    column_catalogs[idx].column_desc.generated_or_default_column =
                        Some(GeneratedOrDefaultColumn::DefaultColumn(DefaultColumnDesc {
                            expr: Some(expr_impl.to_expr_proto()),
                        }));
                }
                ColumnOption::Unique { is_primary: true } => {
                    // Bind primary key in `bind_sql_table_column_constraints`
                }
                _ => {
                    return Err(ErrorCode::NotImplemented(
                        format!("column constraints \"{}\"", option_def),
                        None.into(),
                    )
                    .into())
                }
            }
        }
    }
    Ok(())
}

/// Binds constraints that can be specified in both column definitions and table definition.
///
/// It returns the columns together with `pk_column_ids`, and an optional row id column index if
/// added.
pub fn bind_sql_table_column_constraints(
    columns_descs: Vec<ColumnDesc>,
    columns_defs: Vec<ColumnDef>,
    table_constraints: Vec<TableConstraint>,
) -> Result<(Vec<ColumnCatalog>, Vec<ColumnId>, Option<usize>)> {
    let mut pk_column_names = vec![];
    // Mapping from column name to column id.
    let name_to_id = columns_descs
        .iter()
        .map(|c| (c.name.as_str(), c.column_id))
        .collect::<HashMap<_, _>>();

    // Bind column constraints
    for column in columns_defs {
        for option_def in column.options {
            match option_def.option {
                ColumnOption::Unique { is_primary: true } => {
                    if !pk_column_names.is_empty() {
                        return Err(ErrorCode::BindError(
                            "multiple primary keys are not allowed".into(),
                        )
                        .into());
                    }
                    pk_column_names.push(column.name.real_value());
                }
                ColumnOption::GeneratedColumns(_) | ColumnOption::DefaultColumns(_) => {
                    // Bind generated columns in `bind_sql_column_constraints`
                }
                _ => {
                    return Err(ErrorCode::NotImplemented(
                        format!("column constraints \"{}\"", option_def),
                        None.into(),
                    )
                    .into())
                }
            }
        }
    }

    // Bind table constraints.
    for constraint in table_constraints {
        match constraint {
            TableConstraint::Unique {
                name: _,
                columns,
                is_primary: true,
            } => {
                if !pk_column_names.is_empty() {
                    return Err(ErrorCode::BindError(
                        "multiple primary keys are not allowed".into(),
                    )
                    .into());
                }
                pk_column_names = columns.iter().map(|c| c.real_value()).collect_vec();
            }
            _ => {
                return Err(ErrorCode::NotImplemented(
                    format!("table constraint \"{}\"", constraint),
                    None.into(),
                )
                .into())
            }
        }
    }

    let mut pk_column_ids: Vec<_> = pk_column_names
        .iter()
        .map(|name| {
            name_to_id.get(name.as_str()).copied().ok_or_else(|| {
                ErrorCode::BindError(format!("column \"{name}\" named in key does not exist"))
            })
        })
        .try_collect()?;

    let mut columns_catalog = columns_descs
        .into_iter()
        .map(|c| {
            // All columns except `_row_id` or starts with `_rw` should be visible.
            let is_hidden = c.name.starts_with("_rw");
            ColumnCatalog {
                column_desc: c,
                is_hidden,
            }
        })
        .collect_vec();

    // Add `_row_id` column if `pk_column_ids` is empty.
    let row_id_index = pk_column_ids.is_empty().then(|| {
        let column = ColumnCatalog::row_id_column();
        let index = columns_catalog.len();
        pk_column_ids = vec![column.column_id()];
        columns_catalog.push(column);
        index
    });

    if let Some(col) = columns_catalog.iter().map(|c| c.name()).duplicates().next() {
        Err(ErrorCode::InvalidInputSyntax(format!(
            "column \"{col}\" specified more than once"
        )))?;
    }

    Ok((columns_catalog, pk_column_ids, row_id_index))
}

/// `gen_create_table_plan_with_source` generates the plan for creating a table with an external
/// stream source.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn gen_create_table_plan_with_source(
    context: OptimizerContext,
    table_name: ObjectName,
    column_defs: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    source_schema: SourceSchema,
    source_watermarks: Vec<SourceWatermark>,
    mut col_id_gen: ColumnIdGenerator,
    append_only: bool,
) -> Result<(PlanRef, Option<PbSource>, PbTable)> {
    let session = context.session_ctx();
    let column_descs = bind_sql_columns(column_defs.clone(), &mut col_id_gen)?;
    let mut properties = context.with_options().inner().clone().into_iter().collect();

    let (mut columns, mut pk_column_ids, mut row_id_index) =
        bind_sql_table_column_constraints(column_descs, column_defs.clone(), constraints)?;

    let watermark_descs = bind_source_watermark(
        session,
        table_name.real_value(),
        source_watermarks,
        &columns,
    )?;
    // TODO(yuhao): allow multiple watermark on source.
    assert!(watermark_descs.len() <= 1);

    let definition = context.normalized_sql().to_owned();

    let source_info = resolve_source_schema(
        source_schema,
        &mut columns,
        &mut properties,
        &mut row_id_index,
        &mut pk_column_ids,
        true,
    )
    .await?;

    bind_sql_column_constraints(session, table_name.real_value(), &mut columns, column_defs)?;

    check_source_schema(&properties, row_id_index, &columns)?;

    if row_id_index.is_none() && columns.iter().any(|c| c.is_generated()) {
        // TODO(yuhao): allow delete from a non append only source
        return Err(ErrorCode::BindError(
            "Generated columns are only allowed in an append only source.".to_string(),
        )
        .into());
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
        append_only,
        Some(col_id_gen.into_version()),
    )
}

/// `gen_create_table_plan` generates the plan for creating a table without an external stream
/// source.
pub(crate) fn gen_create_table_plan(
    context: OptimizerContext,
    table_name: ObjectName,
    columns: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    mut col_id_gen: ColumnIdGenerator,
    source_watermarks: Vec<SourceWatermark>,
    append_only: bool,
) -> Result<(PlanRef, Option<PbSource>, PbTable)> {
    let definition = context.normalized_sql().to_owned();
    let column_descs = bind_sql_columns(columns.clone(), &mut col_id_gen)?;

    let properties = context.with_options().inner().clone().into_iter().collect();
    gen_create_table_plan_without_bind(
        context,
        table_name,
        column_descs,
        columns,
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
    column_descs: Vec<ColumnDesc>,
    column_defs: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    properties: HashMap<String, String>,
    definition: String,
    source_watermarks: Vec<SourceWatermark>,
    append_only: bool,
    version: Option<TableVersion>,
) -> Result<(PlanRef, Option<PbSource>, PbTable)> {
    let (mut columns, pk_column_ids, row_id_index) =
        bind_sql_table_column_constraints(column_descs, column_defs.clone(), constraints)?;

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
    append_only: bool,
    version: Option<TableVersion>, /* TODO: this should always be `Some` if we support `ALTER
                                    * TABLE` for `CREATE TABLE AS`. */
) -> Result<(PlanRef, Option<PbSource>, PbTable)> {
    let session = context.session_ctx().clone();
    let db_name = session.database();
    let (schema_name, name) = Binder::resolve_schema_qualified_name(db_name, table_name)?;
    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    let source = source_info.map(|source_info| PbSource {
        id: TableId::placeholder().table_id,
        schema_id,
        database_id,
        name: name.clone(),
        row_id_index: row_id_index.map(|i| i as _),
        columns: columns
            .iter()
            .map(|column| column.to_protobuf())
            .collect_vec(),
        pk_column_ids: pk_column_ids.iter().map(Into::into).collect_vec(),
        properties,
        info: Some(source_info),
        owner: session.user_id(),
        watermark_descs: watermark_descs.clone(),
        definition: "".to_string(),
        connection_id: None,
        optional_associated_table_id: Some(OptionalAssociatedTableId::AssociatedTableId(
            TableId::placeholder().table_id,
        )),
    });

    let source_catalog = source.as_ref().map(|source| Rc::new((source).into()));
    let source_node: PlanRef = LogicalSource::new(
        source_catalog,
        columns.clone(),
        row_id_index,
        false,
        true,
        context.clone(),
    )
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
pub async fn handle_create_table(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    columns: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    if_not_exists: bool,
    source_schema: Option<SourceSchema>,
    source_watermarks: Vec<SourceWatermark>,
    append_only: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();

    if let Err(e) = session.check_relation_name_duplicated(table_name.clone()) {
        if if_not_exists {
            return Ok(PgResponse::empty_result_with_notice(
                StatementType::CREATE_TABLE,
                format!("relation \"{}\" already exists, skipping", table_name),
            ));
        } else {
            return Err(e);
        }
    }

    let (graph, source, table, notices) = {
        let context = OptimizerContext::from_handler_args(handler_args);
        let source_schema = check_create_table_with_source(context.with_options(), source_schema)?;
        let col_id_gen = ColumnIdGenerator::new_initial();

        let (plan, source, table) = match source_schema {
            Some(source_schema) => {
                gen_create_table_plan_with_source(
                    context,
                    table_name.clone(),
                    columns,
                    constraints,
                    source_schema,
                    source_watermarks,
                    col_id_gen,
                    append_only,
                )
                .await?
            }
            None => gen_create_table_plan(
                context,
                table_name.clone(),
                columns,
                constraints,
                col_id_gen,
                source_watermarks,
                append_only,
            )?,
        };

        let context = plan.plan_base().ctx.clone();
        let notices = context.take_warnings();

        let mut graph = build_graph(plan);
        graph.parallelism = session
            .config()
            .get_streaming_parallelism()
            .map(|parallelism| Parallelism { parallelism });
        (graph, source, table, notices)
    };

    tracing::trace!(
        "name={}, graph=\n{}",
        table_name,
        serde_json::to_string_pretty(&graph).unwrap()
    );

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.create_table(source, table, graph).await?;

    Ok(PgResponse::empty_result_with_notices(
        StatementType::CREATE_TABLE,
        notices,
    ))
}

pub fn check_create_table_with_source(
    with_options: &WithOptions,
    source_schema: Option<SourceSchema>,
) -> Result<Option<SourceSchema>> {
    if with_options.inner().contains_key(UPSTREAM_SOURCE_KEY) {
        source_schema.as_ref().ok_or_else(|| {
            ErrorCode::InvalidInputSyntax(
                "Please specify a source schema using ROW FORMAT".to_owned(),
            )
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
                columns,
                constraints,
                ..
            } = ast.remove(0) else {
                panic!("test case should be create table")
            };
            let actual: Result<_> = (|| {
                let column_descs =
                    bind_sql_columns(columns.clone(), &mut ColumnIdGenerator::new_initial())?;
                let (_, pk_column_ids, _) =
                    bind_sql_table_column_constraints(column_descs, columns, constraints)?;
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
