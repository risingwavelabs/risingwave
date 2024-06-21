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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;
use std::sync::{Arc, LazyLock};

use anyhow::Context;
use arrow_schema::DataType as ArrowDataType;
use either::Either;
use itertools::Itertools;
use maplit::{convert_args, hashmap};
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::array::arrow::{FromArrow, IcebergArrowConvert};
use risingwave_common::catalog::{ConnectionId, DatabaseId, Schema, SchemaId, TableId, UserId};
use risingwave_common::types::DataType;
use risingwave_common::{bail, catalog};
use risingwave_connector::sink::catalog::{SinkCatalog, SinkFormatDesc, SinkType};
use risingwave_connector::sink::iceberg::{IcebergConfig, ICEBERG_SINK};
use risingwave_connector::sink::{
    CONNECTOR_TYPE_KEY, SINK_TYPE_OPTION, SINK_USER_FORCE_APPEND_ONLY_OPTION, SINK_WITHOUT_BACKFILL,
};
use risingwave_pb::catalog::{PbSource, Table};
use risingwave_pb::ddl_service::ReplaceTablePlan;
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{DispatcherType, MergeNode, StreamFragmentGraph, StreamNode};
use risingwave_sqlparser::ast::{
    ConnectorSchema, CreateSink, CreateSinkStatement, EmitMode, Encode, Format, Query, Statement,
};
use risingwave_sqlparser::parser::Parser;

use super::create_mv::get_column_names;
use super::create_source::UPSTREAM_SOURCE_KEY;
use super::util::gen_query_from_table_name;
use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::catalog_service::CatalogReadGuard;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::view_catalog::ViewCatalog;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{rewrite_now_to_proctime, ExprImpl, InputRef};
use crate::handler::alter_table_column::fetch_table_catalog_for_alter;
use crate::handler::create_mv::parse_column_names;
use crate::handler::create_table::{generate_stream_graph_for_table, ColumnIdGenerator};
use crate::handler::privilege::resolve_query_privileges;
use crate::handler::util::SourceSchemaCompatExt;
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::{
    generic, IcebergPartitionInfo, LogicalSource, PartitionComputeInfo, StreamProject,
};
use crate::optimizer::{OptimizerContext, OptimizerContextRef, PlanRef, RelationCollectorVisitor};
use crate::scheduler::streaming_manager::CreatingStreamingJobInfo;
use crate::session::SessionImpl;
use crate::stream_fragmenter::build_graph;
use crate::utils::{resolve_privatelink_in_with_option, resolve_secret_in_with_options};
use crate::{Explain, Planner, TableCatalog, WithOptions};

// used to store result of `gen_sink_plan`
pub struct SinkPlanContext {
    pub query: Box<Query>,
    pub sink_plan: PlanRef,
    pub sink_catalog: SinkCatalog,
    pub target_table_catalog: Option<Arc<TableCatalog>>,
}

pub fn gen_sink_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    stmt: CreateSinkStatement,
    partition_info: Option<PartitionComputeInfo>,
) -> Result<SinkPlanContext> {
    let user_specified_columns = !stmt.columns.is_empty();
    let db_name = session.database();
    let (sink_schema_name, sink_table_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.sink_name.clone())?;

    // Used for debezium's table name
    let sink_from_table_name;
    // `true` means that sink statement has the form: `CREATE SINK s1 FROM ...`
    // `false` means that sink statement has the form: `CREATE SINK s1 AS <query>`
    let direct_sink;
    let query = match stmt.sink_from {
        CreateSink::From(from_name) => {
            sink_from_table_name = from_name.0.last().unwrap().real_value();
            direct_sink = true;
            Box::new(gen_query_from_table_name(from_name))
        }
        CreateSink::AsQuery(query) => {
            sink_from_table_name = sink_table_name.clone();
            direct_sink = false;
            query
        }
    };

    let sink_into_table_name = stmt.into_table_name.as_ref().map(|name| name.real_value());

    let (sink_database_id, sink_schema_id) =
        session.get_database_and_schema_id_for_create(sink_schema_name.clone())?;

    let definition = context.normalized_sql().to_owned();

    let (dependent_relations, bound) = {
        let mut binder = Binder::new_for_stream(session);
        let bound = binder.bind_query(*query.clone())?;
        (binder.included_relations(), bound)
    };

    let check_items = resolve_query_privileges(&bound);
    session.check_privileges(&check_items)?;

    let col_names = if sink_into_table_name.is_some() {
        parse_column_names(&stmt.columns)
    } else {
        // If column names not specified, use the name in the bound query, which is equal with the plan root's original field name.
        get_column_names(&bound, session, stmt.columns)?
    };

    let mut with_options = context.with_options().clone();

    if sink_into_table_name.is_some() {
        let prev = with_options
            .inner_mut()
            .insert(CONNECTOR_TYPE_KEY.to_string(), "table".to_string());

        if prev.is_some() {
            return Err(RwError::from(ErrorCode::BindError(
                "In the case of sinking into table, the 'connector' parameter should not be provided.".to_string(),
            )));
        }
    }

    let connection_id = {
        let conn_id =
            resolve_privatelink_in_with_option(&mut with_options, &sink_schema_name, session)?;
        conn_id.map(ConnectionId)
    };
    let secret_ref = resolve_secret_in_with_options(&mut with_options, session)?;

    let emit_on_window_close = stmt.emit_mode == Some(EmitMode::OnWindowClose);
    if emit_on_window_close {
        context.warn_to_user("EMIT ON WINDOW CLOSE is currently an experimental feature. Please use it with caution.");
    }

    let connector = with_options
        .get(CONNECTOR_TYPE_KEY)
        .cloned()
        .ok_or_else(|| ErrorCode::BindError(format!("missing field '{CONNECTOR_TYPE_KEY}'")))?;

    let format_desc = match stmt.sink_schema {
        // Case A: new syntax `format ... encode ...`
        Some(f) => {
            validate_compatibility(&connector, &f)?;
            Some(bind_sink_format_desc(f)?)
        }
        None => match with_options.get(SINK_TYPE_OPTION) {
            // Case B: old syntax `type = '...'`
            Some(t) => SinkFormatDesc::from_legacy_type(&connector, t)?.map(|mut f| {
                session.notice_to_user("Consider using the newer syntax `FORMAT ... ENCODE ...` instead of `type = '...'`.");
                if let Some(v) = with_options.get(SINK_USER_FORCE_APPEND_ONLY_OPTION) {
                    f.options.insert(SINK_USER_FORCE_APPEND_ONLY_OPTION.into(), v.into());
                }
                f
            }),
            // Case C: no format + encode required
            None => None,
        },
    };

    let mut plan_root = Planner::new(context).plan_query(bound)?;
    if let Some(col_names) = &col_names {
        plan_root.set_out_names(col_names.clone())?;
    };

    let without_backfill = match with_options.remove(SINK_WITHOUT_BACKFILL) {
        Some(flag) if flag.eq_ignore_ascii_case("false") => {
            if direct_sink {
                true
            } else {
                return Err(ErrorCode::BindError(
                    "`snapshot = false` only support `CREATE SINK FROM MV or TABLE`".to_string(),
                )
                .into());
            }
        }
        _ => false,
    };

    let target_table_catalog = stmt
        .into_table_name
        .as_ref()
        .map(|table_name| fetch_table_catalog_for_alter(session, table_name))
        .transpose()?;

    if let Some(target_table_catalog) = &target_table_catalog {
        if let Some(col_names) = col_names {
            let target_table_columns = target_table_catalog
                .columns()
                .iter()
                .map(|c| c.name())
                .collect::<BTreeSet<_>>();
            for c in col_names {
                if !target_table_columns.contains(c.as_str()) {
                    return Err(RwError::from(ErrorCode::BindError(format!(
                        "Column {} not found in table {}",
                        c,
                        target_table_catalog.name()
                    ))));
                }
            }
        }
    }

    let target_table = target_table_catalog.as_ref().map(|catalog| catalog.id());

    let sink_plan = plan_root.gen_sink_plan(
        sink_table_name,
        definition,
        with_options,
        emit_on_window_close,
        db_name.to_owned(),
        sink_from_table_name,
        format_desc,
        without_backfill,
        target_table,
        partition_info,
    )?;
    let sink_desc = sink_plan.sink_desc().clone();

    let mut sink_plan: PlanRef = sink_plan.into();

    let ctx = sink_plan.ctx();
    let explain_trace = ctx.is_explain_trace();
    if explain_trace {
        ctx.trace("Create Sink:");
        ctx.trace(sink_plan.explain_to_string());
    }

    let dependent_relations =
        RelationCollectorVisitor::collect_with(dependent_relations, sink_plan.clone());

    let sink_catalog = sink_desc.into_catalog(
        SchemaId::new(sink_schema_id),
        DatabaseId::new(sink_database_id),
        UserId::new(session.user_id()),
        connection_id,
        dependent_relations.into_iter().collect_vec(),
        secret_ref,
    );

    if let Some(table_catalog) = &target_table_catalog {
        for column in sink_catalog.full_columns() {
            if column.is_generated() {
                unreachable!("can not derive generated columns in a sink's catalog, but meet one");
            }
        }

        let user_defined_primary_key_table =
            !(table_catalog.append_only || table_catalog.row_id_index.is_some());

        if !(user_defined_primary_key_table
            || sink_catalog.sink_type == SinkType::AppendOnly
            || sink_catalog.sink_type == SinkType::ForceAppendOnly)
        {
            return Err(RwError::from(ErrorCode::BindError(
                "Only append-only sinks can sink to a table without primary keys.".to_string(),
            )));
        }

        let exprs = derive_default_column_project_for_sink(
            &sink_catalog,
            sink_plan.schema(),
            table_catalog,
            user_specified_columns,
        )?;

        let logical_project = generic::Project::new(exprs, sink_plan);

        sink_plan = StreamProject::new(logical_project).into();

        let exprs =
            LogicalSource::derive_output_exprs_from_generated_columns(table_catalog.columns())?;
        if let Some(exprs) = exprs {
            let logical_project = generic::Project::new(exprs, sink_plan);
            sink_plan = StreamProject::new(logical_project).into();
        }
    };

    Ok(SinkPlanContext {
        query,
        sink_plan,
        sink_catalog,
        target_table_catalog,
    })
}

// This function is used to return partition compute info for a sink. More details refer in `PartitionComputeInfo`.
// Return:
// `Some(PartitionComputeInfo)` if the sink need to compute partition.
// `None` if the sink does not need to compute partition.
pub async fn get_partition_compute_info(
    with_options: &WithOptions,
) -> Result<Option<PartitionComputeInfo>> {
    let properties = with_options.clone().into_inner();
    let Some(connector) = properties.get(UPSTREAM_SOURCE_KEY) else {
        return Ok(None);
    };
    match connector.as_str() {
        ICEBERG_SINK => {
            let iceberg_config = IcebergConfig::from_btreemap(properties)?;
            get_partition_compute_info_for_iceberg(&iceberg_config).await
        }
        _ => Ok(None),
    }
}

async fn get_partition_compute_info_for_iceberg(
    iceberg_config: &IcebergConfig,
) -> Result<Option<PartitionComputeInfo>> {
    let table = iceberg_config.load_table().await?;
    let Some(partition_spec) = table.current_table_metadata().current_partition_spec().ok() else {
        return Ok(None);
    };

    if partition_spec.is_unpartitioned() {
        return Ok(None);
    }

    // Separate the partition spec into two parts: sparse partition and range partition.
    // Sparse partition means that the data distribution is more sparse at a given time.
    // Range partition means that the data distribution is likely same at a given time.
    // Only compute the partition and shuffle by them for the sparse partition.
    let has_sparse_partition = partition_spec.fields.iter().any(|f| match f.transform {
        // Sparse partition
        icelake::types::Transform::Identity
        | icelake::types::Transform::Truncate(_)
        | icelake::types::Transform::Bucket(_) => true,
        // Range partition
        icelake::types::Transform::Year
        | icelake::types::Transform::Month
        | icelake::types::Transform::Day
        | icelake::types::Transform::Hour
        | icelake::types::Transform::Void => false,
    });

    if !has_sparse_partition {
        return Ok(None);
    }

    let arrow_type: ArrowDataType = table
        .current_partition_type()
        .map_err(|err| RwError::from(ErrorCode::SinkError(err.into())))?
        .try_into()
        .map_err(|_| {
            RwError::from(ErrorCode::SinkError(
                "Fail to convert iceberg partition type to arrow type".into(),
            ))
        })?;
    let Some(schema) = table.current_table_metadata().current_schema().ok() else {
        return Ok(None);
    };
    let partition_fields = partition_spec
        .fields
        .iter()
        .map(|f| {
            let source_f = schema
                .look_up_field_by_id(f.source_column_id)
                .ok_or(RwError::from(ErrorCode::SinkError(
                    "Fail to look up iceberg partition field".into(),
                )))?;
            Ok((source_f.name.clone(), f.transform))
        })
        .collect::<Result<Vec<_>>>()?;

    let ArrowDataType::Struct(partition_type) = arrow_type else {
        return Err(RwError::from(ErrorCode::SinkError(
            "Partition type of iceberg should be a struct type".into(),
        )));
    };

    Ok(Some(PartitionComputeInfo::Iceberg(IcebergPartitionInfo {
        partition_type: IcebergArrowConvert.from_fields(&partition_type)?,
        partition_fields,
    })))
}

pub async fn handle_create_sink(
    handle_args: HandlerArgs,
    stmt: CreateSinkStatement,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();

    if let Either::Right(resp) = session.check_relation_name_duplicated(
        stmt.sink_name.clone(),
        StatementType::CREATE_SINK,
        stmt.if_not_exists,
    )? {
        return Ok(resp);
    }

    let partition_info = get_partition_compute_info(&handle_args.with_options).await?;

    let (sink, graph, target_table_catalog) = {
        let context = Rc::new(OptimizerContext::from_handler_args(handle_args));

        let SinkPlanContext {
            query,
            sink_plan: plan,
            sink_catalog: sink,
            target_table_catalog,
        } = gen_sink_plan(&session, context.clone(), stmt, partition_info)?;

        let has_order_by = !query.order_by.is_empty();
        if has_order_by {
            context.warn_to_user(
                r#"The ORDER BY clause in the CREATE SINK statement has no effect at all."#
                    .to_string(),
            );
        }

        let mut graph = build_graph(plan)?;

        graph.parallelism =
            session
                .config()
                .streaming_parallelism()
                .map(|parallelism| Parallelism {
                    parallelism: parallelism.get(),
                });

        (sink, graph, target_table_catalog)
    };

    let mut target_table_replace_plan = None;
    if let Some(table_catalog) = target_table_catalog {
        check_cycle_for_sink(session.as_ref(), sink.clone(), table_catalog.id())?;

        let (mut graph, mut table, source) =
            reparse_table_for_sink(&session, &table_catalog).await?;

        table
            .incoming_sinks
            .clone_from(&table_catalog.incoming_sinks);

        for _ in 0..(table_catalog.incoming_sinks.len() + 1) {
            for fragment in graph.fragments.values_mut() {
                if let Some(node) = &mut fragment.node {
                    insert_merger_to_union(node);
                }
            }
        }

        target_table_replace_plan = Some(ReplaceTablePlan {
            source,
            table: Some(table),
            fragment_graph: Some(graph),
            table_col_index_mapping: None,
        });
    }

    let _job_guard =
        session
            .env()
            .creating_streaming_job_tracker()
            .guard(CreatingStreamingJobInfo::new(
                session.session_id(),
                sink.database_id.database_id,
                sink.schema_id.schema_id,
                sink.name.clone(),
            ));

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .create_sink(sink.to_proto(), graph, target_table_replace_plan)
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_SINK))
}

fn check_cycle_for_sink(
    session: &SessionImpl,
    sink_catalog: SinkCatalog,
    table_id: catalog::TableId,
) -> Result<()> {
    let reader = session.env().catalog_reader().read_guard();

    let mut sinks = HashMap::new();
    let mut sources = HashMap::new();
    let mut views = HashMap::new();
    let db_name = session.database();
    for schema in reader.iter_schemas(db_name)? {
        for sink in schema.iter_sink() {
            sinks.insert(sink.id.sink_id, sink.as_ref());
        }

        for source in schema.iter_source() {
            sources.insert(source.id, source.as_ref());
        }

        for view in schema.iter_view() {
            views.insert(view.id, view.as_ref());
        }
    }

    struct Context<'a> {
        reader: &'a CatalogReadGuard,
        sink_index: &'a HashMap<u32, &'a SinkCatalog>,
        source_index: &'a HashMap<u32, &'a SourceCatalog>,
        view_index: &'a HashMap<u32, &'a ViewCatalog>,
    }

    impl Context<'_> {
        fn visit_table(
            &self,
            table: &TableCatalog,
            target_table_id: catalog::TableId,
            path: &mut Vec<String>,
        ) -> Result<()> {
            if table.id == target_table_id {
                path.reverse();
                path.push(table.name.clone());
                return Err(RwError::from(ErrorCode::BindError(
                    format!(
                        "Creating such a sink will result in circular dependency, path = [{}]",
                        path.join(", ")
                    )
                    .to_string(),
                )));
            }

            for sink_id in &table.incoming_sinks {
                if let Some(sink) = self.sink_index.get(sink_id) {
                    path.push(sink.name.clone());
                    self.visit_dependent_jobs(&sink.dependent_relations, target_table_id, path)?;
                    path.pop();
                } else {
                    bail!("sink not found: {:?}", sink_id);
                }
            }

            self.visit_dependent_jobs(&table.dependent_relations, target_table_id, path)?;

            Ok(())
        }

        fn visit_dependent_jobs(
            &self,
            dependent_jobs: &[TableId],
            target_table_id: TableId,
            path: &mut Vec<String>,
        ) -> Result<()> {
            for table_id in dependent_jobs {
                if let Ok(table) = self.reader.get_table_by_id(table_id) {
                    path.push(table.name.clone());
                    self.visit_table(table.as_ref(), target_table_id, path)?;
                    path.pop();
                } else if self.source_index.contains_key(&table_id.table_id)
                    || self.view_index.contains_key(&table_id.table_id)
                {
                    continue;
                } else {
                    bail!("streaming job not found: {:?}", table_id);
                }
            }

            Ok(())
        }
    }

    let mut path = vec![];

    path.push(sink_catalog.name.clone());

    let ctx = Context {
        reader: &reader,
        sink_index: &sinks,
        source_index: &sources,
        view_index: &views,
    };

    ctx.visit_dependent_jobs(&sink_catalog.dependent_relations, table_id, &mut path)?;

    Ok(())
}

pub(crate) async fn reparse_table_for_sink(
    session: &Arc<SessionImpl>,
    table_catalog: &Arc<TableCatalog>,
) -> Result<(StreamFragmentGraph, Table, Option<PbSource>)> {
    // Retrieve the original table definition and parse it to AST.
    let [definition]: [_; 1] = Parser::parse_sql(&table_catalog.definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();
    let Statement::CreateTable {
        name,
        source_schema,
        ..
    } = &definition
    else {
        panic!("unexpected statement: {:?}", definition);
    };

    let table_name = name.clone();
    let source_schema = source_schema
        .clone()
        .map(|source_schema| source_schema.into_v2_with_warning());

    // Create handler args as if we're creating a new table with the altered definition.
    let handler_args = HandlerArgs::new(session.clone(), &definition, Arc::from(""))?;
    let col_id_gen = ColumnIdGenerator::new_alter(table_catalog);
    let Statement::CreateTable {
        columns,
        wildcard_idx,
        constraints,
        source_watermarks,
        append_only,
        on_conflict,
        with_version_column,
        ..
    } = definition
    else {
        panic!("unexpected statement type: {:?}", definition);
    };

    let (graph, table, source) = generate_stream_graph_for_table(
        session,
        table_name,
        table_catalog,
        source_schema,
        handler_args,
        col_id_gen,
        columns,
        wildcard_idx,
        constraints,
        source_watermarks,
        append_only,
        on_conflict,
        with_version_column,
    )
    .await?;

    Ok((graph, table, source))
}

pub(crate) fn insert_merger_to_union(node: &mut StreamNode) {
    if let Some(NodeBody::Union(_union_node)) = &mut node.node_body {
        node.input.push(StreamNode {
            identity: "Merge (sink into table)".to_string(),
            fields: node.fields.clone(),
            node_body: Some(NodeBody::Merge(MergeNode {
                upstream_dispatcher_type: DispatcherType::Hash as _,
                ..Default::default()
            })),
            ..Default::default()
        });

        return;
    }

    for input in &mut node.input {
        insert_merger_to_union(input);
    }
}

fn derive_sink_to_table_expr(
    sink_schema: &Schema,
    idx: usize,
    target_type: &DataType,
) -> Result<ExprImpl> {
    let input_type = &sink_schema.fields()[idx].data_type;

    if target_type != input_type {
        bail!(
            "column type mismatch: {:?} vs {:?}",
            target_type,
            input_type
        );
    } else {
        Ok(ExprImpl::InputRef(Box::new(InputRef::new(
            idx,
            input_type.clone(),
        ))))
    }
}

fn derive_default_column_project_for_sink(
    sink: &SinkCatalog,
    sink_schema: &Schema,
    target_table_catalog: &Arc<TableCatalog>,
    user_specified_columns: bool,
) -> Result<Vec<ExprImpl>> {
    assert_eq!(sink.full_schema().len(), sink_schema.len());

    let mut exprs = vec![];

    let sink_visible_col_idxes = sink
        .full_columns()
        .iter()
        .positions(|c| !c.is_hidden())
        .collect_vec();
    let sink_visible_col_idxes_by_name = sink
        .full_columns()
        .iter()
        .enumerate()
        .filter(|(_, c)| !c.is_hidden())
        .map(|(i, c)| (c.name(), i))
        .collect::<BTreeMap<_, _>>();

    for (idx, table_column) in target_table_catalog.columns().iter().enumerate() {
        if table_column.is_generated() {
            continue;
        }

        let default_col_expr = || -> ExprImpl {
            rewrite_now_to_proctime(target_table_catalog.default_column_expr(idx))
        };

        let sink_col_expr = |sink_col_idx: usize| -> Result<ExprImpl> {
            derive_sink_to_table_expr(sink_schema, sink_col_idx, table_column.data_type())
        };

        // If users specified the columns to be inserted e.g. `CREATE SINK s INTO t(a, b)`, the expressions of `Project` will be generated accordingly.
        // The missing columns will be filled with default value (`null` if not explicitly defined).
        // Otherwise, e.g. `CREATE SINK s INTO t`, the columns will be matched by their order in `select` query and the target table.
        #[allow(clippy::collapsible_else_if)]
        if user_specified_columns {
            if let Some(idx) = sink_visible_col_idxes_by_name.get(table_column.name()) {
                exprs.push(sink_col_expr(*idx)?);
            } else {
                exprs.push(default_col_expr());
            }
        } else {
            if idx < sink_visible_col_idxes.len() {
                exprs.push(sink_col_expr(sink_visible_col_idxes[idx])?);
            } else {
                exprs.push(default_col_expr());
            };
        }
    }
    Ok(exprs)
}

/// Transforms the (format, encode, options) from sqlparser AST into an internal struct `SinkFormatDesc`.
/// This is an analogy to (part of) [`crate::handler::create_source::bind_columns_from_source`]
/// which transforms sqlparser AST `SourceSchemaV2` into `StreamSourceInfo`.
fn bind_sink_format_desc(value: ConnectorSchema) -> Result<SinkFormatDesc> {
    use risingwave_connector::sink::catalog::{SinkEncode, SinkFormat};
    use risingwave_connector::sink::encoder::TimestamptzHandlingMode;
    use risingwave_sqlparser::ast::{Encode as E, Format as F};

    let format = match value.format {
        F::Plain => SinkFormat::AppendOnly,
        F::Upsert => SinkFormat::Upsert,
        F::Debezium => SinkFormat::Debezium,
        f @ (F::Native | F::DebeziumMongo | F::Maxwell | F::Canal | F::None) => {
            return Err(ErrorCode::BindError(format!("sink format unsupported: {f}")).into());
        }
    };
    let encode = match value.row_encode {
        E::Json => SinkEncode::Json,
        E::Protobuf => SinkEncode::Protobuf,
        E::Avro => SinkEncode::Avro,
        E::Template => SinkEncode::Template,
        e @ (E::Native | E::Csv | E::Bytes | E::None | E::Text) => {
            return Err(ErrorCode::BindError(format!("sink encode unsupported: {e}")).into());
        }
    };

    let mut key_encode = None;
    if let Some(encode) = value.key_encode {
        if encode == E::Text {
            key_encode = Some(SinkEncode::Text);
        } else {
            return Err(ErrorCode::BindError(format!(
                "sink key encode unsupported: {encode}, only TEXT supported"
            ))
            .into());
        }
    }

    let mut options = WithOptions::try_from(value.row_options.as_slice())?.into_inner();

    options
        .entry(TimestamptzHandlingMode::OPTION_KEY.to_owned())
        .or_insert(TimestamptzHandlingMode::FRONTEND_DEFAULT.to_owned());

    Ok(SinkFormatDesc {
        format,
        encode,
        options,
        key_encode,
    })
}

static CONNECTORS_COMPATIBLE_FORMATS: LazyLock<HashMap<String, HashMap<Format, Vec<Encode>>>> =
    LazyLock::new(|| {
        use risingwave_connector::sink::google_pubsub::GooglePubSubSink;
        use risingwave_connector::sink::kafka::KafkaSink;
        use risingwave_connector::sink::kinesis::KinesisSink;
        use risingwave_connector::sink::mqtt::MqttSink;
        use risingwave_connector::sink::pulsar::PulsarSink;
        use risingwave_connector::sink::redis::RedisSink;
        use risingwave_connector::sink::Sink as _;

        convert_args!(hashmap!(
                GooglePubSubSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json],
                ),
                KafkaSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Avro, Encode::Protobuf],
                    Format::Upsert => vec![Encode::Json, Encode::Avro],
                    Format::Debezium => vec![Encode::Json],
                ),
                KinesisSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json],
                    Format::Upsert => vec![Encode::Json],
                    Format::Debezium => vec![Encode::Json],
                ),
                MqttSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf],
                ),
                PulsarSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json],
                    Format::Upsert => vec![Encode::Json],
                    Format::Debezium => vec![Encode::Json],
                ),
                RedisSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Template],
                    Format::Upsert => vec![Encode::Json, Encode::Template],
                ),
        ))
    });

pub fn validate_compatibility(connector: &str, format_desc: &ConnectorSchema) -> Result<()> {
    let compatible_formats = CONNECTORS_COMPATIBLE_FORMATS
        .get(connector)
        .ok_or_else(|| {
            ErrorCode::BindError(format!(
                "connector {} is not supported by FORMAT ... ENCODE ... syntax",
                connector
            ))
        })?;
    let compatible_encodes = compatible_formats.get(&format_desc.format).ok_or_else(|| {
        ErrorCode::BindError(format!(
            "connector {} does not support format {:?}",
            connector, format_desc.format
        ))
    })?;
    if !compatible_encodes.contains(&format_desc.row_encode) {
        return Err(ErrorCode::BindError(format!(
            "connector {} does not support format {:?} with encode {:?}",
            connector, format_desc.format, format_desc.row_encode
        ))
        .into());
    }
    Ok(())
}

/// For `planner_test` crate so that it does not depend directly on `connector` crate just for `SinkFormatDesc`.
impl TryFrom<&WithOptions> for Option<SinkFormatDesc> {
    type Error = risingwave_connector::sink::SinkError;

    fn try_from(value: &WithOptions) -> std::result::Result<Self, Self::Error> {
        let connector = value.get(CONNECTOR_TYPE_KEY);
        let r#type = value.get(SINK_TYPE_OPTION);
        match (connector, r#type) {
            (Some(c), Some(t)) => SinkFormatDesc::from_legacy_type(c, t),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_create_sink_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t1
    WITH (connector = 'kafka', kafka.topic = 'abc', kafka.servers = 'localhost:1001')
    FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecord', schema.location = 'file://{}')"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let sql = "create materialized view mv1 as select t1.country from t1;";
        frontend.run_sql(sql).await.unwrap();

        let sql = r#"CREATE SINK snk1 FROM mv1
                    WITH (connector = 'jdbc', mysql.endpoint = '127.0.0.1:3306', mysql.table =
                        '<table_name>', mysql.database = '<database_name>', mysql.user = '<user_name>',
                        mysql.password = '<password>', type = 'append-only', force_append_only = 'true');"#.to_string();
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t1")
            .unwrap();
        assert_eq!(source.name, "t1");

        // Check table exists.
        let (table, schema_name) = catalog_reader
            .get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "mv1")
            .unwrap();
        assert_eq!(table.name(), "mv1");

        // Check sink exists.
        let (sink, _) = catalog_reader
            .get_sink_by_name(DEFAULT_DATABASE_NAME, SchemaPath::Name(schema_name), "snk1")
            .unwrap();
        assert_eq!(sink.name, "snk1");
    }
}
