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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use either::Either;
use iceberg::arrow::type_to_arrow_type;
use iceberg::spec::Transform;
use itertools::Itertools;
use maplit::{convert_args, hashmap, hashset};
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::arrow::arrow_schema_iceberg::DataType as ArrowDataType;
use risingwave_common::bail;
use risingwave_common::catalog::{
    ColumnCatalog, ConnectionId, DatabaseId, ObjectId, Schema, SchemaId, UserId,
};
use risingwave_common::license::Feature;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::types::DataType;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkFormatDesc};
use risingwave_connector::sink::iceberg::{ICEBERG_SINK, IcebergConfig};
use risingwave_connector::sink::kafka::KAFKA_SINK;
use risingwave_connector::sink::{
    CONNECTOR_TYPE_KEY, SINK_SNAPSHOT_OPTION, SINK_TYPE_OPTION, SINK_USER_FORCE_APPEND_ONLY_OPTION,
    enforce_secret_sink,
};
use risingwave_connector::{AUTO_SCHEMA_CHANGE_KEY, WithPropertiesExt};
use risingwave_pb::catalog::PbSink;
use risingwave_pb::catalog::connection_params::PbConnectionType;
use risingwave_pb::ddl_service::{ReplaceJobPlan, TableJobType, replace_job_plan};
use risingwave_pb::stream_plan::stream_node::{NodeBody, PbNodeBody};
use risingwave_pb::stream_plan::{MergeNode, StreamFragmentGraph, StreamNode};
use risingwave_pb::telemetry::TelemetryDatabaseObject;
use risingwave_sqlparser::ast::{
    CreateSink, CreateSinkStatement, EmitMode, Encode, ExplainOptions, Format, FormatEncodeOptions,
    ObjectName, Query, Statement,
};

use super::RwPgResponse;
use super::create_mv::get_column_names;
use super::create_source::{SqlColumnStrategy, UPSTREAM_SOURCE_KEY};
use super::util::gen_query_from_table_name;
use crate::binder::{Binder, Relation};
use crate::catalog::SinkId;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::table_catalog::TableType;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{ExprImpl, InputRef, rewrite_now_to_proctime};
use crate::handler::HandlerArgs;
use crate::handler::alter_table_column::fetch_table_catalog_for_alter;
use crate::handler::create_mv::parse_column_names;
use crate::handler::create_table::{ColumnIdGenerator, generate_stream_graph_for_replace_table};
use crate::handler::util::{check_connector_match_connection_type, ensure_connection_type_allowed};
use crate::optimizer::plan_node::{
    IcebergPartitionInfo, LogicalSource, PartitionComputeInfo, StreamPlanRef as PlanRef,
    StreamProject, generic,
};
use crate::optimizer::{OptimizerContext, RelationCollectorVisitor};
use crate::scheduler::streaming_manager::CreatingStreamingJobInfo;
use crate::session::SessionImpl;
use crate::session::current::notice_to_user;
use crate::stream_fragmenter::{GraphJobType, build_graph};
use crate::utils::{resolve_connection_ref_and_secret_ref, resolve_privatelink_in_with_option};
use crate::{Explain, Planner, TableCatalog, WithOptions, WithOptionsSecResolved};

static SINK_ALLOWED_CONNECTION_CONNECTOR: LazyLock<HashSet<PbConnectionType>> =
    LazyLock::new(|| {
        hashset! {
            PbConnectionType::Unspecified,
            PbConnectionType::Kafka,
            PbConnectionType::Iceberg,
            PbConnectionType::Elasticsearch,
        }
    });

static SINK_ALLOWED_CONNECTION_SCHEMA_REGISTRY: LazyLock<HashSet<PbConnectionType>> =
    LazyLock::new(|| {
        hashset! {
            PbConnectionType::Unspecified,
            PbConnectionType::SchemaRegistry,
        }
    });

// used to store result of `gen_sink_plan`
pub struct SinkPlanContext {
    pub query: Box<Query>,
    pub sink_plan: PlanRef,
    pub sink_catalog: SinkCatalog,
    pub target_table_catalog: Option<Arc<TableCatalog>>,
    pub dependencies: HashSet<ObjectId>,
}

pub async fn gen_sink_plan(
    handler_args: HandlerArgs,
    stmt: CreateSinkStatement,
    explain_options: Option<ExplainOptions>,
    is_iceberg_engine_internal: bool,
) -> Result<SinkPlanContext> {
    let session = handler_args.session.clone();
    let session = session.as_ref();
    let user_specified_columns = !stmt.columns.is_empty();
    let db_name = &session.database();
    let (sink_schema_name, sink_table_name) =
        Binder::resolve_schema_qualified_name(db_name, &stmt.sink_name)?;

    let mut with_options = handler_args.with_options.clone();

    if session
        .env()
        .system_params_manager()
        .get_params()
        .load()
        .enforce_secret()
        && Feature::SecretManagement.check_available().is_ok()
    {
        enforce_secret_sink(&with_options)?;
    }

    resolve_privatelink_in_with_option(&mut with_options)?;
    let (mut resolved_with_options, connection_type, connector_conn_ref) =
        resolve_connection_ref_and_secret_ref(
            with_options,
            session,
            Some(TelemetryDatabaseObject::Sink),
        )?;
    ensure_connection_type_allowed(connection_type, &SINK_ALLOWED_CONNECTION_CONNECTOR)?;

    // if not using connection, we don't need to check connector match connection type
    if !matches!(connection_type, PbConnectionType::Unspecified) {
        let Some(connector) = resolved_with_options.get_connector() else {
            return Err(RwError::from(ErrorCode::ProtocolError(format!(
                "missing field '{}' in WITH clause",
                CONNECTOR_TYPE_KEY
            ))));
        };
        check_connector_match_connection_type(connector.as_str(), &connection_type)?;
    }

    let partition_info = get_partition_compute_info(&resolved_with_options).await?;

    let context = if let Some(explain_options) = explain_options {
        OptimizerContext::new(handler_args.clone(), explain_options)
    } else {
        OptimizerContext::from_handler_args(handler_args.clone())
    };

    let is_auto_schema_change = resolved_with_options
        .remove(AUTO_SCHEMA_CHANGE_KEY)
        .map(|value| {
            value.parse::<bool>().map_err(|_| {
                ErrorCode::InvalidInputSyntax(format!(
                    "invalid value {} of '{}' option, expect",
                    value, AUTO_SCHEMA_CHANGE_KEY
                ))
            })
        })
        .transpose()?
        .unwrap_or(false);

    if is_auto_schema_change {
        Feature::SinkAutoSchemaChange
            .check_available()
            .map_err(|e| anyhow::anyhow!(e))?;
    }

    // Used for debezium's table name
    let sink_from_table_name;
    // `true` means that sink statement has the form: `CREATE SINK s1 FROM ...`
    // `false` means that sink statement has the form: `CREATE SINK s1 AS <query>`
    let direct_sink_from_name: Option<(ObjectName, bool)>;
    let query = match stmt.sink_from {
        CreateSink::From(from_name) => {
            sink_from_table_name = from_name.0.last().unwrap().real_value();
            direct_sink_from_name = Some((from_name.clone(), is_auto_schema_change));
            if is_auto_schema_change && stmt.into_table_name.is_some() {
                return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                    "auto schema change not supported for sink-into-table".to_owned(),
                )));
            }
            Box::new(gen_query_from_table_name(from_name))
        }
        CreateSink::AsQuery(query) => {
            if is_auto_schema_change {
                return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                    "auto schema change not supported for CREATE SINK AS QUERY".to_owned(),
                )));
            }
            sink_from_table_name = sink_table_name.clone();
            direct_sink_from_name = None;
            query
        }
    };

    let sink_into_table_name = stmt.into_table_name.as_ref().map(|name| name.real_value());

    let (sink_database_id, sink_schema_id) =
        session.get_database_and_schema_id_for_create(sink_schema_name.clone())?;

    let (dependent_relations, dependent_udfs, bound, auto_refresh_schema_from_table) = {
        let mut binder = Binder::new_for_stream(session);
        let auto_refresh_schema_from_table = if let Some((from_name, true)) = &direct_sink_from_name
        {
            let from_relation = binder.bind_relation_by_name(from_name, None, None, true)?;
            if let Relation::BaseTable(table) = from_relation {
                if table.table_catalog.table_type != TableType::Table {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "auto schema change only support on TABLE, but got {:?}",
                        table.table_catalog.table_type
                    ))
                    .into());
                }
                if table.table_catalog.database_id != sink_database_id {
                    return Err(ErrorCode::InvalidInputSyntax(
                        "auto schema change sink does not support created from cross database table".to_owned()
                    )
                        .into());
                }
                for col in &table.table_catalog.columns {
                    if !col.is_hidden() && (col.is_generated() || col.is_rw_sys_column()) {
                        return Err(ErrorCode::InvalidInputSyntax(format!("auto schema change not supported for table with non-hidden generated column or sys column, but got {}", col.name())).into());
                    }
                }
                Some(table.table_catalog)
            } else {
                return Err(RwError::from(ErrorCode::NotSupported(
                    "auto schema change only supported for TABLE".to_owned(),
                    "try recreating the sink from table".to_owned(),
                )));
            }
        } else {
            None
        };
        let bound = binder.bind_query(&query)?;

        (
            binder.included_relations().clone(),
            binder.included_udfs().clone(),
            bound,
            auto_refresh_schema_from_table,
        )
    };

    let col_names = if sink_into_table_name.is_some() {
        parse_column_names(&stmt.columns)
    } else {
        // If column names not specified, use the name in the bound query, which is equal with the plan root's original field name.
        get_column_names(&bound, stmt.columns)?
    };

    if sink_into_table_name.is_some() {
        let prev = resolved_with_options.insert(CONNECTOR_TYPE_KEY.to_owned(), "table".to_owned());

        if prev.is_some() {
            return Err(RwError::from(ErrorCode::BindError(
                "In the case of sinking into table, the 'connector' parameter should not be provided.".to_owned(),
            )));
        }
    }

    let emit_on_window_close = stmt.emit_mode == Some(EmitMode::OnWindowClose);
    if emit_on_window_close {
        context.warn_to_user("EMIT ON WINDOW CLOSE is currently an experimental feature. Please use it with caution.");
    }

    let connector = resolved_with_options
        .get(CONNECTOR_TYPE_KEY)
        .cloned()
        .ok_or_else(|| ErrorCode::BindError(format!("missing field '{CONNECTOR_TYPE_KEY}'")))?;

    let format_desc = match stmt.sink_schema {
        // Case A: new syntax `format ... encode ...`
        Some(f) => {
            validate_compatibility(&connector, &f)?;
            Some(bind_sink_format_desc(session,f)?)
        }
        None => match resolved_with_options.get(SINK_TYPE_OPTION) {
            // Case B: old syntax `type = '...'`
            Some(t) => SinkFormatDesc::from_legacy_type(&connector, t)?.map(|mut f| {
                session.notice_to_user("Consider using the newer syntax `FORMAT ... ENCODE ...` instead of `type = '...'`.");
                if let Some(v) = resolved_with_options.get(SINK_USER_FORCE_APPEND_ONLY_OPTION) {
                    f.options.insert(SINK_USER_FORCE_APPEND_ONLY_OPTION.into(), v.into());
                }
                f
            }),
            // Case C: no format + encode required
            None => None,
        },
    };

    let definition = context.normalized_sql().to_owned();
    let mut plan_root = if is_iceberg_engine_internal {
        Planner::new_for_iceberg_table_engine_sink(context.into()).plan_query(bound)?
    } else {
        Planner::new_for_stream(context.into()).plan_query(bound)?
    };
    if let Some(col_names) = &col_names {
        plan_root.set_out_names(col_names.clone())?;
    };

    let without_backfill = match resolved_with_options.remove(SINK_SNAPSHOT_OPTION) {
        Some(flag) if flag.eq_ignore_ascii_case("false") => {
            if direct_sink_from_name.is_some() || is_iceberg_engine_internal {
                true
            } else {
                return Err(ErrorCode::BindError(
                    "`snapshot = false` only support `CREATE SINK FROM MV or TABLE`".to_owned(),
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
        if target_table_catalog
            .columns()
            .iter()
            .any(|col| !col.nullable())
        {
            notice_to_user(format!(
                "The target table `{}` contains columns with NOT NULL constraints. Any sinked rows violating the constraints will be ignored silently.",
                target_table_catalog.name(),
            ));
        }
    }

    let sink_plan = plan_root.gen_sink_plan(
        sink_table_name,
        definition,
        resolved_with_options,
        emit_on_window_close,
        db_name.to_owned(),
        sink_from_table_name,
        format_desc,
        without_backfill,
        target_table_catalog.clone(),
        partition_info,
        user_specified_columns,
        auto_refresh_schema_from_table,
    )?;

    let sink_desc = sink_plan.sink_desc().clone();

    let mut sink_plan: PlanRef = sink_plan.into();

    let ctx = sink_plan.ctx();
    let explain_trace = ctx.is_explain_trace();
    if explain_trace {
        ctx.trace("Create Sink:");
        ctx.trace(sink_plan.explain_to_string());
    }
    tracing::trace!("sink_plan: {:?}", sink_plan.explain_to_string());

    // TODO(rc): To be consistent with UDF dependency check, we should collect relation dependencies
    // during binding instead of visiting the optimized plan.
    let dependencies =
        RelationCollectorVisitor::collect_with(dependent_relations, sink_plan.clone())
            .into_iter()
            .map(|id| id.table_id() as ObjectId)
            .chain(
                dependent_udfs
                    .into_iter()
                    .map(|id| id.function_id() as ObjectId),
            )
            .collect();

    let sink_catalog = sink_desc.into_catalog(
        SchemaId::new(sink_schema_id),
        DatabaseId::new(sink_database_id),
        UserId::new(session.user_id()),
        connector_conn_ref.map(ConnectionId::from),
    );

    if let Some(table_catalog) = &target_table_catalog {
        for column in sink_catalog.full_columns() {
            if !column.can_dml() {
                unreachable!(
                    "can not derive generated columns and system column `_rw_timestamp` in a sink's catalog, but meet one"
                );
            }
        }

        let table_columns_without_rw_timestamp = table_catalog.columns_without_rw_timestamp();
        let exprs = derive_default_column_project_for_sink(
            &sink_catalog,
            sink_plan.schema(),
            &table_columns_without_rw_timestamp,
            user_specified_columns,
        )?;

        let logical_project = generic::Project::new(exprs, sink_plan);

        sink_plan = StreamProject::new(logical_project).into();

        let exprs = LogicalSource::derive_output_exprs_from_generated_columns(
            &table_columns_without_rw_timestamp,
        )?;

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
        dependencies,
    })
}

// This function is used to return partition compute info for a sink. More details refer in `PartitionComputeInfo`.
// Return:
// `Some(PartitionComputeInfo)` if the sink need to compute partition.
// `None` if the sink does not need to compute partition.
pub async fn get_partition_compute_info(
    with_options: &WithOptionsSecResolved,
) -> Result<Option<PartitionComputeInfo>> {
    let (options, secret_refs) = with_options.clone().into_parts();
    let Some(connector) = options.get(UPSTREAM_SOURCE_KEY).cloned() else {
        return Ok(None);
    };
    let properties = LocalSecretManager::global().fill_secrets(options, secret_refs)?;
    match connector.as_str() {
        ICEBERG_SINK => {
            let iceberg_config = IcebergConfig::from_btreemap(properties)?;
            get_partition_compute_info_for_iceberg(&iceberg_config).await
        }
        _ => Ok(None),
    }
}

#[allow(clippy::unused_async)]
async fn get_partition_compute_info_for_iceberg(
    _iceberg_config: &IcebergConfig,
) -> Result<Option<PartitionComputeInfo>> {
    // TODO: check table if exists
    if _iceberg_config.create_table_if_not_exists {
        return Ok(None);
    }
    let table = _iceberg_config.load_table().await?;
    let partition_spec = table.metadata().default_partition_spec();
    if partition_spec.is_unpartitioned() {
        return Ok(None);
    }

    // Separate the partition spec into two parts: sparse partition and range partition.
    // Sparse partition means that the data distribution is more sparse at a given time.
    // Range partition means that the data distribution is likely same at a given time.
    // Only compute the partition and shuffle by them for the sparse partition.
    let has_sparse_partition = partition_spec.fields().iter().any(|f| match f.transform {
        // Sparse partition
        Transform::Identity | Transform::Truncate(_) | Transform::Bucket(_) => true,
        // Range partition
        Transform::Year
        | Transform::Month
        | Transform::Day
        | Transform::Hour
        | Transform::Void
        | Transform::Unknown => false,
    });
    if !has_sparse_partition {
        return Ok(None);
    }

    let arrow_type = type_to_arrow_type(&iceberg::spec::Type::Struct(
        table.metadata().default_partition_type().clone(),
    ))
    .map_err(|_| {
        RwError::from(ErrorCode::SinkError(
            "Fail to convert iceberg partition type to arrow type".into(),
        ))
    })?;
    let ArrowDataType::Struct(struct_fields) = arrow_type else {
        return Err(RwError::from(ErrorCode::SinkError(
            "Partition type of iceberg should be a struct type".into(),
        )));
    };

    let schema = table.metadata().current_schema();
    let partition_fields = partition_spec
        .fields()
        .iter()
        .map(|f| {
            let source_f =
                schema
                    .field_by_id(f.source_id)
                    .ok_or(RwError::from(ErrorCode::SinkError(
                        "Fail to look up iceberg partition field".into(),
                    )))?;
            Ok((source_f.name.clone(), f.transform))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(PartitionComputeInfo::Iceberg(IcebergPartitionInfo {
        partition_type: IcebergArrowConvert.struct_from_fields(&struct_fields)?,
        partition_fields,
    })))
}

pub async fn handle_create_sink(
    handle_args: HandlerArgs,
    stmt: CreateSinkStatement,
    is_iceberg_engine_internal: bool,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();

    session.check_cluster_limits().await?;

    let if_not_exists = stmt.if_not_exists;
    if let Either::Right(resp) = session.check_relation_name_duplicated(
        stmt.sink_name.clone(),
        StatementType::CREATE_SINK,
        if_not_exists,
    )? {
        return Ok(resp);
    }

    let (mut sink, graph, target_table_catalog, dependencies) = {
        let SinkPlanContext {
            query,
            sink_plan: plan,
            sink_catalog: sink,
            target_table_catalog,
            dependencies,
        } = gen_sink_plan(handle_args, stmt, None, is_iceberg_engine_internal).await?;

        let has_order_by = !query.order_by.is_empty();
        if has_order_by {
            plan.ctx().warn_to_user(
                r#"The ORDER BY clause in the CREATE SINK statement has no effect at all."#
                    .to_owned(),
            );
        }

        let graph = build_graph(plan, Some(GraphJobType::Sink))?;

        (sink, graph, target_table_catalog, dependencies)
    };

    let mut target_table_replace_plan = None;
    if let Some(table_catalog) = target_table_catalog {
        use crate::handler::alter_table_column::hijack_merger_for_target_table;

        let (mut graph, mut table, source, target_job_type) =
            reparse_table_for_sink(&session, &table_catalog).await?;

        sink.original_target_columns = table.columns.clone();

        table
            .incoming_sinks
            .clone_from(&table_catalog.incoming_sinks);

        let incoming_sink_ids: HashSet<_> = table_catalog.incoming_sinks.iter().copied().collect();
        let incoming_sinks = fetch_incoming_sinks(&session, &incoming_sink_ids)?;

        let columns_without_rw_timestamp = table_catalog.columns_without_rw_timestamp();
        for existing_sink in incoming_sinks {
            hijack_merger_for_target_table(
                &mut graph,
                &columns_without_rw_timestamp,
                &existing_sink,
                Some(&existing_sink.unique_identity()),
            )?;
        }

        // for new creating sink, we don't have a unique identity because the sink id is not generated yet.
        hijack_merger_for_target_table(&mut graph, &columns_without_rw_timestamp, &sink, None)?;

        target_table_replace_plan = Some(ReplaceJobPlan {
            replace_job: Some(replace_job_plan::ReplaceJob::ReplaceTable(
                replace_job_plan::ReplaceTable {
                    table: Some(table.to_prost()),
                    source: source.map(|x| x.to_prost()),
                    job_type: target_job_type as _,
                },
            )),
            fragment_graph: Some(graph),
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
        .create_sink(
            sink.to_proto(),
            graph,
            target_table_replace_plan,
            dependencies,
            if_not_exists,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_SINK))
}

pub fn fetch_incoming_sinks(
    session: &Arc<SessionImpl>,
    incoming_sink_ids: &HashSet<SinkId>,
) -> Result<Vec<Arc<SinkCatalog>>> {
    let reader = session.env().catalog_reader().read_guard();
    let mut sinks = Vec::with_capacity(incoming_sink_ids.len());
    let db_name = &session.database();
    for schema in reader.iter_schemas(db_name)? {
        for sink in schema.iter_sink() {
            if incoming_sink_ids.contains(&sink.id.sink_id) {
                sinks.push(sink.clone());
            }
        }
    }

    Ok(sinks)
}

pub(crate) async fn reparse_table_for_sink(
    session: &Arc<SessionImpl>,
    table_catalog: &Arc<TableCatalog>,
) -> Result<(
    StreamFragmentGraph,
    TableCatalog,
    Option<SourceCatalog>,
    TableJobType,
)> {
    // Retrieve the original table definition and parse it to AST.
    let definition = table_catalog.create_sql_ast_purified()?;
    let Statement::CreateTable { name, .. } = &definition else {
        panic!("unexpected statement: {:?}", definition);
    };
    let table_name = name.clone();

    // Create handler args as if we're creating a new table with the altered definition.
    let handler_args = HandlerArgs::new(session.clone(), &definition, Arc::from(""))?;
    let col_id_gen = ColumnIdGenerator::new_alter(table_catalog);

    let (graph, table, source, job_type) = generate_stream_graph_for_replace_table(
        session,
        table_name,
        table_catalog,
        handler_args,
        definition,
        col_id_gen,
        SqlColumnStrategy::FollowUnchecked,
    )
    .await?;

    Ok((graph, table, source, job_type))
}

pub(crate) fn insert_merger_to_union_with_project(
    node: &mut StreamNode,
    project_node: &PbNodeBody,
    uniq_identity: Option<&str>,
) {
    if let Some(NodeBody::Union(_union_node)) = &mut node.node_body {
        // TODO: MergeNode is used as a placeholder, see issue #17658
        node.input.push(StreamNode {
            input: vec![StreamNode {
                node_body: Some(NodeBody::Merge(Box::new(MergeNode {
                    ..Default::default()
                }))),
                ..Default::default()
            }],
            identity: uniq_identity
                .unwrap_or(PbSink::UNIQUE_IDENTITY_FOR_CREATING_TABLE_SINK)
                .to_owned(),
            fields: node.fields.clone(),
            node_body: Some(project_node.clone()),
            ..Default::default()
        });

        return;
    }

    for input in &mut node.input {
        insert_merger_to_union_with_project(input, project_node, uniq_identity);
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
            "column type mismatch: {:?} vs {:?}, column name: {:?}",
            target_type,
            input_type,
            sink_schema.fields()[idx].name
        );
    } else {
        Ok(ExprImpl::InputRef(Box::new(InputRef::new(
            idx,
            input_type.clone(),
        ))))
    }
}

pub(crate) fn derive_default_column_project_for_sink(
    sink: &SinkCatalog,
    sink_schema: &Schema,
    columns: &[ColumnCatalog],
    user_specified_columns: bool,
) -> Result<Vec<ExprImpl>> {
    assert_eq!(sink.full_schema().len(), sink_schema.len());

    let default_column_exprs = TableCatalog::default_column_exprs(columns);

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

    for (idx, column) in columns.iter().enumerate() {
        if !column.can_dml() {
            continue;
        }

        let default_col_expr =
            || -> ExprImpl { rewrite_now_to_proctime(default_column_exprs[idx].clone()) };

        let sink_col_expr = |sink_col_idx: usize| -> Result<ExprImpl> {
            derive_sink_to_table_expr(sink_schema, sink_col_idx, column.data_type())
        };

        // If users specified the columns to be inserted e.g. `CREATE SINK s INTO t(a, b)`, the expressions of `Project` will be generated accordingly.
        // The missing columns will be filled with default value (`null` if not explicitly defined).
        // Otherwise, e.g. `CREATE SINK s INTO t`, the columns will be matched by their order in `select` query and the target table.
        #[allow(clippy::collapsible_else_if)]
        if user_specified_columns {
            if let Some(idx) = sink_visible_col_idxes_by_name.get(column.name()) {
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
fn bind_sink_format_desc(
    session: &SessionImpl,
    value: FormatEncodeOptions,
) -> Result<SinkFormatDesc> {
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
        E::Parquet => SinkEncode::Parquet,
        e @ (E::Native | E::Csv | E::Bytes | E::None | E::Text) => {
            return Err(ErrorCode::BindError(format!("sink encode unsupported: {e}")).into());
        }
    };

    let mut key_encode = None;
    if let Some(encode) = value.key_encode {
        match encode {
            E::Text => key_encode = Some(SinkEncode::Text),
            E::Bytes => key_encode = Some(SinkEncode::Bytes),
            _ => {
                return Err(ErrorCode::BindError(format!(
                    "sink key encode unsupported: {encode}, only TEXT and BYTES supported"
                ))
                .into());
            }
        }
    }

    let (props, connection_type_flag, schema_registry_conn_ref) =
        resolve_connection_ref_and_secret_ref(
            WithOptions::try_from(value.row_options.as_slice())?,
            session,
            Some(TelemetryDatabaseObject::Sink),
        )?;
    ensure_connection_type_allowed(
        connection_type_flag,
        &SINK_ALLOWED_CONNECTION_SCHEMA_REGISTRY,
    )?;
    let (mut options, secret_refs) = props.into_parts();

    options
        .entry(TimestamptzHandlingMode::OPTION_KEY.to_owned())
        .or_insert(TimestamptzHandlingMode::FRONTEND_DEFAULT.to_owned());

    Ok(SinkFormatDesc {
        format,
        encode,
        options,
        secret_refs,
        key_encode,
        connection_id: schema_registry_conn_ref,
    })
}

static CONNECTORS_COMPATIBLE_FORMATS: LazyLock<HashMap<String, HashMap<Format, Vec<Encode>>>> =
    LazyLock::new(|| {
        use risingwave_connector::sink::Sink as _;
        use risingwave_connector::sink::file_sink::azblob::AzblobSink;
        use risingwave_connector::sink::file_sink::fs::FsSink;
        use risingwave_connector::sink::file_sink::gcs::GcsSink;
        use risingwave_connector::sink::file_sink::opendal_sink::FileSink;
        use risingwave_connector::sink::file_sink::s3::{S3Sink, SnowflakeSink};
        use risingwave_connector::sink::file_sink::webhdfs::WebhdfsSink;
        use risingwave_connector::sink::google_pubsub::GooglePubSubSink;
        use risingwave_connector::sink::kafka::KafkaSink;
        use risingwave_connector::sink::kinesis::KinesisSink;
        use risingwave_connector::sink::mqtt::MqttSink;
        use risingwave_connector::sink::pulsar::PulsarSink;
        use risingwave_connector::sink::redis::RedisSink;

        convert_args!(hashmap!(
                GooglePubSubSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json],
                ),
                KafkaSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Avro, Encode::Protobuf],
                    Format::Upsert => vec![Encode::Json, Encode::Avro, Encode::Protobuf],
                    Format::Debezium => vec![Encode::Json],
                ),
                FileSink::<S3Sink>::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Parquet, Encode::Json],
                ),
                FileSink::<SnowflakeSink>::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Parquet, Encode::Json],
                ),
                FileSink::<GcsSink>::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Parquet, Encode::Json],
                ),
                FileSink::<AzblobSink>::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Parquet, Encode::Json],
                ),
                FileSink::<WebhdfsSink>::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Parquet, Encode::Json],
                ),
                FileSink::<FsSink>::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Parquet, Encode::Json],
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

pub fn validate_compatibility(connector: &str, format_desc: &FormatEncodeOptions) -> Result<()> {
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

    // only allow Kafka connector work with `bytes` as key encode
    if let Some(encode) = &format_desc.key_encode
        && connector != KAFKA_SINK
        && matches!(encode, Encode::Bytes)
    {
        return Err(ErrorCode::BindError(format!(
            "key encode bytes only works with kafka connector, but found {}",
            connector
        ))
        .into());
    }

    Ok(())
}

#[cfg(test)]
pub mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::{LocalFrontend, PROTO_FILE_DATA, create_proto_file};

    #[tokio::test]
    async fn test_create_sink_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t1
    WITH (connector = 'kafka', kafka.topic = 'abc', kafka.brokers = 'localhost:1001')
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
                        mysql.password = '<password>', type = 'append-only', force_append_only = 'true');"#.to_owned();
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
            .get_created_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "mv1")
            .unwrap();
        assert_eq!(table.name(), "mv1");

        // Check sink exists.
        let (sink, _) = catalog_reader
            .get_created_sink_by_name(DEFAULT_DATABASE_NAME, SchemaPath::Name(schema_name), "snk1")
            .unwrap();
        assert_eq!(sink.name, "snk1");
    }
}
