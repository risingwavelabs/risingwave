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

use risingwave_common::catalog::{ColumnId, Field, Schema, TableId};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::source::external::{CdcTableType, SchemaTableName};
use risingwave_connector::source::{ConnectorProperties, SourceCtrlOpts};
use risingwave_pb::stream_plan::SourceNode;
use risingwave_source::source_desc::SourceDescBuilder;
use risingwave_storage::panic_store::PanicStateStore;
use tokio::sync::mpsc::unbounded_channel;

use super::*;
use crate::executor::external::ExternalStorageTable;
use crate::executor::source::{FsListExecutor, StreamSourceCore};
use crate::executor::source_executor::SourceExecutor;
use crate::executor::state_table_handler::SourceStateTableHandler;
use crate::executor::{CdcBackfillExecutor, FlowControlExecutor, FsSourceExecutor};

const FS_CONNECTORS: &[&str] = &["s3"];
pub struct SourceExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for SourceExecutorBuilder {
    type Node = SourceNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let (sender, barrier_receiver) = unbounded_channel();
        stream
            .context
            .lock_barrier_manager()
            .register_sender(params.actor_context.id, sender);
        let system_params = params.env.system_params_manager_ref().get_params();

        if let Some(source) = &node.source_inner {
            let executor = {
                let source_id = TableId::new(source.source_id);
                let source_name = source.source_name.clone();
                let source_info = source.get_info()?;

                let source_desc_builder = SourceDescBuilder::new(
                    source.columns.clone(),
                    params.env.source_metrics(),
                    source.row_id_index.map(|x| x as _),
                    source.properties.clone(),
                    source_info.clone(),
                    params.env.connector_params(),
                    params.env.config().developer.connector_message_buffer_size,
                    // `pk_indices` is used to ensure that a message will be skipped instead of parsed
                    // with null pk when the pk column is missing.
                    //
                    // Currently pk_indices for source is always empty since pk information is not
                    // passed via `StreamSource` so null pk may be emitted to downstream.
                    //
                    // TODO: use the correct information to fill in pk_dicies.
                    // We should consdier add back the "pk_column_ids" field removed by #8841 in
                    // StreamSource
                    params.pk_indices.clone(),
                );

                let source_ctrl_opts = SourceCtrlOpts {
                    chunk_size: params.env.config().developer.chunk_size,
                };

                let column_ids: Vec<_> = source
                    .columns
                    .iter()
                    .map(|column| ColumnId::from(column.get_column_desc().unwrap().column_id))
                    .collect();
                let fields = source
                    .columns
                    .iter()
                    .map(|prost| {
                        let column_desc = prost.column_desc.as_ref().unwrap();
                        let data_type = DataType::from(column_desc.column_type.as_ref().unwrap());
                        let name = column_desc.name.clone();
                        Field::with_name(data_type, name)
                    })
                    .collect();
                let schema = Schema::new(fields);

                let state_table_handler = SourceStateTableHandler::from_table_catalog(
                    source.state_table.as_ref().unwrap(),
                    store.clone(),
                )
                .await;
                let stream_source_core = StreamSourceCore::new(
                    source_id,
                    source_name,
                    column_ids,
                    source_desc_builder,
                    state_table_handler,
                );

                let connector = source
                    .properties
                    .get("connector")
                    .map(|c| c.to_ascii_lowercase())
                    .unwrap_or_default();
                let is_fs_connector = FS_CONNECTORS.contains(&connector.as_str());
                let is_fs_v2_connector =
                    ConnectorProperties::is_new_fs_connector_hash_map(&source.properties);

                if is_fs_connector {
                    FsSourceExecutor::new(
                        params.actor_context,
                        schema,
                        params.pk_indices,
                        stream_source_core,
                        params.executor_stats,
                        barrier_receiver,
                        system_params,
                        params.executor_id,
                        source_ctrl_opts,
                    )?
                    .boxed()
                } else if is_fs_v2_connector {
                    FsListExecutor::new(
                        params.actor_context.clone(),
                        schema.clone(),
                        params.pk_indices.clone(),
                        Some(stream_source_core),
                        params.executor_stats.clone(),
                        barrier_receiver,
                        system_params,
                        params.executor_id,
                        source_ctrl_opts.clone(),
                        params.env.connector_params(),
                    )
                    .boxed()
                } else {
                    let source_exec = SourceExecutor::new(
                        params.actor_context.clone(),
                        schema.clone(),
                        params.pk_indices.clone(),
                        Some(stream_source_core),
                        params.executor_stats.clone(),
                        barrier_receiver,
                        system_params,
                        params.executor_id,
                        source_ctrl_opts.clone(),
                        params.env.connector_params(),
                    );

                    let table_type = CdcTableType::from_properties(&source.properties);
                    if table_type.can_backfill()
                        && let Some(table_desc) = source_info.external_table.clone()
                    {
                        let table_schema = Schema::new(table_desc.columns.iter().map(Into::into).collect());
                        let upstream_table_name = SchemaTableName::from_properties(&source.properties);
                        let table_pk_indices = table_desc
                            .pk
                            .iter()
                            .map(|k| k.column_index as usize)
                            .collect_vec();
                        let table_pk_order_types = table_desc
                            .pk
                            .iter()
                            .map(|desc| OrderType::from_protobuf(desc.get_order_type().unwrap()))
                            .collect_vec();

                        let table_reader = table_type
                            .create_table_reader(source.properties.clone(), table_schema.clone())?;
                        let external_table = ExternalStorageTable::new(
                            TableId::new(source.source_id),
                            upstream_table_name,
                            table_reader,
                            table_schema.clone(),
                            table_pk_order_types,
                            table_pk_indices,
                            (0..table_schema.len()).collect_vec(),
                        );

                        // use the state table from source to store the backfill state (may refactor in future)
                        let source_state_handler = SourceStateTableHandler::from_table_catalog(
                            source.state_table.as_ref().unwrap(),
                            store.clone(),
                        ).await;
                        // use schema from table_desc
                        let cdc_backfill = CdcBackfillExecutor::new(
                            params.actor_context.clone(),
                            external_table,
                            Box::new(source_exec),
                            (0..table_schema.len()).collect_vec(),
                            None,
                            table_schema,
                            params.pk_indices,
                            params.executor_stats,
                            source_state_handler,
                            false,
                            source_ctrl_opts.chunk_size,
                        );
                        cdc_backfill.boxed()
                    } else {
                        source_exec.boxed()
                    }
                }
            };
            let rate_limit = source.get_rate_limit().cloned().ok();
            Ok(FlowControlExecutor::new(executor, rate_limit).boxed())
        } else {
            // If there is no external stream source, then no data should be persisted. We pass a
            // `PanicStateStore` type here for indication.
            Ok(SourceExecutor::<PanicStateStore>::new(
                params.actor_context,
                params.schema,
                params.pk_indices,
                None,
                params.executor_stats,
                barrier_receiver,
                system_params,
                params.executor_id,
                // we don't expect any data in, so no need to set chunk_sizes
                SourceCtrlOpts::default(),
                params.env.connector_params(),
            )
            .boxed())
        }
    }
}
