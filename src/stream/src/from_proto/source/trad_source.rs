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

use risingwave_common::catalog::{default_key_column_name_version_mapping, ColumnId, TableId};
use risingwave_connector::source::{
    should_copy_to_format_encode_options, ConnectorProperties, SourceCtrlOpts, UPSTREAM_SOURCE_KEY,
};
use risingwave_pb::data::data_type::TypeName as PbTypeName;
use risingwave_pb::plan_common::{
    AdditionalColumnType, ColumnDescVersion, FormatType, PbEncodeType,
};
use risingwave_pb::stream_plan::SourceNode;
use risingwave_source::source_desc::SourceDescBuilder;
use risingwave_storage::panic_store::PanicStateStore;
use tokio::sync::mpsc::unbounded_channel;

use super::*;
use crate::executor::source::{FsListExecutor, StreamSourceCore};
use crate::executor::source_executor::SourceExecutor;
use crate::executor::state_table_handler::SourceStateTableHandler;
use crate::executor::{FlowControlExecutor, FsSourceExecutor};

const FS_CONNECTORS: &[&str] = &["s3"];
pub struct SourceExecutorBuilder;

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
                let mut source_info = source.get_info()?.clone();

                if source_info.format_encode_options.is_empty() {
                    // compatible code: quick fix for <https://github.com/risingwavelabs/risingwave/issues/14755>,
                    // will move the logic to FragmentManager::init in release 1.7.
                    let connector = source
                        .with_properties
                        .get(UPSTREAM_SOURCE_KEY)
                        .unwrap_or(&String::default())
                        .to_owned();
                    source_info.format_encode_options.extend(
                        source.with_properties.iter().filter_map(|(k, v)| {
                            should_copy_to_format_encode_options(k, &connector)
                                .then_some((k.to_owned(), v.to_owned()))
                        }),
                    );
                }

                let mut source_columns = source.columns.clone();

                {
                    // compatible code: introduced in https://github.com/risingwavelabs/risingwave/pull/13707
                    // for upsert and (avro | protobuf) overwrite the `_rw_key` column's ColumnDesc.additional_column_type to Key
                    if source_info.format() == FormatType::Upsert
                        && (source_info.row_encode() == PbEncodeType::Avro
                            || source_info.row_encode() == PbEncodeType::Protobuf)
                    {
                        let _ = source_columns.iter_mut().map(|c| {
                            let _ = c.column_desc.as_mut().map(|desc| {
                                let is_bytea = desc
                                    .get_column_type()
                                    .map(|col_type| col_type.type_name == PbTypeName::Bytea as i32)
                                    .unwrap();
                                if desc.name == default_key_column_name_version_mapping(
                                    &desc.version()
                                )
                                    && is_bytea
                                    // the column is from a legacy version
                                    && desc.version == ColumnDescVersion::Unspecified as i32
                                {
                                    desc.additional_column_type = AdditionalColumnType::Key as i32;
                                }
                            });
                        });
                    }
                }

                let source_desc_builder = SourceDescBuilder::new(
                    source_columns.clone(),
                    params.env.source_metrics(),
                    source.row_id_index.map(|x| x as _),
                    source.with_properties.clone(),
                    source_info,
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
                    params.info.pk_indices.clone(),
                );

                let source_ctrl_opts = SourceCtrlOpts {
                    chunk_size: params.env.config().developer.chunk_size,
                    rate_limit: source.rate_limit.map(|x| x as _),
                };

                let source_column_ids: Vec<_> = source_columns
                    .iter()
                    .map(|column| ColumnId::from(column.get_column_desc().unwrap().column_id))
                    .collect();

                let state_table_handler = SourceStateTableHandler::from_table_catalog(
                    source.state_table.as_ref().unwrap(),
                    store.clone(),
                )
                .await;
                let stream_source_core = StreamSourceCore::new(
                    source_id,
                    source_name,
                    source_column_ids,
                    source_desc_builder,
                    state_table_handler,
                );

                let connector = source
                    .with_properties
                    .get("connector")
                    .map(|c| c.to_ascii_lowercase())
                    .unwrap_or_default();
                let is_fs_connector = FS_CONNECTORS.contains(&connector.as_str());
                let is_fs_v2_connector =
                    ConnectorProperties::is_new_fs_connector_hash_map(&source.with_properties);

                if is_fs_connector {
                    FsSourceExecutor::new(
                        params.actor_context.clone(),
                        params.info,
                        stream_source_core,
                        params.executor_stats,
                        barrier_receiver,
                        system_params,
                        source_ctrl_opts,
                    )?
                    .boxed()
                } else if is_fs_v2_connector {
                    FsListExecutor::new(
                        params.actor_context.clone(),
                        params.info.clone(),
                        Some(stream_source_core),
                        params.executor_stats.clone(),
                        barrier_receiver,
                        system_params,
                        source_ctrl_opts.clone(),
                        params.env.connector_params(),
                    )
                    .boxed()
                } else {
                    SourceExecutor::new(
                        params.actor_context.clone(),
                        params.info.clone(),
                        Some(stream_source_core),
                        params.executor_stats.clone(),
                        barrier_receiver,
                        system_params,
                        source_ctrl_opts.clone(),
                        params.env.connector_params(),
                    )
                    .boxed()
                }
            };
            let rate_limit = source.rate_limit.map(|x| x as _);
            Ok(FlowControlExecutor::new(executor, params.actor_context, rate_limit).boxed())
        } else {
            // If there is no external stream source, then no data should be persisted. We pass a
            // `PanicStateStore` type here for indication.
            Ok(SourceExecutor::<PanicStateStore>::new(
                params.actor_context,
                params.info,
                None,
                params.executor_stats,
                barrier_receiver,
                system_params,
                // we don't expect any data in, so no need to set chunk_sizes
                SourceCtrlOpts::default(),
                params.env.connector_params(),
            )
            .boxed())
        }
    }
}
