// Copyright 2023 Singularity Data
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
use risingwave_pb::stream_plan::SourceNode;
use risingwave_source::connector_source::SourceDescBuilderV2;
use risingwave_storage::panic_store::PanicStateStore;
use tokio::sync::mpsc::unbounded_channel;

use super::*;
use crate::executor::source_executor::{SourceExecutor, StreamSourceCore};
use crate::executor::state_table_handler::SourceStateTableHandler;
use crate::executor::FsSourceExecutor;

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

        if let Some(source) = &node.source_inner {
            let source_id = TableId::new(source.source_id);
            let source_name = source.source_name.clone();

            let source_desc_builder = SourceDescBuilderV2::new(
                source.columns.clone(),
                params.env.source_metrics(),
                source.pk_column_ids.clone(),
                source.row_id_index.clone(),
                source.properties.clone(),
                source.get_info()?.clone(),
                params.env.connector_params(),
                params
                    .env
                    .config()
                    .developer
                    .stream_connector_message_buffer_size,
            );

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

            // so ugly here, need some graceful method
            let is_s3 = source
                .properties
                .get("connector")
                .map(|s| s.to_lowercase())
                .unwrap_or_default()
                .eq("s3");
            if is_s3 {
                Ok(Box::new(FsSourceExecutor::new(
                    params.actor_context,
                    source_desc_builder,
                    source_id,
                    source_name,
                    state_table_handler,
                    column_ids,
                    schema,
                    params.pk_indices,
                    barrier_receiver,
                    params.executor_id,
                    params.operator_id,
                    params.op_info,
                    params.executor_stats,
                    stream.config.barrier_interval_ms as u64,
                )?))
            } else {
                let stream_source_core = StreamSourceCore::new(
                    source_id,
                    source_name,
                    column_ids,
                    source_desc_builder,
                    state_table_handler,
                );

                Ok(Box::new(SourceExecutor::new(
                    params.actor_context,
                    schema,
                    params.pk_indices,
                    Some(stream_source_core),
                    params.executor_stats,
                    barrier_receiver,
                    stream.config.barrier_interval_ms as u64,
                    params.executor_id,
                )))
            }
        } else {
            // If there is no external stream source, then no data should be persisted. We pass a
            // `PanicStateStore` type here for indication.
            Ok(Box::new(SourceExecutor::<PanicStateStore>::new(
                params.actor_context,
                params.schema,
                params.pk_indices,
                None,
                params.executor_stats,
                barrier_receiver,
                stream.config.barrier_interval_ms as u64,
                params.executor_id,
            )))
        }
    }
}
