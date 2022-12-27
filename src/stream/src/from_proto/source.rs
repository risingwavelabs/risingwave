// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::catalog::{ColumnId, Field, Schema, TableId};
use risingwave_common::types::DataType;
use risingwave_pb::stream_plan::SourceNode;
use risingwave_source::SourceDescBuilder;
use tokio::sync::mpsc::unbounded_channel;

use super::*;
use crate::executor::state_table_handler::SourceStateTableHandler;
use crate::executor::{FsSourceExecutor, SourceExecutor};

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

        let source_id = TableId::new(node.source_id);
        let source_name = node.source_name.clone();

        let source_builder = SourceDescBuilder::new(
            source_id,
            node.row_id_index.clone(),
            node.columns.clone(),
            node.pk_column_ids.clone(),
            node.properties.clone(),
            node.get_info()?.get_source_info()?.clone(),
            params.env.source_manager_ref(),
            params.env.connector_params(),
        );

        let columns = node.columns.clone();
        let column_ids: Vec<_> = columns
            .iter()
            .map(|column| ColumnId::from(column.get_column_desc().unwrap().column_id))
            .collect();
        let fields = columns
            .iter()
            .map(|prost| {
                let column_desc = prost.column_desc.as_ref().unwrap();
                let data_type = DataType::from(column_desc.column_type.as_ref().unwrap());
                let name = column_desc.name.clone();
                Field::with_name(data_type, name)
            })
            .collect();
        let schema = Schema::new(fields);

        let vnodes = params
            .vnode_bitmap
            .expect("vnodes not set for source executor");

        let state_table_handler =
            SourceStateTableHandler::from_table_catalog(node.state_table.as_ref().unwrap(), store)
                .await;

        // so ugly here, need some graceful method
        let is_s3 = node
            .properties
            .get("connector")
            .map(|s| s.to_lowercase())
            .unwrap_or_default()
            .eq("s3");
        if is_s3 {
            Ok(Box::new(FsSourceExecutor::new(
                params.actor_context,
                source_builder,
                source_id,
                source_name,
                vnodes,
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
            Ok(Box::new(SourceExecutor::new(
                params.actor_context,
                source_builder,
                source_id,
                source_name,
                vnodes,
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
        }
    }
}
