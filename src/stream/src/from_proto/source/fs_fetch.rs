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

use std::sync::Arc;

use risingwave_common::catalog::{ColumnId, Field, Schema, TableId};
use risingwave_common::types::DataType;
use risingwave_connector::source::SourceCtrlOpts;
use risingwave_pb::stream_plan::StreamFsFetchNode;
use risingwave_source::source_desc::SourceDescBuilder;
use risingwave_storage::StateStore;

use crate::error::StreamResult;
use crate::executor::{
    BoxedExecutor, Executor, FlowControlExecutor, FsFetchExecutor, SourceStateTableHandler,
    StreamSourceCore,
};
use crate::from_proto::ExecutorBuilder;
use crate::task::{ExecutorParams, LocalStreamManagerCore};

pub struct FsFetchExecutorBuilder;

impl ExecutorBuilder for FsFetchExecutorBuilder {
    type Node = StreamFsFetchNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [upstream]: [_; 1] = params.input.try_into().unwrap();

        let source = node.node_inner.as_ref().unwrap();

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

        let vnodes = Some(Arc::new(
            params
                .vnode_bitmap
                .expect("vnodes not set for fetch executor"),
        ));
        let state_table_handler = SourceStateTableHandler::from_table_catalog_with_vnodes(
            source.state_table.as_ref().unwrap(),
            store.clone(),
            vnodes,
        )
        .await;
        let stream_source_core = StreamSourceCore::new(
            source_id,
            source_name,
            column_ids,
            source_desc_builder,
            state_table_handler,
        );

        let executor = FsFetchExecutor::new(
            params.actor_context,
            schema,
            params.pk_indices,
            stream_source_core,
            params.executor_id,
            upstream,
            source_ctrl_opts,
            params.env.connector_params(),
        )
        .boxed();

        let rate_limit = source.get_rate_limit().cloned().ok();
        Ok(FlowControlExecutor::new(executor, rate_limit).boxed())
    }
}
