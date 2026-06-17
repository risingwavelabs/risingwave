// Copyright 2026 RisingWave Labs
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

use anyhow::anyhow;
use risingwave_common::secret::LocalSecretManager;
use risingwave_connector::sink::iceberg::{
    ICEBERG_SINK, IcebergConfig, create_and_validate_table_impl,
};
use risingwave_connector::sink::{SinkMetaClient, SinkWriterParam};
use risingwave_pb::id::SinkId;
use risingwave_pb::stream_plan::IcebergWithPkIndexWriterNode;
use risingwave_storage::StateStore;

use super::super::sink::build_sink_param;
use crate::common::table::state_table::{StateTableBuilder, StateTableOpConsistencyLevel};
use crate::error::StreamResult;
use crate::executor::{Executor, IcebergWriterImpl, StreamExecutorError, WriterExecutor};
use crate::from_proto::ExecutorBuilder;
use crate::task::ExecutorParams;

pub struct IcebergWithPkIndexWriterExecutorBuilder;

impl_stream_node_body!(IcebergWithPkIndexWriter(IcebergWithPkIndexWriterNode) => IcebergWithPkIndexWriterExecutorBuilder);

impl ExecutorBuilder for IcebergWithPkIndexWriterExecutorBuilder {
    type Node = IcebergWithPkIndexWriterNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let sink_desc = node.sink_desc.as_ref().unwrap();
        let sink_id: SinkId = sink_desc.get_id();
        let sink_name = sink_desc.get_name().to_owned();

        let properties_with_secret = LocalSecretManager::global().fill_secrets(
            sink_desc.get_properties().clone(),
            sink_desc.get_secret_refs().clone(),
        )?;
        let config = IcebergConfig::from_btreemap(properties_with_secret.clone())
            .map_err(|err| StreamExecutorError::from((err, sink_id)))?;

        let pk_indices = sink_desc
            .downstream_pk
            .iter()
            .map(|&idx| idx as usize)
            .collect::<Vec<_>>();
        if pk_indices.is_empty() {
            return Err(anyhow!("missing downstream pk in iceberg sink desc").into());
        }

        let (sink_param, columns) =
            build_sink_param(sink_desc, properties_with_secret, ICEBERG_SINK)?;

        // Indices of the visible columns, i.e. the ones actually written to Iceberg. Hidden
        // columns carried by the input chunk for PK derivation are projected away by the writer.
        let write_col_indices = columns
            .iter()
            .enumerate()
            .filter(|(_, col)| !col.is_hidden)
            .map(|(idx, _)| idx)
            .collect::<Vec<_>>();

        let table = create_and_validate_table_impl(&config, &sink_param)
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;

        let pk_index_state_table = StateTableBuilder::new(
            node.get_pk_index_table()?,
            store,
            params.vnode_bitmap.clone().map(Arc::new),
        )
        .enable_preload_all_rows_by_config(&params.config)
        .with_op_consistency_level(StateTableOpConsistencyLevel::Inconsistent)
        .build()
        .await;

        let meta_client = params
            .env
            .meta_client()
            .ok_or_else(|| anyhow!("meta client is required for Iceberg writer"))?;
        let meta_client = SinkMetaClient::MetaClient(meta_client);

        let writer_param = SinkWriterParam {
            executor_id: params.executor_id,
            vnode_bitmap: params.vnode_bitmap.clone(),
            meta_client: Some(meta_client),
            extra_partition_col_idx: sink_desc.extra_partition_col_idx.map(|v| v as usize),
            actor_id: params.actor_context.id,
            sink_id,
            sink_name,
            connector: ICEBERG_SINK.to_owned(),
            streaming_config: params.config.as_ref().clone(),
            time_zone: params.actor_context.time_zone,
        };
        let writer = IcebergWriterImpl::build(&config, table, &writer_param)?;

        let exec = WriterExecutor::new(
            params.actor_context,
            input,
            pk_indices,
            pk_index_state_table,
            write_col_indices,
            writer,
            params.config.developer.chunk_size,
            sink_id,
            params.local_barrier_manager.clone(),
        );
        Ok((params.info, exec).into())
    }
}
