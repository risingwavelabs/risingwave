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

use risingwave_common::secret::LocalSecretManager;
use risingwave_connector::sink::iceberg::{
    ICEBERG_SINK, IcebergConfig, create_and_validate_table_impl,
};
use risingwave_pb::id::SinkId;
use risingwave_pb::stream_plan::IcebergV3ResolveNode;
use risingwave_storage::StateStore;

use super::super::sink::build_sink_param;
use crate::error::StreamResult;
use crate::executor::{Executor, ResolveExecutor, StreamExecutorError};
use crate::from_proto::ExecutorBuilder;
use crate::task::ExecutorParams;

pub struct IcebergV3ResolveExecutorBuilder;

impl_stream_node_body!(IcebergV3ResolveExecutor(IcebergV3ResolveExecutorNode) => IcebergV3ResolveExecutorBuilder);

impl ExecutorBuilder for IcebergV3ResolveExecutorBuilder {
    type Node = IcebergV3ResolveNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        assert!(
            params.input.is_empty(),
            "iceberg v3 resolve is a leaf and should not have input"
        );

        let sink_desc = node.sink_desc.as_ref().unwrap();
        let sink_id: SinkId = sink_desc.get_id();

        // Mirror the writer's table-loading path so the resolve scan sees the same table/schema.
        let properties_with_secret = LocalSecretManager::global().fill_secrets(
            sink_desc.get_properties().clone(),
            sink_desc.get_secret_refs().clone(),
        )?;
        let config = IcebergConfig::from_btreemap(properties_with_secret.clone())
            .map_err(|err| StreamExecutorError::from((err, sink_id)))?;
        let (sink_param, _columns) =
            build_sink_param(sink_desc, properties_with_secret, ICEBERG_SINK)?;
        let table = create_and_validate_table_impl(&config, &sink_param)
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;

        let output_files = ResolveExecutor::decode_output_files(&table, &node.output_files)?;

        // Leaf executor: barriers come straight from the local barrier manager.
        let barrier_receiver = params
            .local_barrier_manager
            .subscribe_barrier(params.actor_context.id);

        // The resolve fragment mirrors the writer's hash distribution. The actor's vnode ownership
        // selects which output FILES it scans (each file's path hashes to one owning actor), so each
        // output file is scanned exactly once across the fragment.
        let vnodes = params.vnode_bitmap.clone().map(std::sync::Arc::new);

        let exec = ResolveExecutor::new(
            params.actor_context,
            sink_id,
            table,
            output_files,
            node.pk_column_names.clone(),
            vnodes,
            barrier_receiver,
            params.local_barrier_manager.clone(),
            params.config.developer.chunk_size,
        );
        Ok((params.info, exec).into())
    }
}
