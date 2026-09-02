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

use anyhow::anyhow;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::secret::LocalSecretManager;
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_pb::stream_plan::CompactionResolverNode;
use risingwave_storage::StateStore;

use crate::error::StreamResult;
use crate::executor::{CompactionResolverExecutor, Executor, StreamExecutorError};
use crate::from_proto::ExecutorBuilder;
use crate::task::ExecutorParams;

pub struct CompactionResolverExecutorBuilder;

impl_stream_node_body!(CompactionResolver(CompactionResolverNode) => CompactionResolverExecutorBuilder);

impl ExecutorBuilder for CompactionResolverExecutorBuilder {
    type Node = CompactionResolverNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        assert!(
            params.input.is_empty(),
            "compaction resolver executor should not have input"
        );

        let sink_id = node.sink_id;

        let properties_with_secret = LocalSecretManager::global()
            .fill_secrets(node.properties.clone(), node.secret_refs.clone())?;
        let iceberg_config = IcebergConfig::from_btreemap(properties_with_secret)
            .map_err(|err| StreamExecutorError::from((err, sink_id)))?;

        let pk_indices = node
            .pk_columns
            .iter()
            .map(|column| column.data_file_index as usize)
            .collect::<Vec<_>>();
        if pk_indices.is_empty() {
            return Err(anyhow!("missing primary-key columns in compaction resolver").into());
        }

        let pk_data_types = node
            .pk_columns
            .iter()
            .map(|column| {
                column
                    .column_desc
                    .as_ref()
                    .map(ColumnDesc::from)
                    .map(|column| column.data_type)
                    .ok_or_else(|| anyhow!("compaction resolver PK column missing column_desc"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let barrier_receiver = params
            .local_barrier_manager
            .subscribe_barrier(params.actor_context.id);
        let local_barrier_manager = params.local_barrier_manager.clone();
        let meta_client = params.env.meta_client().ok_or_else(|| {
            anyhow!("meta client is required for iceberg pk-index compaction resolver")
        })?;
        let exec = CompactionResolverExecutor::new(
            params.actor_context,
            sink_id,
            iceberg_config,
            pk_indices,
            pk_data_types,
            params.config.developer.chunk_size,
            local_barrier_manager,
            barrier_receiver,
            meta_client,
        );
        Ok((params.info, exec).into())
    }
}
