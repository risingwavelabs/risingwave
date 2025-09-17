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

use std::sync::Arc;

use risingwave_common::catalog::ConflictBehavior;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_common::util::value_encoding::BasicSerde;
use risingwave_common::util::value_encoding::column_aware_row_encoding::ColumnAwareSerde;
use risingwave_pb::stream_plan::{ArrangeNode, MaterializeNode};

use super::*;
use crate::executor::MaterializeExecutor;
use crate::executor::materialize::RefreshableMaterializeArgs;

pub struct MaterializeExecutorBuilder;

impl ExecutorBuilder for MaterializeExecutorBuilder {
    type Node = MaterializeNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let order_key = node
            .column_orders
            .iter()
            .map(ColumnOrder::from_protobuf)
            .collect();

        let table = node.get_table()?;
        let versioned = table.version.is_some();
        let refreshable = table.refreshable;

        let conflict_behavior =
            ConflictBehavior::from_protobuf(&table.handle_pk_conflict_behavior());
        let version_column_indices: Vec<u32> = table.version_column_indices.clone();

        let exec = if refreshable {
            // Create refresh args for refreshable tables
            let refresh_args = RefreshableMaterializeArgs::<_, ColumnAwareSerde>::new(
                store.clone(),
                table,
                node.staging_table.as_ref().unwrap(),
                node.refresh_progress_table.as_ref().unwrap(),
                params.vnode_bitmap.clone().map(Arc::new),
            )
            .await;

            // Use unified MaterializeExecutor with refresh args
            MaterializeExecutor::<_, ColumnAwareSerde>::new(
                input,
                params.info.schema.clone(),
                store,
                order_key,
                params.actor_context,
                params.vnode_bitmap.map(Arc::new),
                table,
                params.watermark_epoch,
                conflict_behavior,
                version_column_indices.clone(),
                params.executor_stats.clone(),
                Some(refresh_args),
                params.local_barrier_manager.clone(),
            )
            .await
            .boxed()
        } else {
            // Use standard MaterializeExecutor for regular tables (no refresh args)
            if versioned {
                MaterializeExecutor::<_, ColumnAwareSerde>::new(
                    input,
                    params.info.schema.clone(),
                    store,
                    order_key,
                    params.actor_context,
                    params.vnode_bitmap.map(Arc::new),
                    table,
                    params.watermark_epoch,
                    conflict_behavior,
                    version_column_indices.clone(),
                    params.executor_stats.clone(),
                    None, // No refresh args for regular tables
                    params.local_barrier_manager.clone(),
                )
                .await
                .boxed()
            } else {
                MaterializeExecutor::<_, BasicSerde>::new(
                    input,
                    params.info.schema.clone(),
                    store,
                    order_key,
                    params.actor_context,
                    params.vnode_bitmap.map(Arc::new),
                    table,
                    params.watermark_epoch,
                    conflict_behavior,
                    version_column_indices.clone(),
                    params.executor_stats.clone(),
                    None, // No refresh args for regular tables
                    params.local_barrier_manager.clone(),
                )
                .await
                .boxed()
            }
        };

        Ok((params.info, exec).into())
    }
}

pub struct ArrangeExecutorBuilder;

impl ExecutorBuilder for ArrangeExecutorBuilder {
    type Node = ArrangeNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let keys = node
            .get_table_info()?
            .arrange_key_orders
            .iter()
            .map(ColumnOrder::from_protobuf)
            .collect();

        let table = node.get_table()?;

        // FIXME: Lookup is now implemented without cell-based table API and relies on all vnodes
        // being `SINGLETON_VNODE`, so we need to make the Arrange a singleton.
        let vnodes = params.vnode_bitmap.map(Arc::new);
        let conflict_behavior =
            ConflictBehavior::from_protobuf(&table.handle_pk_conflict_behavior());
        let version_column_indices: Vec<u32> = table.version_column_indices.clone();
        let exec = MaterializeExecutor::<_, BasicSerde>::new(
            input,
            params.info.schema.clone(),
            store,
            keys,
            params.actor_context,
            vnodes,
            table,
            params.watermark_epoch,
            conflict_behavior,
            version_column_indices,
            params.executor_stats.clone(),
            None, // ArrangeExecutor doesn't support refresh functionality
            params.local_barrier_manager.clone(),
        )
        .await;

        Ok((params.info, exec).into())
    }
}
