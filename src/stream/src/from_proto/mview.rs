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

        let conflict_behavior =
            ConflictBehavior::from_protobuf(&table.handle_pk_conflict_behavior());
        let version_column_index = table.version_column_index;

        macro_rules! new_executor {
            ($SD:ident) => {
                MaterializeExecutor::<_, $SD>::new(
                    input,
                    params.info.schema.clone(),
                    store,
                    order_key,
                    params.actor_context,
                    params.vnode_bitmap.map(Arc::new),
                    table,
                    params.watermark_epoch,
                    conflict_behavior,
                    version_column_index,
                    params.executor_stats.clone(),
                )
                .await
                .boxed()
            };
        }

        let exec = if versioned {
            new_executor!(ColumnAwareSerde)
        } else {
            new_executor!(BasicSerde)
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
        let version_column_index = table.version_column_index;
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
            version_column_index,
            params.executor_stats.clone(),
        )
        .await;

        Ok((params.info, exec).into())
    }
}
