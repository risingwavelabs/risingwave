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

use risingwave_common::catalog::ConflictBehavior;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_common::util::value_encoding::column_aware_row_encoding::ColumnAwareSerde;
use risingwave_common::util::value_encoding::BasicSerde;
use risingwave_pb::stream_plan::{ArrangeNode, MaterializeNode};

use super::*;
use crate::executor::replicate::ReplicateExecutor;

// #[async_trait::async_trait]
// impl ExecutorBuilder for ReplicateExecutorBuilder {
//     type Node = MaterializeNode;
//
//     async fn new_boxed_executor(
//         params: ExecutorParams,
//         node: &Self::Node,
//         store: impl StateStore,
//         stream: &mut LocalStreamManagerCore,
//     ) -> StreamResult<BoxedExecutor> {
//         let [input]: [_; 1] = params.input.try_into().unwrap();
//
//         let order_key = node
//             .column_orders
//             .iter()
//             .map(ColumnOrder::from_protobuf)
//             .collect();
//
//         let table = node.get_table()?;
//         let versioned = table.version.is_some();
//
//         let conflict_behavior =
//             ConflictBehavior::from_protobuf(&table.handle_pk_conflict_behavior());
//
//         macro_rules! new_executor {
//             ($SD:ident) => {
//                 MaterializeExecutor::<_, $SD>::new(
//                     input,
//                     store,
//                     order_key,
//                     params.executor_id,
//                     params.actor_context,
//                     params.vnode_bitmap.map(Arc::new),
//                     table,
//                     stream.get_watermark_epoch(),
//                     conflict_behavior,
//                     stream.streaming_metrics.clone(),
//                 )
//                 .await
//                 .boxed()
//             };
//         }
//
//         let executor = if versioned {
//             new_executor!(ColumnAwareSerde)
//         } else {
//             new_executor!(BasicSerde)
//         };
//
//         Ok(executor)
//     }
// }

pub struct ReplicateExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for ReplicateExecutorBuilder {
    type Node = ArrangeNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let keys = node
            .get_table_info()?
            .arrange_key_orders
            .iter()
            .map(ColumnOrder::from_protobuf)
            .collect();

        let table = node.get_table()?;

        let vnodes = params.vnode_bitmap.map(Arc::new);
        let executor = ReplicateExecutor::<_, BasicSerde>::new(
            input,
            store,
            keys,
            params.executor_id,
            params.actor_context,
            vnodes,
            table,
        )
        .await;

        Ok(executor.boxed())
    }
}
