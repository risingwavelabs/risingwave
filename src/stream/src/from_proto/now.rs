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

use risingwave_pb::stream_plan::NowNode;
use risingwave_storage::StateStore;
use tokio::sync::mpsc::unbounded_channel;

use super::ExecutorBuilder;
use crate::common::table::state_table::StateTable;
use crate::error::StreamResult;
use crate::executor::{BoxedExecutor, NowExecutor};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

pub struct NowExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for NowExecutorBuilder {
    type Node = NowNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &NowNode,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let (sender, barrier_receiver) = unbounded_channel();
        stream
            .context
            .lock_barrier_manager()
            .register_sender(params.actor_context.id, sender);

        let table = node.get_state_table()?;
        let state_table = StateTable::from_table_catalog(table, store, None).await;
        stream.streaming_metrics.actor_info_collector.add_table(
            table.id.into(),
            params.actor_context.id,
            &table.name,
        );
        Ok(Box::new(NowExecutor::new(
            barrier_receiver,
            params.executor_id,
            state_table,
        )))
    }
}
