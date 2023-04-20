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

use itertools::Itertools;
use risingwave_pb::stream_plan::DedupNode;
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::common::table::state_table::StateTable;
use crate::error::StreamResult;
use crate::executor::{AppendOnlyDedupExecutor, BoxedExecutor};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

pub struct AppendOnlyDedupExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for AppendOnlyDedupExecutorBuilder {
    type Node = DedupNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let table = node.get_state_table()?;
        let vnodes = params.vnode_bitmap.map(Arc::new);
        let state_table = StateTable::from_table_catalog(table, store, vnodes).await;
        let pk_indices = node
            .dedup_column_indices
            .iter()
            .map(|idx| *idx as _)
            .collect_vec();
        Ok(Box::new(AppendOnlyDedupExecutor::new(
            input,
            state_table,
            pk_indices,
            params.executor_id,
            params.actor_context,
            stream.get_watermark_epoch(),
        )))
    }
}
