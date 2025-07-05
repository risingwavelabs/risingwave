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

use risingwave_pb::stream_plan::VectorIndexWriteNode;
use risingwave_storage::StateStore;

use crate::error::StreamResult;
use crate::executor::{Executor, VectorIndexWriteExecutor};
use crate::from_proto::ExecutorBuilder;
use crate::task::ExecutorParams;

pub struct VectorIndexWriteExecutorBuilder;

impl ExecutorBuilder for VectorIndexWriteExecutorBuilder {
    type Node = VectorIndexWriteNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let vector_column_id = node.vector_column_idx as usize;
        let info_column_ids = node
            .info_column_indices
            .iter()
            .map(|&id| id as usize)
            .collect();

        let executor = VectorIndexWriteExecutor::<_>::new(
            input,
            store,
            node.table_id.into(),
            vector_column_id,
            info_column_ids,
        )
        .await?;
        Ok(Executor::new(params.info, Box::new(executor)))
    }
}
