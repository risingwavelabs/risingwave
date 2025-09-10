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

use risingwave_pb::stream_plan::GlobalApproxPercentileNode;

use crate::common::table::state_table::StateTableBuilder;
use crate::executor::GlobalApproxPercentileExecutor;
use crate::from_proto::*;

pub struct GlobalApproxPercentileExecutorBuilder;

impl ExecutorBuilder for GlobalApproxPercentileExecutorBuilder {
    type Node = GlobalApproxPercentileNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let bucket_table = node
            .bucket_state_table
            .as_ref()
            .expect("bucket_state_table not provided");
        let count_table = node
            .count_state_table
            .as_ref()
            .expect("count_state_table not provided");
        let bucket_state_table = StateTableBuilder::new(bucket_table, store.clone(), None)
            .enable_preload_all_rows_by_config(&params.actor_context.streaming_config)
            .build()
            .await;
        let count_state_table = StateTableBuilder::new(count_table, store, None)
            .enable_preload_all_rows_by_config(&params.actor_context.streaming_config)
            .build()
            .await;
        let exec = GlobalApproxPercentileExecutor::new(
            params.actor_context,
            input,
            node.quantile,
            node.base,
            params.env.config().developer.chunk_size,
            bucket_state_table,
            count_state_table,
        )
        .boxed();
        Ok(Executor::new(params.info, exec))
    }
}
