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

use risingwave_pb::stream_plan::BarrierRecvNode;
use tokio::sync::mpsc::unbounded_channel;

use super::*;
use crate::executor::BarrierRecvExecutor;

pub struct BarrierRecvExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for BarrierRecvExecutorBuilder {
    type Node = BarrierRecvNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        _node: &Self::Node,
        _store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        assert!(
            params.input.is_empty(),
            "barrier receiver should not have input"
        );

        let (sender, barrier_receiver) = unbounded_channel();
        stream
            .context
            .lock_barrier_manager()
            .register_sender(params.actor_context.id, sender);

        Ok(
            BarrierRecvExecutor::new(params.actor_context, barrier_receiver, params.executor_id)
                .boxed(),
        )
    }
}
