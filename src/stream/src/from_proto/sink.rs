// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::catalog::{ColumnId, Field, Schema, TableId};
use tokio::sync::mpsc::unbounded_channel;

use super::*;
// use crate::executor::SinkExecutor;

pub struct SinkExecutorBuilder;

impl ExecutorBuilder for SinkExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::Sink)?;
        let (sender, barrier_receiver) = unbounded_channel();
        stream
            .context
            .lock_barrier_manager()
            .register_sender(params.actor_id, sender);

        todo!();
        // Ok(Box::new(SinkExecutor::new(
        //         child, //materialize_executor
        //         params.executor_id,
        //     )?))
    }
}
