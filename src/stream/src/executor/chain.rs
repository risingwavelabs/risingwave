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

use risingwave_common::error::Result;
use risingwave_common::try_match_expand;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::StateStore;

use crate::executor::ExecutorBuilder;
use crate::executor_v2::{BoxedExecutor, ChainExecutor, Executor, RearrangedChainExecutor};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

pub struct ChainExecutorBuilder;

impl ExecutorBuilder for ChainExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &stream_plan::StreamNode,
        _store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::ChainNode)?;
        let snapshot = params.input.remove(1);
        let mview = params.input.remove(0);

        // TODO(MrCroxx): Use column_descs to get idx after mv planner can generate stable
        // column_ids. Now simply treat column_id as column_idx.
        // TODO(bugen): how can we know the way of mapping?
        let column_idxs: Vec<usize> = node.column_ids.iter().map(|id| *id as usize).collect();

        // For notifying about creation finish.
        let notifier = stream
            .context
            .register_finish_create_mview_notifier(params.actor_id);

        // The batch query executor scans on a mapped adhoc mview table, thus we should directly use
        // its schema.
        let schema = snapshot.schema().clone();

        if node.disable_rearrange {
            let executor = ChainExecutor::new(snapshot, mview, column_idxs, notifier, schema);
            Ok(executor.boxed())
        } else {
            let executor =
                RearrangedChainExecutor::new(snapshot, mview, column_idxs, notifier, schema);
            Ok(executor.boxed())
        }
    }
}
