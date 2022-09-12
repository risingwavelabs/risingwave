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

use super::*;
use crate::executor::{ChainExecutor, RearrangedChainExecutor};

pub struct ChainExecutorBuilder;

impl ExecutorBuilder for ChainExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        _store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::Chain)?;
        let [mview, snapshot]: [_; 2] = params.input.try_into().unwrap();

        let upstream_indices: Vec<usize> = node
            .upstream_column_indices
            .iter()
            .map(|&i| i as usize)
            .collect();

        // For reporting the progress.
        let progress = stream
            .context
            .register_create_mview_progress(params.actor_context.id);

        // The batch query executor scans on a mapped adhoc mview table, thus we should directly use
        // its schema.
        let schema = snapshot.schema().clone();

        if node.disable_rearrange {
            let executor = ChainExecutor::new(snapshot, mview, upstream_indices, progress, schema);
            Ok(executor.boxed())
        } else {
            let executor =
                RearrangedChainExecutor::new(snapshot, mview, upstream_indices, progress, schema);
            Ok(executor.boxed())
        }
    }
}
