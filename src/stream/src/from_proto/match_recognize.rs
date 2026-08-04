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

use anyhow::anyhow;
use risingwave_pb::stream_plan::MatchRecognizeNode;
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::error::StreamResult;
use crate::executor::Executor;
use crate::task::ExecutorParams;

pub struct MatchRecognizeExecutorBuilder;

impl_stream_node_body!(MatchRecognize(MatchRecognizeNode) => MatchRecognizeExecutorBuilder);

impl ExecutorBuilder for MatchRecognizeExecutorBuilder {
    type Node = MatchRecognizeNode;

    async fn new_boxed_executor(
        _params: ExecutorParams,
        _node: &MatchRecognizeNode,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        // Keeps the dispatch exhaustive while the ordered-input executor lands in the next change
        // of the series; a plan reaching this in the meantime fails the actor build loudly rather
        // than running silently wrong.
        Err(anyhow!("MATCH_RECOGNIZE executor is not yet wired in this build").into())
    }
}
