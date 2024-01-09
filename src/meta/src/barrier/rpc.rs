// Copyright 2024 RisingWave Labs
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

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use risingwave_common::util::pending_on_none;

use crate::barrier::command::CommandContext;
use crate::barrier::{BarrierCompletion, BarrierCompletionFuture, GlobalBarrierManagerContext};

pub(super) struct BarrierRpcManager {
    context: GlobalBarrierManagerContext,

    /// Futures that await on the completion of barrier.
    injected_in_progress_barrier: FuturesUnordered<BarrierCompletionFuture>,
}

impl BarrierRpcManager {
    pub(super) fn new(context: GlobalBarrierManagerContext) -> Self {
        Self {
            context,
            injected_in_progress_barrier: FuturesUnordered::new(),
        }
    }

    pub(super) fn clear(&mut self) {
        self.injected_in_progress_barrier = FuturesUnordered::new();
    }

    pub(super) async fn inject_barrier(&mut self, command_context: Arc<CommandContext>) {
        let await_complete_future = self.context.inject_barrier(command_context).await;
        self.injected_in_progress_barrier
            .push(await_complete_future);
    }

    pub(super) async fn next_complete_barrier(&mut self) -> BarrierCompletion {
        pending_on_none(self.injected_in_progress_barrier.next()).await
    }
}
