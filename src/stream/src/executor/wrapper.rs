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

use crate::executor::prelude::*;

mod epoch_check;
mod epoch_provide;
mod schema_check;
mod stream_node_metrics;
mod trace;
mod update_check;

/// [`WrapperExecutor`] will do some sanity checks and logging for the wrapped executor.
pub struct WrapperExecutor {
    operator_id: u64,

    input: Executor,

    actor_ctx: ActorContextRef,

    enable_executor_row_count: bool,

    enable_explain_analyze_stats: bool,
}

impl WrapperExecutor {
    pub fn new(
        operator_id: u64,
        input: Executor,
        actor_ctx: ActorContextRef,
        enable_executor_row_count: bool,
        enable_explain_analyze_stats: bool,
    ) -> Self {
        Self {
            operator_id,
            input,
            actor_ctx,
            enable_executor_row_count,
            enable_explain_analyze_stats,
        }
    }

    #[allow(clippy::let_and_return)]
    fn wrap_debug(
        info: Arc<ExecutorInfo>,
        stream: impl MessageStream + 'static,
    ) -> impl MessageStream + 'static {
        // Update check
        let stream = update_check::update_check(info, stream);

        stream
    }

    fn wrap(
        operator_id: u64,
        enable_executor_row_count: bool,
        enable_explain_analyze_stats: bool,
        info: Arc<ExecutorInfo>,
        actor_ctx: ActorContextRef,
        stream: impl MessageStream + 'static,
    ) -> BoxedMessageStream {
        // -- Shared wrappers --

        // Await tree
        let stream = trace::instrument_await_tree(info.clone(), stream);

        // Schema check
        let stream = schema_check::schema_check(info.clone(), stream);
        // Epoch check
        let stream = epoch_check::epoch_check(info.clone(), stream);

        // Epoch provide
        let stream = epoch_provide::epoch_provide(stream);

        // Trace
        let stream = trace::trace(
            enable_executor_row_count,
            info.clone(),
            actor_ctx.clone(),
            stream,
        );

        // operator-level metrics
        let stream = stream_node_metrics::stream_node_metrics(
            enable_explain_analyze_stats,
            operator_id,
            stream,
            actor_ctx.clone(),
        );

        if cfg!(debug_assertions) {
            Self::wrap_debug(info, stream).boxed()
        } else {
            stream.boxed()
        }
    }
}

impl Execute for WrapperExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        let info = Arc::new(self.input.info().clone());
        Self::wrap(
            self.operator_id,
            self.enable_executor_row_count,
            self.enable_explain_analyze_stats,
            info,
            self.actor_ctx,
            self.input.execute(),
        )
        .boxed()
    }

    fn execute_with_epoch(self: Box<Self>, epoch: u64) -> BoxedMessageStream {
        let info = Arc::new(self.input.info().clone());
        Self::wrap(
            self.operator_id,
            self.enable_executor_row_count,
            self.enable_explain_analyze_stats,
            info,
            self.actor_ctx,
            self.input.execute_with_epoch(epoch),
        )
        .boxed()
    }
}
