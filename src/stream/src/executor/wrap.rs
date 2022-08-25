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

use std::sync::Arc;

use futures::StreamExt;
use risingwave_common::catalog::Schema;

use super::monitor::StreamingMetrics;
use super::{
    BoxedExecutor, BoxedMessageStream, Executor, ExecutorInfo, MessageStream, PkIndicesRef,
};
use crate::task::ActorId;

mod epoch_check;
mod schema_check;
mod trace;
mod update_check;

struct ExtraInfo {
    /// Index of input to this operator.
    input_pos: usize,

    actor_id: ActorId,
    executor_id: u64,

    metrics: Arc<StreamingMetrics>,
}

/// [`WrapExecutor`] will do some sanity checks and logging for the wrapped executor.
pub struct WrapExecutor {
    input: BoxedExecutor,

    extra: ExtraInfo,
}

impl WrapExecutor {
    pub fn new(
        input: BoxedExecutor,
        input_pos: usize,
        actor_id: ActorId,
        executor_id: u64,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        Self {
            input,
            extra: ExtraInfo {
                input_pos,
                actor_id,
                executor_id,
                metrics,
            },
        }
    }

    #[allow(clippy::let_and_return)]
    fn wrap_debug(
        info: Arc<ExecutorInfo>,
        extra: ExtraInfo,
        stream: impl MessageStream + 'static,
    ) -> impl MessageStream + 'static {
        // Trace
        let stream = trace::trace(
            info.clone(),
            extra.input_pos,
            extra.actor_id,
            extra.executor_id,
            extra.metrics,
            stream,
        );
        // Stack trace
        let stream = trace::stack_trace(info.clone(), extra.actor_id, extra.executor_id, stream);

        // Schema check
        let stream = schema_check::schema_check(info.clone(), stream);
        // Epoch check
        let stream = epoch_check::epoch_check(info.clone(), stream);
        // Update check
        let stream = update_check::update_check(info, stream);

        stream
    }

    #[allow(clippy::let_and_return)]
    fn wrap_release(
        info: Arc<ExecutorInfo>,
        extra: ExtraInfo,
        stream: impl MessageStream + 'static,
    ) -> impl MessageStream + 'static {
        // Metrics
        let stream = trace::metrics(extra.actor_id, extra.executor_id, extra.metrics, stream);
        // Stack trace
        let stream = trace::stack_trace(info.clone(), extra.actor_id, extra.executor_id, stream);

        // Epoch check
        let stream = epoch_check::epoch_check(info, stream);

        stream
    }

    fn wrap(
        info: Arc<ExecutorInfo>,
        extra: ExtraInfo,
        stream: impl MessageStream + 'static,
    ) -> BoxedMessageStream {
        if cfg!(debug_assertions) {
            Self::wrap_debug(info, extra, stream).boxed()
        } else {
            Self::wrap_release(info, extra, stream).boxed()
        }
    }
}

impl Executor for WrapExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        let info = Arc::new(self.input.info());
        Self::wrap(info, self.extra, self.input.execute()).boxed()
    }

    fn execute_with_epoch(self: Box<Self>, epoch: u64) -> BoxedMessageStream {
        let info = Arc::new(self.input.info());
        Self::wrap(info, self.extra, self.input.execute_with_epoch(epoch)).boxed()
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef {
        self.input.pk_indices()
    }

    fn identity(&self) -> &str {
        self.input.identity()
    }
}
