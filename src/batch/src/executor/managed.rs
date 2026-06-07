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

use std::pin::pin;

use futures::future::{BoxFuture, Either, FutureExt, select};
use futures::stream::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use tracing::Instrument;

use crate::error::{BatchError, Result};
use crate::executor::{
    BatchPipelineOperator, BoxedExecutor, Executor, PushContext, PushSink, PushStatus,
};
use crate::task::{ShutdownMsg, ShutdownToken};

/// `ManagedExecutor` build on top of the underlying executor. For now, it does two things:
/// 1. the duration of performance-critical operations will be traced, such as open/next/close.
/// 2. receive shutdown signal
pub struct ManagedExecutor {
    child: BoxedExecutor,
    shutdown_rx: ShutdownToken,
}

impl ManagedExecutor {
    pub fn new(child: BoxedExecutor, shutdown_rx: ShutdownToken) -> Self {
        Self { child, shutdown_rx }
    }
}

impl Executor for ManagedExecutor {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn identity(&self) -> &str {
        self.child.identity()
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn execute(mut self: Box<Self>) {
        let input_desc = self.child.identity().to_owned();
        let span = tracing::info_span!("batch_executor", "otel.name" = input_desc);

        let mut child_stream = self.child.execute();

        loop {
            let shutdown = pin!(self.shutdown_rx.cancelled());

            match select(shutdown, child_stream.next().instrument(span.clone())).await {
                Either::Left(_) => break,
                Either::Right((res, _)) => {
                    if let Some(chunk) = res {
                        yield chunk?;
                    } else {
                        return Ok(());
                    }
                }
            }
        }

        match self.shutdown_rx.message() {
            ShutdownMsg::Abort(reason) => {
                return Err(BatchError::aborted(reason));
            }
            ShutdownMsg::Cancel => {
                return Err(BatchError::aborted("cancelled"));
            }
            ShutdownMsg::Init => {}
        }
    }

    fn execute_push<'a>(
        self: Box<Self>,
        context: PushContext,
        sink: &'a mut dyn PushSink,
    ) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            let input_desc = self.child.identity().to_owned();
            let span = tracing::info_span!("batch_executor", "otel.name" = input_desc);
            let mut shutdown_rx = self.shutdown_rx.clone();
            let child = self.child;
            let execute = child.execute_push(context, sink).instrument(span);
            tokio::select! {
                _ = shutdown_rx.cancelled() => match shutdown_rx.message() {
                    ShutdownMsg::Abort(reason) => Err(BatchError::aborted(reason)),
                    ShutdownMsg::Cancel => Err(BatchError::aborted("cancelled")),
                    ShutdownMsg::Init => Ok(PushStatus::NeedMoreInput),
                },
                result = execute => result,
            }
        }
        .boxed()
    }

    fn execute_push_with_operators<'a>(
        self: Box<Self>,
        context: PushContext,
        operators: Vec<Box<dyn BatchPipelineOperator>>,
        sink: &'a mut dyn PushSink,
    ) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            let input_desc = self.child.identity().to_owned();
            let span = tracing::info_span!("batch_executor", "otel.name" = input_desc);
            let mut shutdown_rx = self.shutdown_rx.clone();
            let child = self.child;
            let execute = child
                .execute_push_with_operators(context, operators, sink)
                .instrument(span);
            tokio::select! {
                _ = shutdown_rx.cancelled() => match shutdown_rx.message() {
                    ShutdownMsg::Abort(reason) => Err(BatchError::aborted(reason)),
                    ShutdownMsg::Cancel => Err(BatchError::aborted("cancelled")),
                    ShutdownMsg::Init => Ok(PushStatus::NeedMoreInput),
                },
                result = execute => result,
            }
        }
        .boxed()
    }
}
