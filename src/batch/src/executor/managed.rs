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

use futures::future::{select, Either};
use futures::stream::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, RwError};
use tokio::sync::watch::Receiver;
use tracing::Instrument;

use crate::executor::{BoxedExecutor, Executor};
use crate::task::ShutdownMsg;

/// `ManagedExecutor` build on top of the underlying executor. For now, it does two things:
/// 1. the duration of performance-critical operations will be traced, such as open/next/close.
/// 2. receive shutdown signal
pub struct ManagedExecutor {
    child: BoxedExecutor,
    shutdown_rx: Receiver<ShutdownMsg>,
}

impl ManagedExecutor {
    pub fn new(child: BoxedExecutor, shutdown_rx: Receiver<ShutdownMsg>) -> Self {
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

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn execute(mut self: Box<Self>) {
        let input_desc = self.child.identity().to_string();
        let span = tracing::info_span!("batch_executor", "otel.name" = input_desc);

        let mut child_stream = self.child.execute();

        loop {
            let shutdown = pin!(self.shutdown_rx.changed());
            let res = select(shutdown, child_stream.next().instrument(span.clone())).await;

            match res {
                Either::Left((res, _)) => {
                    res.expect("shutdown_rx should not drop before task finish");
                    break;
                }
                Either::Right((res, _)) => {
                    if let Some(chunk) = res {
                        yield chunk?;
                    } else {
                        return Ok(());
                    }
                }
            }
        }

        let shutdown_msg = self.shutdown_rx.borrow().clone();
        match shutdown_msg {
            ShutdownMsg::Abort(reason) => {
                Err(ErrorCode::BatchError(reason.into()))?;
            }
            ShutdownMsg::Cancel => {
                Err(ErrorCode::BatchError("".into()))?;
            }
            ShutdownMsg::Init => {}
        }
    }
}
