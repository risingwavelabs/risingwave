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

use async_stream::stream;
use futures::stream::StreamExt;
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode;
use tokio::select;
use tokio::sync::watch::Receiver;

use crate::executor::{BoxedDataChunkStream, BoxedExecutor, Executor};
use crate::task::ShutdownMsg;
use crate::tracing::Instrument;

/// `ManagerExecutor` build on top of the underlying executor. For now, it do two things:
/// 1. the duration of performance-critical operations will be traced, such as open/next/close.
/// 2. receive shutdown signal
pub struct ManagedExecutor {
    child: BoxedExecutor,
    /// Description of input executor
    input_desc: String,
    shutdown_rx: Receiver<ShutdownMsg>,
}

impl ManagedExecutor {
    pub fn new(
        child: BoxedExecutor,
        input_desc: String,
        shutdown_rx: Receiver<ShutdownMsg>,
    ) -> Self {
        Self {
            child,
            input_desc,
            shutdown_rx,
        }
    }
}

impl Executor for ManagedExecutor {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn identity(&self) -> &str {
        self.child.identity()
    }

    fn execute(mut self: Box<Self>) -> BoxedDataChunkStream {
        stream! {
            let input_desc = self.input_desc.as_str();
            let span_name = format!("{input_desc}_next");
            let mut child_stream = self.child.execute();

            loop {
                select! {
                    // We prioritize abort signal over normal data chunks.
                    biased;
                    res = self.shutdown_rx.changed() => {
                        match res {
                            Ok(_) => {
                                let msg = self.shutdown_rx.borrow().clone();
                                match msg {
                                    ShutdownMsg::Abort(reason) => {
                                        yield Err(ErrorCode::BatchError(reason.into()).into());
                                    }
                                    ShutdownMsg::Cancel => {
                                        yield Err(ErrorCode::BatchError("".into()).into());
                                    }
                                    ShutdownMsg::Init => {
                                        unreachable!("Never receive Init message");
                                    }
                                }
                            },
                            Err(_) => {
                                panic!("shutdown_rx should not drop before task finish");
                            }
                        }
                    }
                    res = child_stream.next().instrument(tracing::trace_span!("next", otel.name = span_name.as_str(), next = input_desc)) => {
                        if let Some(chunk) = res {
                            match chunk {
                                Ok(chunk) => {
                                    event!(tracing::Level::TRACE, prev = %input_desc, msg = "chunk", "input = \n{:#?}",chunk);
                                    yield Ok(chunk);
                                },
                                Err(error) => {
                                    yield Err(ErrorCode::BatchError(error.into()).into());
                                }
                            }
                        } else {
                            return;
                        }
                    }
                }
            }
        }
        .boxed()
    }
}
