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
//
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::error::Result;
use tracing::event;
use tracing_futures::Instrument;

use crate::executor::monitor::StreamingMetrics;
use crate::executor::{Executor, Message};
use crate::task::ActorId;

/// `TraceExecutor` prints data passing in the stream graph to stdout.
///
/// The position of `TraceExecutor` in graph:
///
/// ```plain
/// Next <- Trace <- Input <- (Previous) Trace
/// ```
pub struct TraceExecutor {
    /// The input of the current executor
    input: Box<dyn Executor>,
    /// Description of input executor
    input_desc: String,
    /// Input position of the input executor
    input_pos: usize,
    /// Actor id
    #[allow(dead_code)]
    actor_id: ActorId,
    actor_id_string: String,

    // monitor
    metrics: Arc<StreamingMetrics>,

    span_name: String,
}

impl Debug for TraceExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterExecutor")
            .field("input", &self.input)
            .finish()
    }
}

impl TraceExecutor {
    pub fn new(
        input: Box<dyn Executor>,
        input_desc: String,
        input_pos: usize,
        actor_id: ActorId,
        streaming_metrics: Arc<StreamingMetrics>,
    ) -> Self {
        let span_name = format!("{input_desc}_{input_pos}_next");

        Self {
            input,
            input_desc,
            input_pos,
            actor_id,
            actor_id_string: actor_id.to_string(),
            metrics: streaming_metrics,
            span_name,
        }
    }
}

#[async_trait]
impl super::DebugExecutor for TraceExecutor {
    async fn next(&mut self) -> Result<Message> {
        let span_name = self.span_name.as_str();
        let input_desc = self.input_desc.as_str();
        let input_pos = self.input_pos;

        let input_message = self
            .input
            .next()
            .instrument(tracing::trace_span!(
                "next",
                otel.name = span_name,
                // For the upstream trace pipe, its output is our input.
                next = input_desc,
                input_pos = input_pos,
            ))
            .await;
        match input_message {
            Ok(message) => {
                if let Message::Chunk(ref chunk) = message {
                    if chunk.cardinality() > 0 {
                        self.metrics
                            .actor_row_count
                            .with_label_values(&[self.actor_id_string.as_str()])
                            .inc_by(chunk.cardinality() as u64);
                        event!(tracing::Level::TRACE, prev = %input_desc, msg = "chunk", "input = \n{:#?}", chunk);
                    }
                }
                Ok(message)
            }
            Err(e) => Err(e),
        }
    }

    fn input(&self) -> &dyn Executor {
        self.input.as_ref()
    }

    fn input_mut(&mut self) -> &mut dyn Executor {
        self.input.as_mut()
    }
}
