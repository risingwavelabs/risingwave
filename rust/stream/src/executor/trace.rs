use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use tracing::event;
use tracing_futures::Instrument;

use super::{Executor, Message, PkIndicesRef};

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
}

impl Debug for TraceExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterExecutor")
            .field("input", &self.input)
            .finish()
    }
}

impl TraceExecutor {
    pub fn new(input: Box<dyn Executor>, input_desc: String, input_pos: usize) -> Self {
        Self {
            input,
            input_desc,
            input_pos,
        }
    }
}

#[async_trait]
impl Executor for TraceExecutor {
    async fn next(&mut self) -> Result<Message> {
        let input_desc = self.input_desc.as_str();
        let input_pos = self.input_pos;
        let span_name = format!("{}_{}_next", input_desc, input_pos);
        let input_message = self
            .input
            .next()
            .instrument(tracing::trace_span!(
                "next",
                otel.name = span_name.as_str(),
                // For the upstream trace pipe, its output is our input.
                next = input_desc,
                input_pos = input_pos
            ))
            .await;
        match input_message {
            Ok(message) => {
                if let Message::Chunk(ref chunk) = message {
                    if chunk.cardinality() > 0 {
                        event!(tracing::Level::TRACE, prev = %input_desc, msg = "chunk", "input = \n{:#?}", chunk);
                    }
                }
                Ok(message)
            }
            Err(e) => Err(e),
        }
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef {
        self.input.pk_indices()
    }

    fn identity(&self) -> &'static str {
        "TraceExecutor"
    }
}
