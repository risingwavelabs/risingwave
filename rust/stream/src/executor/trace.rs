use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use tracing::event;
use tracing_futures::Instrument;

use super::{Barrier, Executor, Message, PkIndicesRef};

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
    /// Current epoch
    epoch: Option<u64>,
}

impl Debug for TraceExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterExecutor")
            .field("input", &self.input)
            .finish()
    }
}

impl TraceExecutor {
    pub fn new(input: Box<dyn Executor>, input_desc: String) -> Self {
        Self {
            input,
            input_desc,
            epoch: None,
        }
    }
}

#[async_trait]
impl Executor for TraceExecutor {
    async fn next(&mut self) -> Result<Message> {
        let input_desc = self.input_desc.as_str();
        let input_message = self
            .input
            .next()
            .instrument(tracing::trace_span!(
                "next",
                // For the upstream trace pipe, its output is our input.
                next = input_desc
            ))
            .await;
        match input_message {
            Ok(message) => {
                match &message {
                    Message::Chunk(chunk) => {
                        event!(tracing::Level::TRACE, prev = %input_desc, epoch = ?self.epoch, "\n{:#?}", chunk);
                    }
                    Message::Barrier(Barrier { epoch, .. }) => {
                        self.epoch = Some(*epoch);
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
