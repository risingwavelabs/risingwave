use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use opentelemetry::metrics::MeterProvider;
use opentelemetry::KeyValue;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use tracing::event;
use tracing_futures::Instrument;

use crate::executor::monitor::DEFAULT_COMPUTER_STATS;
use crate::executor::{Executor, Message, PkIndicesRef};

/// Barrier event might quickly flush the log to millions of lines. Should enable this when you
/// really want to debug.
pub const ENABLE_BARRIER_EVENT: bool = false;

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
    actor_id: u32,
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
        actor_id: u32,
    ) -> Self {
        Self {
            input,
            input_desc,
            input_pos,
            actor_id,
        }
    }
}

#[async_trait]
impl Executor for TraceExecutor {
    async fn next(&mut self) -> Result<Message> {
        let stats = DEFAULT_COMPUTER_STATS.clone();
        let meter = stats
            .prometheus_exporter
            .provider()
            .unwrap()
            .meter("compute_monitor", None);
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
                match &message {
                    Message::Chunk(chunk) => {
                        if chunk.cardinality() > 0 {
                            let attributes =
                                vec![KeyValue::new("actor_id", self.actor_id.to_string())];
                            let actor_row_count = meter
                                .u64_counter("actor_row_count")
                                .with_description("")
                                .init();
                            actor_row_count.add(chunk.cardinality() as u64, &attributes);
                            event!(tracing::Level::TRACE, prev = %input_desc, msg = "chunk", "input = \n{:#?}", chunk);
                        }
                    }
                    Message::Barrier(barrier) => {
                        if ENABLE_BARRIER_EVENT {
                            event!(tracing::Level::TRACE, prev = %input_desc, msg = "barrier", epoch = barrier.epoch, actor_id = self.actor_id, "process barrier");
                        }
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

    fn identity(&self) -> &str {
        self.input.identity()
    }
}
