use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use async_trait::async_trait;
use opentelemetry::metrics::{Counter, MeterProvider};
use opentelemetry::KeyValue;
use risingwave_common::error::Result;
use tracing::event;
use tracing_futures::Instrument;

use crate::executor::monitor::{DEFAULT_COMPUTE_STATS, StreamingMetrics};
use crate::executor::{Executor, Message};
use prometheus::Registry;
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
    actor_id: u32,

    // monitor
    /// attributes of the OpenTelemetry monitor
    attributes: Vec<KeyValue>,
    actor_row_count: Counter<u64>,
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
        actor_id: u32,
        registry: Registry,
    ) -> Self {
        let meter = DEFAULT_COMPUTE_STATS
            .clone()
            .prometheus_exporter
            .provider()
            .unwrap()
            .meter("compute_monitor", None);

        let span_name = format!("{input_desc}_{input_pos}_next");

        Self {
            input,
            input_desc,
            input_pos,
            actor_id,
            attributes: vec![KeyValue::new("actor_id", actor_id.to_string())],
            actor_row_count: meter
                .u64_counter("stream_actor_row_count")
                .with_description("Total number of rows that have been ouput from each actor")
                .init(),
            metrics:Arc::new(StreamingMetrics::new(registry)),
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
                        // self.metrics.boot_metrics_service()
                        self.metrics.actor_row_count.with_label_values(&[self.actor_id.to_string().as_str()]).inc_by(chunk.cardinality() as u64);
                        self.actor_row_count
                            .add(chunk.cardinality() as u64, &self.attributes);
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
