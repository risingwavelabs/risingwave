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

pub mod layer;

use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Context for tracing used for propagating tracing information in a distributed system.
///
/// Generally, the caller of a service should create a tracing context from the current tracing span
/// and pass it to the callee through the network. The callee will then attach its local tracing
/// span as a child of the tracing context, so that the external tracing service can associate them
/// in a single trace.
///
/// The tracing context must be serialized and deserialized when passing through the network.
/// There're two ways to do this:
///
/// - For RPC calls with clear caller/callee relationship, the tracing context can be serialized
///   into the W3C trace context format and passed in HTTP headers seamlessly with a service
///   middleware. For example, the DDL requests from the frontend to the meta service.
/// - For RPC calls with no clear caller/callee relationship or with asynchronous semantics, the
///   tracing context should be passed manually as a protobuf field in the request and response for
///   better lifetime management. For example, the exchange services between streaming actors or
///   batch stages, and the `create_task` requests from the frontend to the batch service.
///
/// See [Trace Context](https://www.w3.org/TR/trace-context/) for more information.
#[derive(Debug, Clone)]
pub struct TracingContext(opentelemetry::Context);

type Propagator = TraceContextPropagator;

impl TracingContext {
    /// Create a new tracing context from a tracing span.
    pub fn from_span(span: &tracing::Span) -> Self {
        Self(span.context())
    }

    /// Create a new tracing context from the current tracing span considered by the subscriber.
    pub fn from_current_span() -> Self {
        Self::from_span(&tracing::Span::current())
    }

    /// Create a no-op tracing context.
    pub fn none() -> Self {
        Self(opentelemetry::Context::new())
    }

    /// Attach the given span as a child of the context. Returns the attached span.
    pub fn attach(&self, span: tracing::Span) -> tracing::Span {
        span.set_parent(self.0.clone());
        span
    }

    /// Convert the tracing context to the W3C trace context format.
    fn to_w3c(&self) -> HashMap<String, String> {
        let mut fields = HashMap::new();
        Propagator::new().inject_context(&self.0, &mut fields);
        fields
    }

    /// Create a new tracing context from the W3C trace context format.
    fn from_w3c(fields: &HashMap<String, String>) -> Self {
        let context = Propagator::new().extract(fields);
        Self(context)
    }

    /// Convert the tracing context to the protobuf format.
    pub fn to_protobuf(&self) -> HashMap<String, String> {
        self.to_w3c()
    }

    /// Create a new tracing context from the protobuf format.
    pub fn from_protobuf(fields: &HashMap<String, String>) -> Self {
        Self::from_w3c(fields)
    }

    /// Convert the tracing context to the W3C trace context format in HTTP headers.
    pub fn to_http_headers(&self) -> http::HeaderMap {
        let map = self.to_w3c();
        http::HeaderMap::try_from(&map).unwrap_or_default()
    }

    /// Create a new tracing context from the W3C trace context format in HTTP headers.
    ///
    /// Returns `None` if the headers are invalid.
    pub fn from_http_headers(headers: &http::HeaderMap) -> Option<Self> {
        let mut map = HashMap::new();

        // See [Trace Context](https://www.w3.org/TR/trace-context/) for these header names.
        for key in ["traceparent", "tracestate"] {
            let value = headers.get(key)?.to_str().ok()?;
            map.insert(key.to_owned(), value.to_owned());
        }

        Some(Self::from_w3c(&map))
    }
}

/// Extension trait allowing [`futures::Stream`]s to be instrumented with a [`tracing::Span`].
#[easy_ext::ext(InstrumentStream)]
impl<T> T
where
    T: futures::Stream + Sized,
{
    /// Instruments the stream with the given span. Alias for
    /// [`tracing_futures::Instrument::instrument`].
    ///
    /// The span will be entered and exited every time the stream is polled. The span will be
    /// closed only when the stream is dropped.
    ///
    /// If the stream is long-lived, consider [`InstrumentStream::instrument_with`] instead to
    /// avoid accumulating too many events in the span.
    pub fn instrument(self, span: tracing::Span) -> tracing_futures::Instrumented<Self> {
        tracing_futures::Instrument::instrument(self, span)
    }

    /// Instruments the stream with spans created by the given closure **every time an item is
    /// yielded**.
    pub fn instrument_with<S>(self, make_span: S) -> WithInstrumented<Self, S>
    where
        S: FnMut() -> tracing::Span,
    {
        WithInstrumented::new(self, make_span)
    }
}

pin_project_lite::pin_project! {
    /// A [`futures::Stream`] that has been instrumented with
    /// [`InstrumentStream::instrument_with`].
    #[derive(Debug, Clone)]
    pub struct WithInstrumented<T, S> {
        #[pin]
        inner: T,
        make_span: S,
        current_span: Option<tracing::Span>,
    }
}

impl<T, S> WithInstrumented<T, S> {
    /// Creates a new [`WithInstrumented`] stream.
    fn new(inner: T, make_span: S) -> Self {
        Self {
            inner,
            make_span,
            current_span: None,
        }
    }

    /// Returns the inner stream.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T, S> futures::Stream for WithInstrumented<T, S>
where
    T: futures::Stream + Sized,
    S: FnMut() -> tracing::Span,
{
    type Item = T::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let poll = {
            let _entered = this.current_span.get_or_insert_with(this.make_span).enter();
            this.inner.poll_next(cx)
        };

        if poll.is_ready() {
            // Drop the span when a new item is yielded.
            *this.current_span = None;
        }

        poll
    }
}
