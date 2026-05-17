// Copyright 2026 RisingWave Labs
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

pub mod test_util;

use std::borrow::Cow;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use fastrace::prelude::SpanContext;

const TRACEPARENT_HEADER: &str = "traceparent";
const TRACESTATE_HEADER: &str = "tracestate";

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
#[derive(Debug, Clone, Default)]
pub struct TracingContext {
    span_context: Option<SpanContext>,
    tracestate: Option<String>,
}

impl TracingContext {
    /// Create a new tracing context from a tracing span.
    pub fn from_span(_span: &tracing::Span) -> Self {
        // fastrace contexts are attached to `fastrace::Span` or to the thread-local fastrace
        // parent stack, not to `tracing::Span`. Until the call sites are migrated to native
        // fastrace spans, the best available source is the current fastrace local parent.
        Self::from_current_span()
    }

    /// Create a new tracing context from a fastrace span.
    pub fn from_fastrace_span(span: &fastrace::Span) -> Self {
        Self {
            span_context: SpanContext::from_span(span),
            tracestate: None,
        }
    }

    /// Create a new tracing context from the current tracing span considered by the subscriber.
    pub fn from_current_span() -> Self {
        Self {
            span_context: SpanContext::current_local_parent(),
            tracestate: None,
        }
    }

    /// Create a no-op tracing context.
    pub fn none() -> Self {
        Self::default()
    }

    /// Attach the given span as a child of the context. Returns the attached span.
    pub fn attach(&self, span: tracing::Span) -> tracing::Span {
        // fastrace has no equivalent of `tracing_opentelemetry::OpenTelemetrySpanExt::set_parent`
        // for a `tracing::Span`. Native fastrace call sites should instead create a fastrace root
        // from `to_fastrace_span_context` and set it as the local parent around the async work.
        span
    }

    /// Returns the fastrace parent context, if one exists.
    pub fn to_fastrace_span_context(&self) -> Option<SpanContext> {
        self.span_context
    }

    /// Create a fastrace root span from this propagated context.
    pub fn root_span(&self, name: impl Into<Cow<'static, str>>) -> fastrace::Span {
        self.span_context
            .map(|parent| fastrace::Span::root(name, parent))
            .unwrap_or_else(fastrace::Span::noop)
    }

    /// Convert the tracing context to the W3C trace context format.
    fn to_w3c(&self) -> HashMap<String, String> {
        let mut fields = HashMap::new();
        if let Some(span_context) = self.span_context {
            fields.insert(
                TRACEPARENT_HEADER.to_owned(),
                span_context.encode_w3c_traceparent(),
            );
            if let Some(tracestate) = &self.tracestate {
                fields.insert(TRACESTATE_HEADER.to_owned(), tracestate.clone());
            }
        }
        fields
    }

    /// Create a new tracing context from the W3C trace context format.
    fn from_w3c(fields: &HashMap<String, String>) -> Self {
        let Some(span_context) = fields
            .get(TRACEPARENT_HEADER)
            .and_then(|traceparent| SpanContext::decode_w3c_traceparent(traceparent))
        else {
            return Self::none();
        };

        Self {
            span_context: Some(span_context),
            tracestate: fields.get(TRACESTATE_HEADER).cloned(),
        }
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
        let mut headers = http::HeaderMap::new();
        let map = self.to_w3c();

        for (key, value) in map {
            if let Ok(value) = http::HeaderValue::from_str(&value) {
                let name = match key.as_str() {
                    TRACEPARENT_HEADER => http::header::HeaderName::from_static(TRACEPARENT_HEADER),
                    TRACESTATE_HEADER => http::header::HeaderName::from_static(TRACESTATE_HEADER),
                    _ => continue,
                };
                headers.insert(name, value);
            }
        }

        headers
    }

    /// Create a new tracing context from the W3C trace context format in HTTP headers.
    ///
    /// Returns `None` if the headers are invalid.
    pub fn from_http_headers(headers: &http::HeaderMap) -> Option<Self> {
        let traceparent = headers.get(TRACEPARENT_HEADER)?.to_str().ok()?;
        let span_context = SpanContext::decode_w3c_traceparent(traceparent)?;
        let tracestate = headers
            .get(TRACESTATE_HEADER)
            .map(|value| value.to_str().map(str::to_owned))
            .transpose()
            .ok()?;

        Some(Self {
            span_context: Some(span_context),
            tracestate,
        })
    }
}

#[cfg(test)]
mod tests {
    use fastrace::prelude::{SpanContext, SpanId, TraceId};

    use super::*;

    const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    const TRACESTATE: &str = "rw=frontend,congo=t61rcWkgMzE";

    fn context() -> TracingContext {
        TracingContext {
            span_context: Some(SpanContext::new(
                TraceId(0x0af7651916cd43dd8448eb211c80319c),
                SpanId(0xb7ad6b7169203331),
            )),
            tracestate: Some(TRACESTATE.to_owned()),
        }
    }

    #[test]
    fn none_context_does_not_emit_w3c_fields() {
        let context = TracingContext::none();

        assert!(context.to_protobuf().is_empty());
        assert!(context.to_http_headers().is_empty());
        assert!(context.to_fastrace_span_context().is_none());
    }

    #[test]
    fn protobuf_roundtrip_preserves_traceparent_and_tracestate() {
        let fields = context().to_protobuf();

        assert_eq!(fields.get(TRACEPARENT_HEADER).unwrap(), TRACEPARENT);
        assert_eq!(fields.get(TRACESTATE_HEADER).unwrap(), TRACESTATE);

        let roundtrip = TracingContext::from_protobuf(&fields).to_protobuf();
        assert_eq!(roundtrip.get(TRACEPARENT_HEADER).unwrap(), TRACEPARENT);
        assert_eq!(roundtrip.get(TRACESTATE_HEADER).unwrap(), TRACESTATE);
    }

    #[test]
    fn http_headers_roundtrip_preserves_traceparent_and_tracestate() {
        let headers = context().to_http_headers();

        assert_eq!(headers.get(TRACEPARENT_HEADER).unwrap(), TRACEPARENT);
        assert_eq!(headers.get(TRACESTATE_HEADER).unwrap(), TRACESTATE);

        let roundtrip = TracingContext::from_http_headers(&headers)
            .unwrap()
            .to_http_headers();
        assert_eq!(roundtrip.get(TRACEPARENT_HEADER).unwrap(), TRACEPARENT);
        assert_eq!(roundtrip.get(TRACESTATE_HEADER).unwrap(), TRACESTATE);
    }

    #[test]
    fn http_headers_accept_missing_tracestate() {
        let mut headers = http::HeaderMap::new();
        headers.insert(TRACEPARENT_HEADER, TRACEPARENT.parse().unwrap());

        let roundtrip = TracingContext::from_http_headers(&headers)
            .unwrap()
            .to_protobuf();
        assert_eq!(roundtrip.get(TRACEPARENT_HEADER).unwrap(), TRACEPARENT);
        assert!(!roundtrip.contains_key(TRACESTATE_HEADER));
    }

    #[test]
    fn http_headers_reject_invalid_traceparent() {
        let mut headers = http::HeaderMap::new();
        headers.insert(TRACEPARENT_HEADER, "not-a-traceparent".parse().unwrap());

        assert!(TracingContext::from_http_headers(&headers).is_none());
    }

    #[test]
    fn from_current_span_reads_fastrace_local_parent() {
        let root = fastrace::Span::root("test_root", SpanContext::random());
        let _guard = root.set_local_parent();

        let fields = TracingContext::from_current_span().to_protobuf();
        assert!(fields.contains_key(TRACEPARENT_HEADER));
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
