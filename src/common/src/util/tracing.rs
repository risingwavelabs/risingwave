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

use std::collections::HashMap;

use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::sdk::propagation::TraceContextPropagator;
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
            map.insert(key.to_string(), value.to_string());
        }

        Some(Self::from_w3c(&map))
    }
}
