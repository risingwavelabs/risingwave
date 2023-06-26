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
/// See [Trace Context](https://www.w3.org/TR/trace-context/) for more information.
#[derive(Debug, Clone)]
pub struct TracingContext(opentelemetry::Context);

type Propagator = TraceContextPropagator;

impl TracingContext {
    /// Create a new tracing context from a tracing span.
    pub fn from_span(span: &tracing::Span) -> Self {
        Self(span.context())
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

    pub fn to_protobuf(&self) -> HashMap<String, String> {
        let mut fields = HashMap::new();
        Propagator::new().inject_context(&self.0, &mut fields);
        fields
    }

    pub fn from_protobuf(fields: &HashMap<String, String>) -> Self {
        let context = Propagator::new().extract(fields);
        Self(context)
    }
}
