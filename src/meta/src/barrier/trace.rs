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

use risingwave_common::util::epoch::Epoch;

/// A wrapper of [`Epoch`] with tracing span, used for issuing epoch-based tracing from the barrier
/// manager on the meta service. This structure is free to clone, which'll extend the lifetime of
/// the underlying span.
///
/// - A new [`TracedEpoch`] is created when the barrier manager is going to inject a new barrier.
///   The span will be created automatically and the start time is recorded.
/// - Then, the previous and the current [`TracedEpoch`]s are stored in the command context.
/// - When the barrier is successfully collected and committed, the command context will be dropped,
///   then the previous span will be automatically closed.
#[derive(Debug, Clone)]
pub struct TracedEpoch {
    epoch: Epoch,
    span: tracing::Span,
}

impl TracedEpoch {
    /// Create a new [`TracedEpoch`] with the given `epoch`.
    pub fn new(epoch: Epoch) -> Self {
        // The span created on the meta service is always a root span for epoch-level tracing.
        let span = tracing::info_span!(
            parent: None,
            "epoch",
            "otel.name" = format!("Epoch {}", epoch.0),
            epoch = epoch.0
        );

        Self { epoch, span }
    }

    /// Create a new [`TracedEpoch`] with the next epoch.
    pub fn next(&self) -> Self {
        Self::new(self.epoch.next())
    }

    /// Retrieve the epoch value.
    pub fn value(&self) -> Epoch {
        self.epoch
    }

    /// Retrieve the tracing span.
    pub fn span(&self) -> &tracing::Span {
        &self.span
    }
}
