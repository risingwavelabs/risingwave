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

//! Toggleable fastrace reporter wrapper for runtime enable/disable control.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fastrace::collector::{Reporter, SpanRecord};

/// A fastrace reporter that can be toggled on/off at runtime via an `Arc<AtomicBool>`.
///
/// When disabled, spans are dropped without being reported. When enabled, spans are
/// forwarded to the inner reporter. This preserves the `enable_tracing` system parameter
/// semantics without needing to swap reporters via `fastrace::set_reporter`.
pub(crate) struct ToggleableReporter<R> {
    inner: R,
    enabled: Arc<AtomicBool>,
}

impl<R> ToggleableReporter<R> {
    /// Create a new toggleable reporter wrapping the given inner reporter.
    ///
    /// Returns the reporter and a handle to the enabled flag. The caller should
    /// store the handle and use it to toggle the reporter on/off.
    pub(crate) fn new(inner: R) -> (Self, Arc<AtomicBool>) {
        let enabled = Arc::new(AtomicBool::new(false));
        let reporter = Self {
            inner,
            enabled: enabled.clone(),
        };
        (reporter, enabled)
    }
}

impl<R: Reporter> Reporter for ToggleableReporter<R> {
    fn report(&mut self, spans: Vec<SpanRecord>) {
        if self.enabled.load(Ordering::Relaxed) {
            self.inner.report(spans);
        }
        // Otherwise drop spans silently (disabled state).
    }
}
