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
/// When disabled, collected spans are dropped at the **export boundary** without being
/// forwarded to the inner reporter (e.g. OTLP). Note that fastrace still records spans
/// in-process even when the reporter gate is off — this toggle only controls whether
/// recorded spans are exported. When enabled, spans are forwarded to the inner reporter.
///
/// This preserves the `enable_tracing` system parameter semantics without needing to
/// swap reporters via `fastrace::set_reporter`.
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use fastrace::collector::SpanRecord;

    use super::*;

    /// A minimal reporter that records how many spans it received.
    #[derive(Default)]
    struct CountingReporter {
        count: usize,
    }

    impl Reporter for CountingReporter {
        fn report(&mut self, spans: Vec<SpanRecord>) {
            self.count += spans.len();
        }
    }

    #[test]
    fn disabled_reporter_drops_spans() {
        let inner = CountingReporter::default();
        let (mut reporter, _flag) = ToggleableReporter::new(inner);

        // Default is disabled — spans should be dropped
        reporter.report(vec![SpanRecord::default()]);
        assert_eq!(reporter.inner.count, 0);
    }

    #[test]
    fn enabled_reporter_forwards_spans() {
        let inner = CountingReporter::default();
        let (mut reporter, flag) = ToggleableReporter::new(inner);

        flag.store(true, Ordering::Relaxed);
        reporter.report(vec![SpanRecord::default(), SpanRecord::default()]);
        assert_eq!(reporter.inner.count, 2);
    }

    #[test]
    fn toggle_on_then_off() {
        let inner = CountingReporter::default();
        let (mut reporter, flag) = ToggleableReporter::new(inner);

        // Enable, send 1 span
        flag.store(true, Ordering::Relaxed);
        reporter.report(vec![SpanRecord::default()]);
        assert_eq!(reporter.inner.count, 1);

        // Disable, send 1 span — should be dropped
        flag.store(false, Ordering::Relaxed);
        reporter.report(vec![SpanRecord::default()]);
        assert_eq!(reporter.inner.count, 1);

        // Re-enable, send 1 span
        flag.store(true, Ordering::Relaxed);
        reporter.report(vec![SpanRecord::default()]);
        assert_eq!(reporter.inner.count, 2);
    }
}
