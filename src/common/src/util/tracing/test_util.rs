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

//! Deterministic in-memory fastrace reporter for tests.
//!
//! This module provides [`MemoryReporter`], a thread-safe in-memory fastrace
//! reporter that captures `SpanRecord`s for deterministic assertions in unit
//! and integration tests. It replaces the ad-hoc `CapturingReporter` pattern
//! in `src/utils/runtime/src/fastrace_bridge.rs` and is intended to be the
//! single reusable test reporter for the fastrace-native tracing stack.
//!
//! # Process-global reporter serialization
//!
//! `fastrace::set_reporter` is process-global. Tests that install a reporter
//! MUST hold [`FASTRACE_REPORTER_LOCK`] for the duration of their setup and
//! assertions. Not doing so risks cross-test span contamination when
//! `cargo test` runs tests in parallel.
//!
//! # Example
//!
//! ```ignore
//! use std::time::Duration;
//! use risingwave_common::util::tracing::test_util::{
//!     FASTRACE_REPORTER_LOCK, MemoryReporter, with_root,
//! };
//!
//! #[test]
//! fn grpc_server_attaches_child_span_to_injected_parent() {
//!     let _guard = FASTRACE_REPORTER_LOCK.lock();
//!     let sink = MemoryReporter::install(Duration::from_millis(10));
//!
//!     with_root("test_root", || {
//!         // ... exercise code that creates a "grpc_serve" span ...
//!     });
//!
//!     sink.assert_has_span("grpc_serve");
//! }
//! ```

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use fastrace::collector::{Config, Reporter, SpanRecord};
use parking_lot::Mutex;

/// Process-global lock that tests MUST hold while installing a reporter and
/// making assertions about collected spans.
///
/// `fastrace::set_reporter` is process-global and has no "unset" API, so
/// concurrent tests would otherwise contaminate each other's span buffers.
pub static FASTRACE_REPORTER_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// A thread-safe, deterministic in-memory fastrace reporter for tests.
///
/// Install once per test after acquiring [`FASTRACE_REPORTER_LOCK`]. The
/// reporter is `Clone` so a handle can be kept for assertions while the
/// inner `fastrace` state holds another copy.
#[derive(Clone, Default)]
pub struct MemoryReporter {
    inner: Arc<Mutex<MemoryReporterInner>>,
}

#[derive(Default)]
struct MemoryReporterInner {
    spans: Vec<SpanRecord>,
    /// Optional filter: only retain spans whose name contains this substring.
    name_filter: Option<String>,
}

impl MemoryReporter {
    /// Install this reporter as the process-global fastrace reporter with the
    /// given report interval. Returns a handle for assertions.
    ///
    /// Caller MUST hold [`FASTRACE_REPORTER_LOCK`] for the duration of the
    /// test.
    pub fn install(report_interval: Duration) -> Self {
        Self::install_with_config(Config::default().report_interval(report_interval))
    }

    /// Install with a fully-custom `fastrace::collector::Config`.
    pub fn install_with_config(config: Config) -> Self {
        let reporter = Self::default();
        fastrace::set_reporter(reporter.clone(), config);
        reporter
    }

    /// Only retain captured spans whose name contains the given substring.
    ///
    /// Must be called BEFORE spans are captured. Does not retroactively
    /// filter already-collected spans.
    pub fn with_name_filter(self, substring: impl Into<String>) -> Self {
        self.inner.lock().name_filter = Some(substring.into());
        self
    }

    /// Flush pending spans and return a snapshot of everything captured so
    /// far. Spans are in arrival-at-reporter order.
    pub fn snapshot(&self) -> Vec<SpanRecord> {
        fastrace::flush();
        self.inner.lock().spans.clone()
    }

    /// Return only spans whose name equals `name`.
    pub fn spans_named(&self, name: &str) -> Vec<SpanRecord> {
        self.snapshot()
            .into_iter()
            .filter(|span| span.name == name)
            .collect()
    }

    /// Return spans whose name contains `pat` as a substring.
    pub fn spans_matching(&self, pat: &str) -> Vec<SpanRecord> {
        self.snapshot()
            .into_iter()
            .filter(|span| span.name.contains(pat))
            .collect()
    }

    /// Return spans whose properties contain the given `(key, value)` pair.
    pub fn spans_with_property(&self, key: &str, value: &str) -> Vec<SpanRecord> {
        self.snapshot()
            .into_iter()
            .filter(|span| span.properties.iter().any(|(k, v)| k == key && v == value))
            .collect()
    }

    /// Assert that at least one span with the given name was captured.
    #[track_caller]
    pub fn assert_has_span(&self, name: &str) {
        let spans = self.snapshot();
        assert!(
            spans.iter().any(|span| span.name == name),
            "expected span `{name}` not found; captured names: {:#?}",
            spans.iter().map(|span| &span.name).collect::<Vec<_>>(),
        );
    }

    /// Assert that no spans were captured.
    #[track_caller]
    pub fn assert_empty(&self) {
        let spans = self.snapshot();
        assert!(spans.is_empty(), "expected no spans, got: {spans:#?}");
    }

    /// Clear the captured span buffer without uninstalling. Useful for
    /// sectioning assertions within a single test.
    pub fn clear(&self) {
        self.inner.lock().spans.clear();
    }
}

impl Reporter for MemoryReporter {
    fn report(&mut self, spans: Vec<SpanRecord>) {
        let mut inner = self.inner.lock();
        if let Some(filter) = inner.name_filter.clone() {
            for span in spans {
                if span.name.contains(&filter) {
                    inner.spans.push(span);
                }
            }
        } else {
            inner.spans.extend(spans);
        }
    }
}

/// Run `f` inside a fresh fastrace root span named `name`.
///
/// The span is installed as the local parent for `f` via
/// `Span::set_local_parent()`. The guard is dropped when `f` returns, so
/// this is safe for synchronous code but NOT for code that needs the
/// parent to persist across `.await` points — use `fastrace::FutureExt::in_span`
/// for async cases.
pub fn with_root<R>(name: &'static str, f: impl FnOnce() -> R) -> R {
    use fastrace::prelude::SpanContext;

    let root = fastrace::Span::root(name, SpanContext::random());
    let _guard = root.set_local_parent();
    f()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_reporter_captures_root_span() {
        let _guard = FASTRACE_REPORTER_LOCK.lock();
        let sink = MemoryReporter::install(Duration::from_millis(10));

        with_root("memory_reporter_captures_root_span", || {});

        sink.assert_has_span("memory_reporter_captures_root_span");
    }

    #[test]
    fn memory_reporter_name_filter_drops_non_matching() {
        let _guard = FASTRACE_REPORTER_LOCK.lock();
        let sink = MemoryReporter::install(Duration::from_millis(10)).with_name_filter("kept");

        with_root("dropped_root", || {
            let _guard = fastrace::Span::enter_with_local_parent("kept_child");
        });

        let spans = sink.snapshot();
        assert!(spans.iter().all(|s| s.name.contains("kept")), "{spans:#?}");
    }

    #[test]
    fn memory_reporter_clear_empties_buffer() {
        let _guard = FASTRACE_REPORTER_LOCK.lock();
        let sink = MemoryReporter::install(Duration::from_millis(10));

        with_root("before_clear", || {});
        let _ = sink.snapshot();
        sink.clear();
        sink.assert_empty();
    }
}
