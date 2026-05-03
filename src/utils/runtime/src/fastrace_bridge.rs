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

use fastrace_tracing::FastraceCompatLayer;
use tracing::Subscriber;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::registry::LookupSpan;

/// Proof-only target used by the Phase-A fastrace bridge tests.
///
/// The compat layer is deliberately restricted to this target plus one representative
/// stream-message event target. This keeps Phase A from becoming a broad production
/// mirror of all `tracing` spans before span volume and root boundaries are validated.
///
/// The `fastrace-bridge` Cargo feature also enables `fastrace/enable`, which turns
/// on global fastrace span collection. This is necessary for the proof reporter, but
/// it also affects any other fastrace users in the process, such as foyer.
pub(crate) const BRIDGE_PROOF_TARGET: &str = "rw_fastrace_bridge::proof";

/// Representative structured stream-message target used to verify event field mapping.
pub(crate) const STREAM_MESSAGE_CHUNK_TARGET: &str = "events::stream::message::chunk";

pub(crate) fn compat_layer<S>() -> impl Layer<S> + Send + Sync + 'static
where
    S: Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
{
    FastraceCompatLayer::new()
        .with_level(true)
        .with_filter(bridge_targets())
}

#[cfg(test)]
pub(crate) fn with_bridge_root<R>(name: &'static str, f: impl FnOnce() -> R) -> R {
    use fastrace::prelude::SpanContext;

    let root = fastrace::Span::root(name, SpanContext::random());
    let _guard = root.set_local_parent();
    f()
}

fn bridge_targets() -> Targets {
    Targets::new()
        .with_target(BRIDGE_PROOF_TARGET, LevelFilter::TRACE)
        .with_target(STREAM_MESSAGE_CHUNK_TARGET, LevelFilter::TRACE)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, LazyLock};
    use std::time::Duration;

    use fastrace::collector::{Config, Reporter, SpanRecord};
    use parking_lot::Mutex;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    // `fastrace::set_reporter` is process-global. Keep these tests serialized so
    // each test sees only its own reporter and captured records.
    static FASTRACE_REPORTER_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[derive(Clone, Default)]
    struct CapturedSpans(Arc<Mutex<Vec<SpanRecord>>>);

    impl CapturedSpans {
        fn install(&self) {
            fastrace::set_reporter(
                CapturingReporter {
                    spans: self.0.clone(),
                },
                Config::default().report_interval(Duration::from_millis(10)),
            );
        }

        fn snapshot(&self) -> Vec<SpanRecord> {
            fastrace::flush();
            self.0.lock().clone()
        }
    }

    struct CapturingReporter {
        spans: Arc<Mutex<Vec<SpanRecord>>>,
    }

    impl Reporter for CapturingReporter {
        fn report(&mut self, spans: Vec<SpanRecord>) {
            self.spans.lock().extend(spans);
        }
    }

    #[test]
    fn fastrace_origin_span_reaches_reporter() {
        let _guard = FASTRACE_REPORTER_LOCK.lock();
        let captured = CapturedSpans::default();
        captured.install();

        with_bridge_root("phase_a_fastrace_origin", || {});

        let spans = captured.snapshot();
        assert!(
            spans
                .iter()
                .any(|span| span.name == "phase_a_fastrace_origin"),
            "fastrace-origin span was not reported: {spans:#?}"
        );
    }

    #[test]
    fn bridge_filter_rejects_unlisted_targets_without_orphan_roots() {
        let _guard = FASTRACE_REPORTER_LOCK.lock();
        let captured = CapturedSpans::default();
        captured.install();

        let subscriber = tracing_subscriber::registry().with(compat_layer());
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::span!(
                target: "rw_fastrace_unlisted",
                tracing::Level::INFO,
                "unlisted_span"
            );
            let _enter = span.enter();
            tracing::info!(target: "rw_fastrace_unlisted", "unlisted event");
        });

        let spans = captured.snapshot();
        assert!(
            spans.is_empty(),
            "unlisted targets should not create fastrace roots: {spans:#?}"
        );
    }

    #[test]
    fn bridge_maps_structured_stream_event_fields() {
        let _guard = FASTRACE_REPORTER_LOCK.lock();
        let captured = CapturedSpans::default();
        captured.install();

        let subscriber = tracing_subscriber::registry().with(compat_layer());
        with_bridge_root("phase_a_bridge_root", || {
            // The compat layer observes the fastrace local parent set by
            // `with_bridge_root`; keep this ordering if this proof test is refactored.
            tracing::subscriber::with_default(subscriber, || {
                let span = tracing::span!(
                    target: BRIDGE_PROOF_TARGET,
                    tracing::Level::INFO,
                    "phase_a_bridge_span",
                    component = "runtime"
                );
                let _enter = span.enter();
                tracing::event!(
                    target: STREAM_MESSAGE_CHUNK_TARGET,
                    tracing::Level::DEBUG,
                    cardinality = 7_i64,
                    capacity = 11_i64,
                    "chunk event"
                );
            });
        });

        let spans = captured.snapshot();
        let bridged_span = spans
            .iter()
            .find(|span| span.name == "phase_a_bridge_span")
            .unwrap_or_else(|| panic!("bridged tracing span was not reported: {spans:#?}"));

        assert_property(&bridged_span.properties, "component", "runtime");
        assert_property(&bridged_span.properties, "level", "INFO");

        let stream_event = bridged_span
            .events
            .iter()
            .find(|event| {
                event
                    .properties
                    .iter()
                    .any(|(key, value)| key == "target" && value == STREAM_MESSAGE_CHUNK_TARGET)
            })
            .unwrap_or_else(|| {
                panic!(
                    "stream message event was not bridged: {:#?}",
                    bridged_span.events
                )
            });

        assert_property(&stream_event.properties, "level", "DEBUG");
        assert_property(
            &stream_event.properties,
            "target",
            STREAM_MESSAGE_CHUNK_TARGET,
        );
        assert_property(&stream_event.properties, "cardinality", "7");
        assert_property(&stream_event.properties, "capacity", "11");
    }

    fn assert_property(
        properties: &[(
            std::borrow::Cow<'static, str>,
            std::borrow::Cow<'static, str>,
        )],
        key: &str,
        expected: &str,
    ) {
        assert!(
            properties
                .iter()
                .any(|(property_key, value)| property_key == key && value == expected),
            "missing property {key}={expected}; properties: {properties:#?}"
        );
    }
}
