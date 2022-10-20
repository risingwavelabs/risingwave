// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::time::Duration;

use futures::Future;
use tokio::sync::watch;

use crate::context::{with_context, TraceContext, TRACE_CONTEXT};
use crate::SpanValue;

pub(crate) type TraceSender = watch::Sender<StackTraceReport>;
pub(crate) type TraceReceiver = watch::Receiver<StackTraceReport>;

/// The report of a stack trace.
#[derive(Debug, Clone)]
pub struct StackTraceReport {
    /// The content of the report, which is structured as a tree with indentation.
    pub report: String,

    /// The time when the report is captured or reported.
    pub capture_time: std::time::Instant,
}

impl Default for StackTraceReport {
    fn default() -> Self {
        Self {
            report: "<initial>\n".to_string(),
            capture_time: std::time::Instant::now(),
        }
    }
}

impl std::fmt::Display for StackTraceReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[captured {:?} ago]\n{}",
            self.capture_time.elapsed(),
            self.report
        )
    }
}

/// Configuration for a traced context.
#[derive(Debug, Clone)]
pub struct TraceConfig {
    /// Whether to report the futures that are not able to be polled now.
    pub report_detached: bool,

    /// Whether to report the "verbose" stack trace.
    pub verbose: bool,

    /// The interval to report the stack trace.
    pub interval: Duration,
}

/// Used to start a reporter along with the traced future.
pub struct TraceReporter {
    /// Used to send the report periodically to the manager.
    pub(crate) tx: TraceSender,
}

impl TraceReporter {
    /// Provide a stack tracing context with the `root_span` for the given future. The reporter will
    /// be started along with this future in the current task and update the captured stack trace
    /// report periodically.
    pub async fn trace<F: Future>(
        self,
        future: F,
        root_span: impl Into<SpanValue>,
        TraceConfig {
            report_detached,
            verbose,
            interval,
        }: TraceConfig,
    ) -> F::Output {
        TRACE_CONTEXT
            .scope(
                TraceContext::new(root_span.into(), report_detached, verbose).into(),
                async move {
                    let reporter = async move {
                        let mut interval = tokio::time::interval(interval);
                        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                        loop {
                            interval.tick().await;
                            let new_trace = with_context(|c| c.to_report());
                            match self.tx.send(new_trace) {
                                Ok(_) => {}
                                Err(e) => {
                                    tracing::error!(
                                        "Trace report error: failed to send trace: {}",
                                        e
                                    );
                                    futures::future::pending().await
                                }
                            }
                        }
                    };

                    tokio::select! {
                        biased; // always prefer reporting
                        _ = reporter => unreachable!(),
                        output = future => output,
                    }
                },
            )
            .await
    }
}

/// Manages the stack traces of multiple tasks.
#[derive(Default, Debug)]
pub struct StackTraceManager<K> {
    rxs: HashMap<K, TraceReceiver>,
}

impl<K> StackTraceManager<K>
where
    K: std::hash::Hash + Eq + std::fmt::Debug,
{
    /// Register with given key. Returns a sender that can be called `trace` on.
    pub fn register(&mut self, key: K) -> TraceReporter {
        let (tx, rx) = watch::channel(Default::default());
        self.rxs.try_insert(key, rx).unwrap();
        TraceReporter { tx }
    }

    /// Get all trace reports registered in this manager.
    ///
    /// Note that the reports might not be updated if the traced task is doing some computation
    /// heavy work and never yields, one may check how long the captured time has elapsed to confirm
    /// this.
    pub fn get_all(&mut self) -> impl Iterator<Item = (&K, watch::Ref<'_, StackTraceReport>)> {
        self.rxs.retain(|_, rx| rx.has_changed().is_ok());
        self.rxs.iter_mut().map(|(k, v)| (k, v.borrow_and_update()))
    }
}
