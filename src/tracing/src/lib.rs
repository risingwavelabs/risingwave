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

use std::env;
use std::thread::JoinHandle;

use anyhow::Error;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::StreamExt;
use minitrace::prelude::*;

pub struct RwTracingService {
    tx: UnboundedSender<Collector>,
    _join_handle: JoinHandle<()>,
    enabled: bool,
}

impl RwTracingService {
    /// Create a new tracing service instance. Spawn a background thread to observe slow requests.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let (tx, rx) = unbounded();
        let slow_request_threshold_ms: u64 = env::var("RW_TRACE_SLOW_REQUEST_THRESHOLD_MS")
            .ok()
            .map_or_else(|| 100, |v| v.parse().unwrap());

        let join_handle = Self::start_tracing_listener(rx, slow_request_threshold_ms);

        Self {
            tx,
            _join_handle: join_handle,
            enabled: env::var("RW_TRACE_SLOW_REQUEST")
                .ok()
                .map_or_else(|| false, |v| v == "true"),
        }
    }

    /// Create a new tracing event.
    pub fn new_tracer(&self, event: &'static str) -> Span {
        if self.enabled {
            let (span, collector) = Span::root(event);
            self.tx.unbounded_send(collector).unwrap();
            span
        } else {
            Span::new_noop()
        }
    }

    fn start_tracing_listener(
        rx: UnboundedReceiver<Collector>,
        slow_request_threshold_ms: u64,
    ) -> JoinHandle<()> {
        tracing::info!(
            "tracing service started with slow_request_threshold_ms={slow_request_threshold_ms}"
        );
        std::thread::Builder::new()
            .name("minitrace_listener".to_string())
            .spawn(move || {
                let func = move || {
                    let stream = rx.for_each_concurrent(None, |collector| async move {
                        let spans = collector.collect().await;
                        if let Some(span) = spans.first() {
                            // print requests > 100ms
                            if span.duration_ns >= slow_request_threshold_ms * 1_000_000 {
                                tracing::info!("{:?}", spans);
                            }
                        }
                    });
                    futures::executor::block_on(stream);
                    Ok::<_, Error>(())
                };
                if let Err(err) = func() {
                    tracing::warn!("tracing thread exited: {:#}", err);
                }
            })
            .unwrap()
    }
}
