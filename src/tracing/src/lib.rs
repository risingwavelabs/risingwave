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

use std::env;
use std::net::SocketAddr;
use std::thread::JoinHandle;

use anyhow::{Error, Result};
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use minitrace::prelude::*;

pub struct RwTracingService {
    tx: UnboundedSender<Collector>,
    _join_handle: Option<JoinHandle<()>>,
    enabled: bool,
}

pub struct TracingConfig {
    pub jaeger_endpoint: Option<String>,
    pub print_to_console: bool,
    pub slow_request_threshold_ms: u64,
}

impl TracingConfig {
    pub fn new(jaeger_endpoint: String) -> Self {
        let slow_request_threshold_ms: u64 = env::var("RW_TRACE_SLOW_REQUEST_THRESHOLD_MS")
            .ok()
            .map_or_else(|| 100, |v| v.parse().unwrap());
        let print_to_console = env::var("RW_TRACE_SLOW_REQUEST")
            .ok()
            .map_or_else(|| false, |v| v == "true");

        Self {
            jaeger_endpoint: Some(jaeger_endpoint),
            print_to_console,
            slow_request_threshold_ms,
        }
    }
}

impl RwTracingService {
    /// Create a new tracing service instance. Spawn a background thread to observe slow requests.
    pub fn new(config: TracingConfig) -> Result<Self> {
        let (tx, rx) = unbounded();

        let jaeger_addr: Option<SocketAddr> =
            config.jaeger_endpoint.map(|x| x.parse()).transpose()?;

        let join_handle = if cfg!(madsim) {
            None
        } else {
            Some(Self::start_tracing_listener(
                rx,
                config.print_to_console,
                config.slow_request_threshold_ms,
                jaeger_addr,
            ))
        };

        let tr = Self {
            tx,
            _join_handle: join_handle,
            enabled: jaeger_addr.is_some(),
        };
        Ok(tr)
    }

    pub fn disabled() -> Self {
        let (tx, _) = unbounded();
        Self {
            tx,
            _join_handle: None,
            enabled: false,
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

    #[cfg(madsim)]
    fn start_tracing_listener(
        _rx: UnboundedReceiver<Collector>,
        _print_to_console: bool,
        _slow_request_threshold_ms: u64,
        _jaeger_addr: Option<SocketAddr>,
    ) -> JoinHandle<()> {
        unreachable!()
    }

    #[cfg(not(madsim))]
    fn start_tracing_listener(
        rx: UnboundedReceiver<Collector>,
        print_to_console: bool,
        slow_request_threshold_ms: u64,
        jaeger_addr: Option<SocketAddr>,
    ) -> JoinHandle<()> {
        use futures::StreamExt;
        use rand::Rng;

        tracing::info!(
            "tracing service started with slow_request_threshold_ms={slow_request_threshold_ms}, print_to_console={print_to_console}"
        );

        std::thread::Builder::new()
            .name("minitrace_listener".to_string())
            .spawn(move || {
                let func = move || {
                    let rt = tokio::runtime::Builder::new_current_thread().build()?;
                    let stream = rx.for_each_concurrent(None, |collector| async {
                        let spans = collector.collect().await;
                        if !spans.is_empty() {
                            // print slow requests
                            if print_to_console {
                                // print requests > 100ms
                                if spans[0].duration_ns >= slow_request_threshold_ms * 1_000_000 {
                                    tracing::info!("{:?}", spans);
                                }
                            }
                            // report spans to jaeger
                            if let Some(ref jaeger_addr) = jaeger_addr {
                                let trace_id = rand::thread_rng().gen::<u64>();
                                let span_id = rand::thread_rng().gen::<u32>();
                                let encoded = minitrace_jaeger::encode(
                                    "risingwave".to_string(),
                                    trace_id,
                                    0,
                                    span_id,
                                    &spans,
                                )
                                .unwrap();
                                if let Err(err) =
                                    minitrace_jaeger::report(*jaeger_addr, &encoded).await
                                {
                                    tracing::warn!("failed to report spans to jaeger: {}", err);
                                }
                            }
                        }
                    });
                    rt.block_on(stream);
                    Ok::<_, Error>(())
                };
                if let Err(err) = func() {
                    tracing::warn!("tracing thread exited: {:#}", err);
                }
            })
            .unwrap()
    }
}
